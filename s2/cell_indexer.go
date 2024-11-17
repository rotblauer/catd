/*
Package s2 provides an implementation of CatTrack indexing using the S2 Geometry Library.

Its input-API is stream-based.

It uses a KV datastore to maintain an index of level-based S2 cells (areas of the Earth's surface) that represent CatTracks.
The Value for each cell is a nominal datum intended only signify the presence of a CatTrack,
and, in its smallness, to consume minimal storage space.

In front of the KV database sits an LRU cache which minimizes the number disk-IO interactions.
This cache is not publicly available, and is not intended for direct use by clients.

The first track representing a unique cell (for some S2 level) is stored gzipped
in an append-only file.

This pattern prohibits the modification or synthesis of cell-unique CatTracks beyond their first representative,
but treads lightly and is sufficient to draw dots on the map.

TODO: Explore tallying or otherwise aggregating track data for indexed cells,
for example the number of tracks matched to that cell.
*/
package s2

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang/geo/s2"
	"github.com/golang/groupcache/lru"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	bbolt "go.etcd.io/bbolt"
	"log"
	"log/slog"
	"path/filepath"
)

const s2DBName = "s2.db"

type CellIndexer struct {
	CatID     conceptual.CatID
	Caches    map[CellLevel]*lru.Cache
	Levels    []CellLevel
	DB        *bbolt.DB
	FlatFiles map[CellLevel]*flat.GZFile
	BatchSize int
}

func NewCellIndexer(catID conceptual.CatID, root string, levels []CellLevel, batchSize int) (*CellIndexer, error) {
	if len(levels) == 0 {
		return nil, fmt.Errorf("no levels provided")
	}

	f := flat.NewFlatWithRoot(root).ForCat(catID)
	if err := f.Ensure(); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(f.Path(), s2DBName)
	db, err := bbolt.Open(dbPath, 0660, nil)
	if err != nil {
		return nil, err
	}

	flatFileMap := make(map[CellLevel]*flat.GZFile)
	for _, level := range levels {
		gzf, err := f.NamedGZ(fmt.Sprintf("s2_level-%d.geojson.gz", level))
		if err != nil {
			return nil, err
		}
		flatFileMap[level] = gzf
	}

	caches := make(map[CellLevel]*lru.Cache)
	for _, level := range levels {
		caches[level] = lru.New(10_000)
	}

	return &CellIndexer{
		CatID:     catID,
		Levels:    levels,
		Caches:    caches,
		DB:        db,
		FlatFiles: flatFileMap,
		BatchSize: batchSize,
	}, nil
}

// Index indexes the given CatTracks into the S2 cell index(es), appending
// all unique tracks to flat files respective of cell level.
// It will block until the in channel closes and all batches are processed.
func (ci *CellIndexer) Index(ctx context.Context, in <-chan *cattrack.CatTrack) error {
	batches := stream.Batch(ctx, nil, func(tracks []*cattrack.CatTrack) bool {
		return len(tracks) == ci.BatchSize
	}, in)
	for batch := range batches {
		for _, level := range ci.Levels {
			slog.Debug("CellIndexer batch", "cat", ci.CatID,
				"level", level, "size", len(batch))

			_, uniqTracks := ci.filterAndIndexUniqCatTracks(ci.CatID.String(), level, batch)
			if len(uniqTracks) == 0 {
				continue
			}
			enc := json.NewEncoder(ci.FlatFiles[level].Writer())
			for _, ct := range uniqTracks {
				if err := enc.Encode(ct); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ci *CellIndexer) Close() error {
	if err := ci.DB.Close(); err != nil {
		return err
	}
	for _, gzf := range ci.FlatFiles {
		if err := gzf.Close(); err != nil {
			return err
		}
	}
	return nil
}

// filterUniqFromDBWriting filters the givens cellIDs and tracklines to those which were not found in the database.
// The unique cells will be written before the function returns.
// I think it's important to read+write in the same transaction, so that the db is not left in an inconsistent state
// with multiple routines potentially accessing it.
// I expect that bolt will lock the db for the duration of the transaction,
// so that other routines will block until the transaction is complete.
func (ci *CellIndexer) filterUniqFromDBWriting(level CellLevel, cellIDs []s2.CellID, tracks []*cattrack.CatTrack) (uniqCellIDs []s2.CellID, uniqTracks []*cattrack.CatTrack, err error) {
	bucket := dbBucket(level)
	err = ci.DB.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		for i, cellID := range cellIDs {
			v := b.Get(cellIDDBKey(cellID))
			if v == nil {
				uniqCellIDs = append(uniqCellIDs, cellID)
				uniqTracks = append(uniqTracks, tracks[i])

				// Write the new cell to the index.
				err = b.Put(cellIDDBKey(cellID), []byte{0x1})
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	return
}

// filterAndIndexUniqCatTracks returns unique cellIDs and associated tracks for the given cat.
// It attempts first to cross reference all tracks against the cache. This step returns the indices of the tracks that are not in the cache.
// Those uncached tracks are then cross referenced against the index db, which writes them if they are unique.
// Only the DB-access part of the process is blocking, since the cache is only a nice-to-have and we
// don't care if some cache misses are false negatives.
func (ci *CellIndexer) filterAndIndexUniqCatTracks(cat string, level CellLevel, tracks []*cattrack.CatTrack) (uniqCellIDs []s2.CellID, uniqTracks []*cattrack.CatTrack) {
	if len(tracks) == 0 {
		return uniqCellIDs, uniqTracks
	}

	// create a cellid slice analogous to tracks
	cellIDs := getTrackCellIDs(tracks, level)
	if len(cellIDs) != len(tracks) {
		log.Fatalln("len(cellIDs) != len(tracks)", len(cellIDs), len(tracks))
	}

	// returns the indices of uniq tracklines (== uniq cellIDs)
	uniqCellIDTrackIndices := ci.uniqIndexesFromCache(level, cellIDs) // eg. 0, 23, 42, 99

	// if there are no uniq cellIDs, return early
	if len(uniqCellIDTrackIndices) == 0 {
		return
	}

	tmpUniqCellIDs := make([]s2.CellID, len(uniqCellIDTrackIndices))
	tmpUniqTracks := make([]*cattrack.CatTrack, len(uniqCellIDTrackIndices))
	for ii, idx := range uniqCellIDTrackIndices {
		tmpUniqCellIDs[ii] = cellIDs[idx]
		tmpUniqTracks[ii] = tracks[idx]
	}

	// so now we've whittled the tracks to only those not in the cache
	// we need to check the db for those that did not have cache hits

	// further whittle the uniqs based on db hits/misses
	_uniqCellIDs, _uniqTracks, err := ci.filterUniqFromDBWriting(level, tmpUniqCellIDs, tmpUniqTracks)
	if err != nil {
		log.Fatalln(err)
	}
	return _uniqCellIDs, _uniqTracks
}

// uniqIndexesFromCache returns the indices of the given cellIDs that are not in the LRU cache.
func (ci *CellIndexer) uniqIndexesFromCache(level CellLevel, cellIDs []s2.CellID) (uniqIndices []int) {
	for i, cellID := range cellIDs {
		if !ci.trackInCacheByCellID(level, cellID) {
			uniqIndices = append(uniqIndices, i)
		}
	}
	return uniqIndices
}

// cellIDWithLevel returns the cellID truncated to the given level.
func cellIDWithLevel(cellID s2.CellID, level CellLevel) s2.CellID {
	// https://docs.s2cell.aliddell.com/en/stable/s2_concepts.html#truncation
	var lsb uint64 = 1 << (2 * (30 - level))
	truncatedCellID := (uint64(cellID) & -lsb) | lsb
	return s2.CellID(truncatedCellID)
}

// getTrackCellID returns the cellID for the given track line, which is a raw string of a JSON-encoded geojson cat track.
// It applies the global cellLevel to the returned cellID.
func getTrackCellID(ct *cattrack.CatTrack, level CellLevel) s2.CellID {
	//var coords []float64
	//// Use GJSON to avoid slow unmarshalling of the entire line.
	//gjson.Get(line, "geometry.coordinates").ForEach(func(key, value gjson.Result) bool {
	//	coords = append(coords, value.Float())
	//	return true
	//})
	coords := ct.Geometry.(orb.Point)
	return cellIDWithLevel(s2.CellIDFromLatLng(s2.LatLngFromDegrees(coords[1], coords[0])), level)
}

func getTrackCellIDs(cts []*cattrack.CatTrack, level CellLevel) (cellIDs []s2.CellID) {
	for _, ct := range cts {
		cellIDs = append(cellIDs, getTrackCellID(ct, level))
	}
	return cellIDs
}

func dbBucket(level CellLevel) []byte { return []byte{byte(level)} }
func cellIDCacheKey(cellID s2.CellID) string {
	return fmt.Sprintf("%s", cellID.ToToken())
}
func cellIDDBKey(cellID s2.CellID) []byte {
	return []byte(cellID.ToToken())
}

// trackInCacheByCellID returns true if the given cellID for some cat exists in the LRU cache.
// If not, it will be added to the cache.
func (ci *CellIndexer) trackInCacheByCellID(level CellLevel, cellID s2.CellID) (exists bool) {
	key := cellIDCacheKey(cellID)
	if _, ok := ci.Caches[level].Get(key); ok {
		return ok
	}
	ci.Caches[level].Add(key, true)
	return false
}