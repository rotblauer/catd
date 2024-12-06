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

Background:
CatTracks used to pass master.json.gz (including all cats' tracks) through tippecanoe
to generate a vector tileset for the map. This was slow and memory-intensive.
The thing is, most of our tracks are spatially redundant - we sleep in the same beds
most nights and shop at the same grocery stores for the most part.
So in order to reduce tippecanoe's processing time and memory usage,
we reduced the number of tracks passed to it by indexing tracks by S2 cell and only using uniques.
This dropped tippecanoe processing time from days to an hour or two.
And we still got dots on the map showing where we'd been.

But now we have a trip detector and are generating (simplified) linestrings (aka laps) from the track points,
and able to coalesce non-trips (aka naps) into a single feature (a point, or even a polygon).
So we're able to maintain an effective representation of where we've been with again, fewer (and overall smaller) features.
This, in turn, lightens the load on tippecanoe.

So now it doesn't make a whole lot of sense to record nothing besides existence of a track in a cell,
because this isn't critical for getting things on a map. In order to make this relevant again
we should tally the number of tracks in a cell, or some other metrics/aggregations, heatmap/histogram style.
*/
package s2

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/event"
	"github.com/golang/geo/s2"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	bbolt "go.etcd.io/bbolt"
	"log/slog"
	"path/filepath"
	"time"
)

const s2DBName = "s2.db"

type CellIndexer struct {
	CatID     conceptual.CatID
	Caches    map[CellLevel]*lru.Cache[string, Indexer]
	Levels    []CellLevel
	DB        *bbolt.DB
	FlatFiles map[CellLevel]*flat.GZFileWriter
	BatchSize int
	IndexerFn func(old, next Indexer) Indexer

	logger *slog.Logger

	indexFeeds map[CellLevel]*event.FeedOf[[]cattrack.CatTrack]
}

type WrappedTrack cattrack.CatTrack

type Indexer interface {
	Index(old, next Indexer) Indexer
	IsEmpty() bool
}

func UnmarshalIndexer(v []byte) (Indexer, error) {
	var targetIndexCountT IndexCountT
	if err := json.Unmarshal(v, &targetIndexCountT); err == nil {
		return targetIndexCountT, nil
	}
	var targetWrappedTrack WrappedTrack
	if err := json.Unmarshal(v, &targetWrappedTrack); err == nil {
		return targetWrappedTrack, nil
	}
	// TODO: add other possible types
	return nil, fmt.Errorf("unknown type")
}

func (wt WrappedTrack) SafeSetProperties(items map[string]any) WrappedTrack {
	props := wt.Properties.Clone()
	for k, v := range items {
		props[k] = v
	}
	wt.Properties = props
	return wt
}

// Index indexes the given CatTracks into the S2 cell index(es).
// Having WrappedTrack implement the Indexer is... maybe a great idea?
// Or maybe just "merge" the index structure to the wrapped track somehow.
func (wt WrappedTrack) Index(old, next Indexer) Indexer {
	cp := wt
	nextWrapped := next.(WrappedTrack)
	nextCount := nextWrapped.Properties.MustInt("Count", 1)
	nextActivity := nextWrapped.Properties.MustString("Activity", "Unknown")
	nextTime, err := time.Parse(time.RFC3339, nextWrapped.Properties.MustString("Time", ""))
	if err != nil {
		panic(err)
	}
	if old == nil || old.IsEmpty() {
		props := map[string]any{
			"Count":                   nextCount,
			"Activity":                nextActivity,
			"FirstTime":               nextTime,
			"ActivityMode.Unknown":    nextWrapped.Properties.MustInt("ActivityMode.Unknown", 0),
			"ActivityMode.Stationary": nextWrapped.Properties.MustInt("ActivityMode.Stationary", 0),
			"ActivityMode.Walking":    nextWrapped.Properties.MustInt("ActivityMode.Walking", 0),
			"ActivityMode.Running":    nextWrapped.Properties.MustInt("ActivityMode.Running", 0),
			"ActivityMode.Bike":       nextWrapped.Properties.MustInt("ActivityMode.Bike", 0),
			"ActivityMode.Automotive": nextWrapped.Properties.MustInt("ActivityMode.Automotive", 0),
			"ActivityMode.Fly":        nextWrapped.Properties.MustInt("ActivityMode.Fly", 0),
		}
		props["ActivityMode."+nextActivity] = nextCount // eg. "Walking": 1, "Running": 1, etc.
		cp = cp.SafeSetProperties(props)
		return cp
	}

	oldWrapped := old.(WrappedTrack)
	updates := map[string]any{}

	oldCount := oldWrapped.Properties.MustInt("Count", 1)
	updates["Count"] = oldCount + nextCount

	for _, act := range []string{"Unknown", "Stationary", "Walking", "Running", "Bike", "Automotive", "Fly"} {
		oldActivityScore := oldWrapped.Properties.MustInt("ActivityMode."+act, 0)
		nextActivityScore := nextWrapped.Properties.MustInt("ActivityMode."+act, 0)
		updates["ActivityMode."+act] = oldActivityScore + nextActivityScore
	}

	oldActivityScore := oldWrapped.Properties.MustInt("ActivityMode."+nextActivity, 1)
	updates["ActivityMode."+nextActivity] = oldActivityScore + nextCount

	// Get the ActivityMode with the greatest value, and assign the activity name to the Activity prop.
	greatest := 0
	name := "Unknown"
	for _, act := range []string{"Unknown", "Stationary", "Walking", "Running", "Bike", "Automotive", "Fly"} {
		if updates["ActivityMode."+act].(int) > greatest {
			greatest = updates["ActivityMode."+act].(int)
			name = act
		}
	}
	updates["Activity"] = name

	updates["LastTime"] = nextTime

	return oldWrapped.SafeSetProperties(updates)
}

func (wt WrappedTrack) IsEmpty() bool {
	_, ok := wt.Properties["Count"]
	return !ok
}

type IndexCountT struct {
	Count int
}

func (it IndexCountT) Index(old, next Indexer) Indexer {
	if old == nil || old.IsEmpty() {
		old = IndexCountT{}
	}
	return IndexCountT{
		Count: old.(IndexCountT).Count + next.(IndexCountT).Count,
	}
}

func (it IndexCountT) IsEmpty() bool {
	return it.Count == 0
}

func NewCellIndexer(catID conceptual.CatID, root string, levels []CellLevel, batchSize int) (*CellIndexer, error) {
	if len(levels) == 0 {
		return nil, fmt.Errorf("no levels provided")
	}

	f := flat.NewFlatWithRoot(root).Joins(flat.CatsDir, catID.String())
	if err := f.MkdirAll(); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(f.Path(), s2DBName)
	db, err := bbolt.Open(dbPath, 0660, nil)
	if err != nil {
		return nil, err
	}

	indexFeeds := make(map[CellLevel]*event.FeedOf[[]cattrack.CatTrack], len(levels))
	flatFileMap := make(map[CellLevel]*flat.GZFileWriter, len(levels))

	for _, level := range levels {
		gzf, err := f.NamedGZWriter(fmt.Sprintf("s2_level-%02d.geojson.gz", level), nil)
		if err != nil {
			return nil, err
		}
		flatFileMap[level] = gzf

		indexFeeds[level] = &event.FeedOf[[]cattrack.CatTrack]{}
	}

	caches := make(map[CellLevel]*lru.Cache[string, Indexer], len(levels))
	for _, level := range levels {
		c, err := lru.New[string, Indexer](batchSize)
		if err != nil {
			return nil, err
		}
		caches[level] = c
	}

	return &CellIndexer{
		CatID:      catID,
		Levels:     levels,
		Caches:     caches,
		DB:         db,
		FlatFiles:  flatFileMap,
		BatchSize:  batchSize,
		indexFeeds: indexFeeds,
		logger:     slog.With("indexer", "s2"),
	}, nil
}

func (ci *CellIndexer) FeedOfIndexedTracksForLevel(level CellLevel) (*event.FeedOf[[]cattrack.CatTrack], error) {
	v, ok := ci.indexFeeds[level]
	if !ok {
		return nil, fmt.Errorf("level %d not found", level)
	}
	return v, nil
}

// Index indexes the given CatTracks into the S2 cell index(es), appending
// all unique tracks to flat files respective of cell level.
// It will block until the in channel closes and all batches are processed.
// It uses batches to minimize disk txes.
func (ci *CellIndexer) Index(ctx context.Context, in <-chan cattrack.CatTrack) error {
	batches := stream.Batch(ctx, nil,
		func(tracks []cattrack.CatTrack) bool {
			return len(tracks) == ci.BatchSize
		}, in)
	for batch := range batches {
		for _, level := range ci.Levels {
			if err := ci.index(level, batch); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ci *CellIndexer) index(level CellLevel, tracks []cattrack.CatTrack) error {
	start := time.Now()
	defer slog.Debug("CellIndexer batch", "cat", ci.CatID,
		"level", level, "size", len(tracks), "elapsed", time.Since(start).Round(time.Millisecond))

	mapIDNext := make(map[string]WrappedTrack)

	cache := ci.Caches[level]
	for _, ct := range tracks {
		ctIdxr := WrappedTrack(ct)
		cellID := getTrackCellID(ct, level)

		var old, next Indexer

		v, exists := cache.Get(cellID.ToToken())
		if exists {
			old = v.(Indexer)
		}
		next = ctIdxr.Index(old, ctIdxr)
		cache.Add(cellID.ToToken(), next)
	}

	err := ci.DB.Update(func(tx *bbolt.Tx) error {
		bucket := dbBucket(level)
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		for _, k := range cache.Keys() {
			cVal, ok := cache.Peek(k)
			if !ok {
				panic("cache key not found")
			}

			var old Indexer

			v := b.Get([]byte(k))

			// Non-nil value means non-unique track/index.
			if v != nil {
				r, err := gzip.NewReader(bytes.NewBuffer(v))
				if err != nil {
					return err
				}
				ungzipped := bytes.NewBuffer([]byte{})
				if _, err := ungzipped.ReadFrom(r); err != nil {
					return err
				}
				_ = r.Close()
				old, err = UnmarshalIndexer(ungzipped.Bytes())
				if err != nil {
					return err
				}
			}
			next := cVal.Index(old, cVal)
			encoded, err := json.Marshal(next)
			if err != nil {
				return err
			}
			buf := bytes.NewBuffer([]byte{})
			w, err := gzip.NewWriterLevel(buf, gzip.BestCompression)
			if err != nil {
				return err
			}
			if _, err := w.Write(encoded); err != nil {
				return err
			}
			_ = w.Close()
			if err := b.Put([]byte(k), buf.Bytes()); err != nil {
				return err
			}
			mapIDNext[k] = next.(WrappedTrack)
		}
		return nil
	})
	if err != nil {
		return err
	}

	var outTracks []cattrack.CatTrack
	for _, ct := range mapIDNext {
		outTracks = append(outTracks, cattrack.CatTrack(ct))
	}

	enc := json.NewEncoder(ci.FlatFiles[level].Writer())
	for _, ct := range outTracks {
		if err := enc.Encode(ct); err != nil {
			return err
		}
	}
	ci.indexFeeds[level].Send(outTracks)
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

//// filterAndIndexUniqCatTracks returns unique cellIDs and associated tracks for the given cat.
//// It attempts first to cross-reference all tracks against the cache. This step returns the indices of the tracks that are not in the cache.
//// Those uncached tracks are then cross-referenced against the index db, which writes them if they are unique.
//// Only the DB-access part of the process is blocking, since the cache is only a nice-to-have and we
//// don't care if some cache misses are false negatives.
//func (ci *CellIndexer) filterAndIndexUniqCatTracks(level CellLevel, tracks []cattrack.CatTrack) (uniqCellIDs []s2.CellID, uniqTracks []cattrack.CatTrack) {
//	if len(tracks) == 0 {
//		return uniqCellIDs, uniqTracks
//	}
//
//	// create a cellid slice analogous to tracks
//	cellIDs := getTrackCellIDs(tracks, level)
//	if len(cellIDs) != len(tracks) {
//		log.Fatalln("len(cellIDs) != len(tracks)", len(cellIDs), len(tracks))
//	}
//
//	// returns the indices of uniq tracks (== uniq cellIDs)
//	uniqCellIDTrackIndices := ci.uniqIndexesFromCache(level, cellIDs) // eg. 0, 23, 42, 99
//
//	// if there are no uniq cellIDs, return early
//	if len(uniqCellIDTrackIndices) == 0 {
//		return
//	}
//
//	tmpUniqCellIDs := make([]s2.CellID, len(uniqCellIDTrackIndices))
//	tmpUniqTracks := make([]cattrack.CatTrack, len(uniqCellIDTrackIndices))
//	for ii, idx := range uniqCellIDTrackIndices {
//		tmpUniqCellIDs[ii] = cellIDs[idx]
//		tmpUniqTracks[ii] = tracks[idx]
//	}
//
//	// so now we've whittled the tracks to only those not in the cache
//	// we need to check the db for those that did not have cache hits
//
//	// further whittle the uniqs based on db hits/misses
//	_uniqCellIDs, _uniqTracks, err := ci.filterUniqFromDBWriting(level, tmpUniqCellIDs, tmpUniqTracks)
//	if err != nil {
//		log.Fatalln(err)
//	}
//	return _uniqCellIDs, _uniqTracks
//}

//// filterUniqFromDBWriting filters the givens cellIDs and tracklines to those which were not found in the database.
//// The unique cells will be written before the function returns.
//// I think it's important to read+write in the same transaction, so that the db is not left in an inconsistent state
//// with multiple routines potentially accessing it.
//// I expect that bolt will lock the db for the duration of the transaction,
//// so that other routines will block until the transaction is complete.
//func (ci *CellIndexer) filterUniqFromDBWriting(level CellLevel, cellIDs []s2.CellID, tracks []cattrack.CatTrack) (uniqCellIDs []s2.CellID, uniqTracks []cattrack.CatTrack, err error) {
//	bucket := dbBucket(level)
//	err = ci.DB.Update(func(tx *bbolt.Tx) error {
//		b, err := tx.CreateBucketIfNotExists(bucket)
//		if err != nil {
//			return err
//		}
//		for i, cellID := range cellIDs {
//			v := b.Get(cellIDDBKey(cellID))
//			if v == nil {
//				uniqCellIDs = append(uniqCellIDs, cellID)
//				uniqTracks = append(uniqTracks, tracks[i])
//
//				// Write the new cell to the index.
//				err = b.Put(cellIDDBKey(cellID), []byte{0x1})
//				if err != nil {
//					return err
//				}
//			}
//		}
//		return nil
//	})
//	return
//}

//// uniqIndexesFromCache returns the indices of the given cellIDs that are not in the LRU cache.
//func (ci *CellIndexer) uniqIndexesFromCache(level CellLevel, cellIDs []s2.CellID) (uniqIndices []int) {
//	for i, cellID := range cellIDs {
//		if !ci.trackInCacheByCellID(level, cellID) {
//			uniqIndices = append(uniqIndices, i)
//		}
//	}
//	return uniqIndices
//}

// CellIDWithLevel returns the cellID truncated to the given level.
func CellIDWithLevel(cellID s2.CellID, level CellLevel) s2.CellID {
	// https://docs.s2cell.aliddell.com/en/stable/s2_concepts.html#truncation
	var lsb uint64 = 1 << (2 * (30 - level))
	truncatedCellID := (uint64(cellID) & -lsb) | lsb
	return s2.CellID(truncatedCellID)
}

// getTrackCellID returns the cellID for the given track line, which is a raw string of a JSON-encoded geojson cat track.
// It applies the global cellLevel to the returned cellID.
func getTrackCellID(ct cattrack.CatTrack, level CellLevel) s2.CellID {
	//var coords []float64
	//// Use GJSON to avoid slow unmarshalling of the entire line.
	//gjson.Get(line, "geometry.coordinates").ForEach(func(key, value gjson.Result) bool {
	//	coords = append(coords, value.Float())
	//	return true
	//})
	coords := ct.Geometry.(orb.Point)
	return CellIDWithLevel(s2.CellIDFromLatLng(s2.LatLngFromDegrees(coords[1], coords[0])), level)
}

//func getTrackCellIDs(cts []cattrack.CatTrack, level CellLevel) (cellIDs []s2.CellID) {
//	for _, ct := range cts {
//		cellIDs = append(cellIDs, getTrackCellID(ct, level))
//	}
//	return cellIDs
//}

func dbBucket(level CellLevel) []byte { return []byte{byte(level)} }

//func cellIDCacheKey(cellID s2.CellID) string {
//	return fmt.Sprintf("%s", cellID.ToToken())
//}
//func cellIDDBKey(cellID s2.CellID) []byte {
//	return []byte(cellID.ToToken())
//}

//// trackInCacheByCellID returns true if the given cellID for some cat exists in the LRU cache.
//// If not, it will be added to the cache.
//func (ci *CellIndexer) trackInCacheByCellID(level CellLevel, cellID s2.CellID) (exists bool) {
//	key := cellIDCacheKey(cellID)
//	if _, ok := ci.Caches[level].Get(key); ok {
//		return ok
//	}
//	ci.Caches[level].Add(key, true)
//	return false
//}
