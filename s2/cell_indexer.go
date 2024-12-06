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

	indexFeeds     map[CellLevel]*event.FeedOf[[]cattrack.CatTrack]
	uniqIndexFeeds map[CellLevel]*event.FeedOf[[]cattrack.CatTrack]
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
	uniqIndexFeeds := make(map[CellLevel]*event.FeedOf[[]cattrack.CatTrack], len(levels))
	flatFileMap := make(map[CellLevel]*flat.GZFileWriter, len(levels))

	for _, level := range levels {
		gzf, err := f.NamedGZWriter(fmt.Sprintf("s2_level-%02d.geojson.gz", level), nil)
		if err != nil {
			return nil, err
		}
		flatFileMap[level] = gzf

		indexFeeds[level] = &event.FeedOf[[]cattrack.CatTrack]{}
		uniqIndexFeeds[level] = &event.FeedOf[[]cattrack.CatTrack]{}
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
		CatID:          catID,
		Levels:         levels,
		Caches:         caches,
		DB:             db,
		FlatFiles:      flatFileMap,
		BatchSize:      batchSize,
		indexFeeds:     indexFeeds,
		uniqIndexFeeds: uniqIndexFeeds,
		logger:         slog.With("indexer", "s2"),
	}, nil
}

func (ci *CellIndexer) FeedOfIndexedTracksForLevel(level CellLevel) (*event.FeedOf[[]cattrack.CatTrack], error) {
	v, ok := ci.indexFeeds[level]
	if !ok {
		return nil, fmt.Errorf("level %d not found", level)
	}
	return v, nil
}

func (ci *CellIndexer) FeedOfUniqueTracksForLevel(level CellLevel) (*event.FeedOf[[]cattrack.CatTrack], error) {
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
	mapIDUnique := make(map[string]WrappedTrack)

	cache := ci.Caches[level]
	for _, ct := range tracks {
		ctIdxr := WrappedTrack(ct)
		cellID := CellIDForTrackLevel(ct, level)

		var old, next Indexer

		v, exists := cache.Get(cellID.ToToken())
		if exists {
			old = v.(Indexer)
		}
		next = ctIdxr.Index(old, ctIdxr)

		// Overwrite the unique cache with the new value.
		// This will let us send the latest version of unique cells.
		mapIDUnique[cellID.ToToken()] = next.(WrappedTrack)

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
				// Strike this value from the unique map.
				delete(mapIDUnique, k)

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
	ci.indexFeeds[level].Send(outTracks)

	var uniqTracks []cattrack.CatTrack
	for _, ct := range mapIDUnique {
		uniqTracks = append(uniqTracks, cattrack.CatTrack(ct))
	}
	ci.uniqIndexFeeds[level].Send(uniqTracks)

	enc := json.NewEncoder(ci.FlatFiles[level].Writer())
	for _, ct := range uniqTracks {
		if err := enc.Encode(ct); err != nil {
			return err
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

func (ci *CellIndexer) DumpLevel(level CellLevel) (chan cattrack.CatTrack, chan error) {
	out := make(chan cattrack.CatTrack)
	errs := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errs)

		err := ci.DB.View(func(tx *bbolt.Tx) error {
			bucket := dbBucket(level)
			b := tx.Bucket(bucket)
			if b == nil {
				return fmt.Errorf("bucket not found")
			}
			return b.ForEach(func(k, v []byte) error {
				r, err := gzip.NewReader(bytes.NewBuffer(v))
				if err != nil {
					return err
				}
				ungzipped := bytes.NewBuffer([]byte{})
				if _, err := ungzipped.ReadFrom(r); err != nil {
					return err
				}
				_ = r.Close()
				wt := WrappedTrack{}
				if err := json.Unmarshal(ungzipped.Bytes(), &wt); err != nil {
					return err
				}
				out <- cattrack.CatTrack(wt)
				return nil
			})
		})
		if err != nil {
			errs <- err
			return
		}
	}()

	return out, errs
}

// CellIDWithLevel returns the cellID truncated to the given level.
// https://docs.s2cell.aliddell.com/en/stable/s2_concepts.html#truncation
func CellIDWithLevel(cellID s2.CellID, level CellLevel) s2.CellID {
	var lsb uint64 = 1 << (2 * (30 - level))
	truncatedCellID := (uint64(cellID) & -lsb) | lsb
	return s2.CellID(truncatedCellID)
}

// CellIDForTrackLevel returns the cellID at some level for the given track.
func CellIDForTrackLevel(ct cattrack.CatTrack, level CellLevel) s2.CellID {
	coords := ct.Geometry.(orb.Point)
	return CellIDWithLevel(s2.CellIDFromLatLng(s2.LatLngFromDegrees(coords[1], coords[0])), level)
}

func dbBucket(level CellLevel) []byte { return []byte{byte(level)} }
