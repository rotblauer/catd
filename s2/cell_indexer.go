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
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	bbolt "go.etcd.io/bbolt"
	"io"
	"log"
	"log/slog"
	"path/filepath"
	"sync"
	"time"
)

type CellIndexer struct {
	Config *CellIndexerConfig

	Caches map[CellLevel]*lru.Cache[string, Indexer]
	DB     *bbolt.DB

	Waiting sync.WaitGroup

	logger         *slog.Logger
	indexFeeds     map[CellLevel]*event.FeedOf[[]cattrack.CatTrack]
	uniqIndexFeeds map[CellLevel]*event.FeedOf[[]cattrack.CatTrack]
}

type CellIndexerConfig struct {
	CatID     conceptual.CatID
	Flat      *flat.Flat
	Levels    []CellLevel
	BatchSize int

	// DefaultIndexerT is (a value of) the type of the Indexer implementation.
	// The Indexer defines logic about how merge non-unique cat tracks.
	// The default is used if no level-specific Indexer is provided.
	DefaultIndexerT Indexer
	LevelIndexerT   map[CellLevel]Indexer
}

func NewCellIndexer(config *CellIndexerConfig) (*CellIndexer, error) {

	if len(config.Levels) == 0 {
		return nil, fmt.Errorf("no levels provided")
	}
	if config.DefaultIndexerT == nil {
		config.DefaultIndexerT = DefaultIndexerT
	}

	if err := config.Flat.MkdirAll(); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(config.Flat.Path(), DBName)
	db, err := bbolt.Open(dbPath, 0660, nil)
	if err != nil {
		return nil, err
	}

	indexFeeds := make(map[CellLevel]*event.FeedOf[[]cattrack.CatTrack], len(config.Levels))
	uniqIndexFeeds := make(map[CellLevel]*event.FeedOf[[]cattrack.CatTrack], len(config.Levels))

	for _, level := range config.Levels {
		//gzf, err := f.NamedGZWriter(fmt.Sprintf("s2_level-%02d.geojson.gz", level), nil)
		//if err != nil {
		//	return nil, err
		//}
		//flatFileMap[level] = gzf

		indexFeeds[level] = &event.FeedOf[[]cattrack.CatTrack]{}
		uniqIndexFeeds[level] = &event.FeedOf[[]cattrack.CatTrack]{}
	}

	return &CellIndexer{
		Config: config,
		Caches: make(map[CellLevel]*lru.Cache[string, Indexer], len(config.Levels)),
		DB:     db,

		indexFeeds:     indexFeeds,
		uniqIndexFeeds: uniqIndexFeeds,
		logger:         slog.With("indexer", "s2"),
	}, nil
}

func (ci *CellIndexer) indexerTForLevel(level CellLevel) Indexer {
	if ci.Config.LevelIndexerT != nil {
		if v, ok := ci.Config.LevelIndexerT[level]; ok {
			return v
		}
	}
	return ci.Config.DefaultIndexerT
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
			return len(tracks) == ci.Config.BatchSize
		}, stream.Buffered(ctx, in, params.DefaultBufferSize))
	batchIndex := 0
	n := params.DefaultBatchSize / ci.Config.BatchSize // eg. 100_000 / 10_000 = 10
	for batch := range batches {
		if batchIndex%n == 0 {
			ci.logger.Debug("S2 Indexing batch", "cat", ci.Config.CatID, "batch.index", batchIndex,
				"size", len(batch), "levels", len(ci.Config.Levels))
		}
		batchIndex++
		for _, level := range ci.Config.Levels {
			if err := ci.index(level, batch); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ci *CellIndexer) index(level CellLevel, tracks []cattrack.CatTrack) error {
	start := time.Now()
	defer slog.Debug("CellIndexer batch", "cat", ci.Config.CatID,
		"level", level, "size", len(tracks), "elapsed", time.Since(start).Round(time.Millisecond))

	// Reinit the level's cache.
	cache, err := lru.New[string, Indexer](ci.Config.BatchSize)
	if err != nil {
		return err
	}
	ci.Caches[level] = cache

	indexT := ci.indexerTForLevel(level)

	mapIDUnique := make(map[string]cattrack.CatTrack)

	for _, ct := range tracks {
		cellID := CellIDForTrackLevel(ct, level)

		var old, next Indexer

		v, exists := cache.Get(cellID.ToToken())
		if exists {
			old = v
		}

		// FIXME Converting and asserting the Indexer type makes this non-generic.
		// Use reflect or tags or something to
		// be able to handle any Indexer interface implementation.
		ict := indexT.FromCatTrack(ct)
		next = indexT.Index(old, ict)

		cache.Add(cellID.ToToken(), next)

		if next == nil {
			log.Fatalln("next is nil", ict.IsEmpty())
		}
		nextTrack := indexT.ApplyToCatTrack(next, ct)

		// Overwrite the unique cache with the new value.
		// This will let us send the latest version of unique cells.
		mapIDUnique[cellID.ToToken()] = nextTrack
	}

	var outTracks []cattrack.CatTrack

	err = ci.DB.Update(func(tx *bbolt.Tx) error {
		bucket := dbBucket(level)
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		for _, k := range cache.Keys() {
			nextIdxr, ok := cache.Peek(k)
			if !ok {
				panic("cache key not found")
			}

			track := mapIDUnique[k]

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

				ct := cattrack.CatTrack{}
				err = json.Unmarshal(ungzipped.Bytes(), &ct)
				if err != nil {
					return err
				}

				old = indexT.FromCatTrack(ct)
			}

			next := indexT.Index(old, nextIdxr)
			nextTrack := indexT.ApplyToCatTrack(next, track)
			outTracks = append(outTracks, nextTrack)

			encoded, err := json.Marshal(nextTrack)
			if err != nil {
				return err
			}
			buf := bytes.NewBuffer([]byte{})
			w, err := gzip.NewWriterLevel(buf, params.DefaultGZipCompressionLevel)
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
		}
		return nil
	})
	if err != nil {
		return err
	}

	ci.indexFeeds[level].Send(outTracks)

	var uniqTracks []cattrack.CatTrack
	for _, ct := range mapIDUnique {
		uniqTracks = append(uniqTracks, ct)
	}
	ci.uniqIndexFeeds[level].Send(uniqTracks)

	// Optionally, write (append) the unique (first) tracks to a flat file.
	//enc := json.NewEncoder(ci.FlatFiles[level].Writer())
	//for _, ct := range uniqTracks {
	//	if err := enc.Encode(ct); err != nil {
	//		return err
	//	}
	//}

	return nil
}

func (ci *CellIndexer) Close() error {
	if err := ci.DB.Close(); err != nil {
		return err
	}
	return nil
}

// DumpLevel dumps all unique CatTracks for the given level.
// It returns a channel of CatTracks and a channel of errors.
// Only non-nil errors are sent.
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

			if err := b.ForEach(func(k, v []byte) error {

				ungzipped := bytes.NewBuffer([]byte{})
				r, err := gzip.NewReader(bytes.NewBuffer(v))
				if err != nil {
					return err
				}
				if _, err := ungzipped.ReadFrom(r); err != nil {
					if err != io.EOF {
						return err
					}
				}
				_ = r.Close()

				ct := cattrack.CatTrack{}
				if err := json.Unmarshal(ungzipped.Bytes(), &ct); err != nil {
					return err
				}
				out <- ct
				return nil
			}); err != nil {
				return err
			}
			return nil
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
