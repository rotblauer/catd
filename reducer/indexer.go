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
package reducer

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/event"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	bbolt "go.etcd.io/bbolt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// A Bucket, a CellLevel, and a Dataset walk into a bar.
type Bucket int

type CellIndexer struct {
	Config *CellIndexerConfig

	Caches map[Bucket]*lru.Cache[string, cattrack.Indexer]
	DB     *bbolt.DB

	Waiting sync.WaitGroup

	logger         *slog.Logger
	indexFeeds     map[Bucket]*event.FeedOf[[]cattrack.CatTrack]
	uniqIndexFeeds map[Bucket]*event.FeedOf[[]cattrack.CatTrack]
}

type CellIndexerConfig struct {
	CatID     conceptual.CatID // logging
	DBPath    string
	BatchSize int

	Buckets []Bucket

	// DefaultIndexerT is (a value of) the type of the Indexer implementation.
	// The Indexer defines logic about how merge non-unique cat tracks.
	// The default is used if no level-specific Indexer is provided.
	DefaultIndexerT cattrack.Indexer
	LevelIndexerT   map[Bucket]cattrack.Indexer // TODO: Slices instead?
	BucketKeyFn     CatKeyFn

	Logger *slog.Logger
}

// ErrNoKeyFound should be returned by a CatKeyFn if no key is found.
var ErrNoKeyFound = errors.New("no key found")

type CatKeyFn func(track cattrack.CatTrack, bucket Bucket) (string, error)

func NewCellIndexer(config *CellIndexerConfig) (*CellIndexer, error) {

	if len(config.Buckets) == 0 {
		return nil, fmt.Errorf("no buckets provided")
	}
	if config.DefaultIndexerT == nil {
		config.DefaultIndexerT = &cattrack.StackerV1{}
	}

	if err := os.MkdirAll(filepath.Dir(config.DBPath), 0777); err != nil {
		return nil, err
	}
	db, err := bbolt.Open(config.DBPath, 0660, nil)
	if err != nil {
		return nil, err
	}

	indexFeeds := make(map[Bucket]*event.FeedOf[[]cattrack.CatTrack], len(config.Buckets))
	uniqIndexFeeds := make(map[Bucket]*event.FeedOf[[]cattrack.CatTrack], len(config.Buckets))

	for _, bucket := range config.Buckets {
		//gzf, err := f.NewGZFileWriter(fmt.Sprintf("s2_level-%02d.geojson.gz", level), nil)
		//if err != nil {
		//	return nil, err
		//}
		//flatFileMap[level] = gzf

		indexFeeds[bucket] = &event.FeedOf[[]cattrack.CatTrack]{}
		uniqIndexFeeds[bucket] = &event.FeedOf[[]cattrack.CatTrack]{}
	}

	logger := slog.With("reducer", "a")
	if config.Logger == nil {
		logger = config.Logger
	}

	return &CellIndexer{
		Config: config,
		Caches: make(map[Bucket]*lru.Cache[string, cattrack.Indexer], len(config.Buckets)),
		DB:     db,

		indexFeeds:     indexFeeds,
		uniqIndexFeeds: uniqIndexFeeds,
		logger:         logger,
	}, nil
}

func (ci *CellIndexer) indexerTForBucket(level Bucket) cattrack.Indexer {
	if ci.Config.LevelIndexerT != nil {
		if v, ok := ci.Config.LevelIndexerT[level]; ok {
			return v
		}
	}
	return ci.Config.DefaultIndexerT
}

func (ci *CellIndexer) FeedOfIndexedTracksForBucket(level Bucket) (*event.FeedOf[[]cattrack.CatTrack], error) {
	v, ok := ci.indexFeeds[level]
	if !ok {
		return nil, fmt.Errorf("level %d not found", level)
	}
	return v, nil
}

func (ci *CellIndexer) FeedOfUniqueTracksForBucket(level Bucket) (*event.FeedOf[[]cattrack.CatTrack], error) {
	v, ok := ci.indexFeeds[level]
	if !ok {
		return nil, fmt.Errorf("level %d not found", level)
	}
	return v, nil
}

// Index indexes the given CatTracks into the S2 cell index(es), appending
// all unique tracks to flat files respective of cell level.
// It will block until the in channel closes and all batches are processed.
// It uses batches to constrain disk txes, and to periodically flush data to disk.
func (ci *CellIndexer) Index(ctx context.Context, in <-chan cattrack.CatTrack) error {
	batches := stream.Batch(ctx, nil,
		func(tracks []cattrack.CatTrack) bool {
			return len(tracks) == ci.Config.BatchSize
		}, stream.Buffered(ctx, in, params.DefaultBatchSize))
	batchIndex := 0
	n := params.DefaultBatchSize / ci.Config.BatchSize // eg. 100_000 / 10_000 = 10
	for batch := range batches {
		if batchIndex%n == 0 {
			ci.logger.Debug("Reducer indexing batch", "cat", ci.Config.CatID, "batch.index", batchIndex,
				"size", len(batch), "buckets", len(ci.Config.Buckets))
		}
		batchIndex++
		for _, level := range ci.Config.Buckets {
			if err := ci.index(level, batch); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ci *CellIndexer) index(level Bucket, tracks []cattrack.CatTrack) error {
	start := time.Now()
	defer slog.Debug("Reducer batch", "cat", ci.Config.CatID,
		"bucket", level, "size", len(tracks), "elapsed", time.Since(start).Round(time.Millisecond))

	// Reinit the level's cache.
	cache, err := lru.New[string, cattrack.Indexer](ci.Config.BatchSize)
	if err != nil {
		return err
	}
	ci.Caches[level] = cache

	indexT := ci.indexerTForBucket(level)

	mapIDUnique := make(map[string]cattrack.CatTrack)

	for _, ct := range tracks {
		ct := ct
		key, err := ci.Config.BucketKeyFn(ct, level)
		if errors.Is(err, ErrNoKeyFound) || key == "" {
			ci.logger.Debug("No indexer key for cattrack, skipping", "track", ct.StringPretty(), "level", level)
			continue
		}
		ct.SetPropertySafe("reducer_key", key)

		var old, next cattrack.Indexer

		v, exists := cache.Get(key)
		if exists {
			old = v
		}

		ict := indexT.FromCatTrack(ct)
		next = indexT.Index(old, ict)
		if next == nil {
			panic("indexer method Index returned nil Indexer")
		}

		cache.Add(key, next)

		nextTrack := indexT.ApplyToCatTrack(next, ct)

		// Overwrite the unique cache with the new value.
		// This sends the latest version of unique cells.
		mapIDUnique[key] = nextTrack
	}

	var outTracks []cattrack.CatTrack

	gzr := new(gzip.Reader)
	gzw := gzip.NewWriter(new(bytes.Buffer))
	defer gzw.Close()

	err = ci.DB.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte{byte(level)})
		if err != nil {
			return err
		}
		for _, k := range cache.Keys() {
			nextIdxr, ok := cache.Peek(k)
			if !ok {
				panic("cache key not found")
			}

			track := mapIDUnique[k]

			var old cattrack.Indexer
			v := b.Get([]byte(k))

			// Non-nil value means non-unique track/index.
			if v != nil {

				// Strike this value from the unique map.
				delete(mapIDUnique, k)

				ct := cattrack.CatTrack{}

				in := bytes.NewBuffer(v)
				err := gzr.Reset(in)
				if err != nil {
					return fmt.Errorf("gzip reader: %w", err)
				}
				err = json.NewDecoder(gzr).Decode(&ct)
				if err != nil {
					return fmt.Errorf("json decode read: %w %d %s",
						err, len(v), string(v))
				}
				err = gzr.Close()
				if err != nil {
					return fmt.Errorf("gzip reader close: %w", err)
				}

				old = indexT.FromCatTrack(ct)
			}

			next := indexT.Index(old, nextIdxr)
			nextTrack := indexT.ApplyToCatTrack(next, track)
			outTracks = append(outTracks, nextTrack)

			encoded, err := json.Marshal(nextTrack)
			if err != nil {
				return fmt.Errorf("json marshal write: %w", err)
			}

			out := new(bytes.Buffer)
			gzw.Reset(out)
			gzw.Write(encoded)
			err = gzw.Close()
			if err != nil {
				return fmt.Errorf("gzip close: %w", err)
			}
			err = b.Put([]byte(k), out.Bytes())
			if err != nil {
				return fmt.Errorf("bbolt put: %w", err)
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
func (ci *CellIndexer) DumpLevel(level Bucket) (chan cattrack.CatTrack, chan error) {
	out := make(chan cattrack.CatTrack, params.DefaultBatchSize)
	errs := make(chan error, 2)
	go func() {
		defer close(out)
		defer close(errs)

		r1 := new(gzip.Reader)
		defer r1.Close()

		err := ci.DB.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte{byte(level)})
			if b == nil {
				return fmt.Errorf("bucket not found")
			}
			if err := b.ForEach(func(k, v []byte) error {
				ct := cattrack.CatTrack{}
				buf := bytes.NewBuffer(v)
				err := r1.Reset(buf)
				if err != nil {
					return fmt.Errorf("failed to create gzip reader: %w", err)
				}
				if err := json.NewDecoder(r1).Decode(&ct); err != nil {
					return fmt.Errorf("failed to unmarshal JSON: %w", err)
				}
				err = r1.Close()
				if err != nil {
					return fmt.Errorf("failed to close gzip reader: %w", err)
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
