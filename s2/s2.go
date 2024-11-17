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
*/
package s2

import (
	"fmt"
	"github.com/golang/geo/s2"
	"github.com/golang/groupcache/lru"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/app"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/events"
	"github.com/rotblauer/catd/types/cattrack"
	bbolt "go.etcd.io/bbolt"
	"log/slog"
	"path/filepath"
	"time"
)

const s2DBName = "s2.db"
const batchSize = 1_000

var defaultCellLevels = []CellLevel{CellLevel16, CellLevel23}

func init() {
	var newTracksCh = make(chan *cattrack.CatTrack)
	var newTracksSub = events.NewStoredTrackFeed.Subscribe(newTracksCh)

	var indexers = lru.New(100)

	type indexerCache struct {
		input     chan *cattrack.CatTrack
		indexer   *Indexer
		lastWrite time.Time
	}

	go func() {
		for {
			select {
			case err := <-newTracksSub.Err():
				slog.Error("Failed to subscribe to NewStoredTrackFeed", "error", err)
				return
			case ct := <-newTracksCh:
				slog.Info("new track", ct.StringPretty())
				indexer, ok := indexers.Get(ct.CatID())
				if !ok {
					indexer, err := NewIndexer(ct.CatID(), app.DatadirRoot, defaultCellLevels)
					if err != nil {
						slog.Error("Failed to create indexer", "error", err)
						continue
					}
					v := &indexerCache{
						input:     make(chan *cattrack.CatTrack),
						indexer:   indexer,
						lastWrite: time.Now(),
					}
					indexers.Add(ct.CatID(), v)
					go indexer.Index(v.input)
					go func() {
						t := time.NewTicker(10 * time.Second)
						for range t.C {
							if time.Since(v.lastWrite) > 10*time.Second {
								if err := v.indexer.Close(); err != nil {
									slog.Error("Failed to close indexer", "error", err)
								}
								indexers.Remove(ct.CatID())
								return
							}
						}
					}()
				}
				indexerCached := indexer.(*indexerCache)
				indexerCached.input <- ct
			}
		}
	}()
}

type Indexer struct {
	CatID     conceptual.CatID
	Cache     *lru.Cache
	Levels    []CellLevel
	DB        *bbolt.DB
	FlatFiles map[CellLevel]*flat.GZFile

	readingChan chan *cattrack.CatTrack
}

func NewIndexer(id conceptual.CatID, root string, levels []CellLevel) (*Indexer, error) {
	if len(levels) == 0 {
		return nil, fmt.Errorf("no levels provided")
	}

	f := flat.NewFlatWithRoot(root).ForCat(id)
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

	return &Indexer{
		CatID:     id,
		Levels:    levels,
		Cache:     lru.New(100_000),
		DB:        db,
		FlatFiles: flatFileMap,
	}, nil
}

func (i *Indexer) Index(in chan *cattrack.CatTrack) any {
	i.readingChan = in
	for track := range i.readingChan {

	}
	return nil
}

func (i *Indexer) FlushBatch() error {
	return nil
}

func (i *Indexer) Close() error {
	i.readingChan = nil
	if err := i.DB.Close(); err != nil {
		return err
	}
	for _, gzf := range i.FlatFiles {
		if err := gzf.Close(); err != nil {
			return err
		}
	}
	return nil
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
func (i *Indexer) trackInCacheByCellID(cellID s2.CellID) (exists bool) {
	key := cellIDCacheKey(cellID)
	if _, ok := i.Cache.Get(key); ok {
		return ok
	}
	i.Cache.Add(key, true)
	return false
}
