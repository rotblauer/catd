package reducer

import (
	"context"
	"fmt"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/testing/testdata"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func myNewTestCellIndexer(t *testing.T) *CellIndexer {
	ci, err := NewCellIndexer(
		&CellIndexerConfig{
			CatID:           conceptual.CatID("any"),
			DBPath:          filepath.Join(os.TempDir(), "reducer_test.catdb"),
			BatchSize:       params.DefaultBatchSize,
			Buckets:         myBuckets,
			DefaultIndexerT: myIndexers[0],
			LevelIndexerT:   myIndexers,
			BucketKeyFn:     myBucketKeyFn,
			Logger:          slog.With("reducer_test", "s2"),
		})
	if err != nil {
		t.Fatal(err)
	}
	return ci
}

var myBuckets = []Bucket{3, 4, 5}

var myIndexers = map[Bucket]cattrack.Indexer{
	3: &cattrack.MyReducerT{},
	4: &cattrack.MyReducerT{},
	5: nil, // fallback to default
}

func myBucketKeyFn(track cattrack.CatTrack, bucket Bucket) (string, error) {
	switch bucket {
	case 3:
		b := track.MustTime().Unix() % 100
		return fmt.Sprintf("tmod100-%02d", b), nil
	case 4:
		b := track.MustTime().Unix() % 10
		return fmt.Sprintf("tmod10-%d", b), nil
	case 5:
		b := track.MustTime().Unix() % 2
		return fmt.Sprintf("tmod2-%d", b), nil
	default:
		return "", fmt.Errorf("unexpected bucket")
	}
}

func TestCellIndexer(t *testing.T) {
	testdataPathGZ := testdata.Path(testdata.Source_EDGE20241217)
	gzr, err := catz.NewGZFileReader(testdataPathGZ)
	if err != nil {
		t.Fatal(err)
	}
	defer gzr.Close()

	ctx := context.Background()
	tracks, errs := stream.NDJSON[cattrack.CatTrack](ctx, gzr)

	first := <-tracks
	if first.IsEmpty() {
		t.Fatal("no tracks")
	}
	t.Log("stream ok", first.StringPretty())

	reducer := myNewTestCellIndexer(t)
	defer os.Remove(reducer.Config.DBPath)
	defer reducer.Close()

	err = reducer.Index(ctx, tracks)
	if err != nil {
		t.Fatal(err)
	}
	err = <-errs
	if err != nil {
		t.Fatal(err)
	}

	for _, level := range reducer.Config.Buckets {

		dump, errs := reducer.DumpLevel(level)

		go func(errs chan error, level Bucket) {
			if err := <-errs; err != nil {
				t.Error(fmt.Errorf("%w: level %d", err, level))
			}
		}(errs, level)

		indexed := stream.Collect[cattrack.CatTrack](ctx, dump)

		// Check how many tracks are indexed at each level.
		// This assumes that there exist enough/sufficient tracks to fill these mod levels.
		switch level {
		case 3:
			if len(indexed) != 100 {
				t.Errorf("expected 100, got %d", len(indexed))
			}
		case 4:
			if len(indexed) != 10 {
				t.Errorf("expected 10, got %d", len(indexed))
			}
		case 5:
			if len(indexed) != 2 {
				t.Errorf("expected 2, got %d", len(indexed))
			}
		}

		for _, track := range indexed {
			keyWant, _ := myBucketKeyFn(track, level)
			keyGot := track.Properties.MustString("reducer_key", "")
			if keyGot != keyWant {
				t.Errorf("expected %s, got %s", keyWant, keyGot)
			}
			if track.IsEmpty() {
				t.Error("empty track")
			}
			if !track.IsValid() {
				t.Errorf("invalid track: %v", track.Validate())
			}
			if track.Geometry == nil || track.Geometry.GeoJSONType() != "Point" {
				t.Errorf("invalid geometry: %v", track.Geometry)
			}
		}
	}
}
