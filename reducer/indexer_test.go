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

func NewTestCellIndexer(t *testing.T) *CellIndexer {
	ci, err := NewCellIndexer(
		&CellIndexerConfig{
			CatID:     conceptual.CatID("any"),
			DBPath:    filepath.Join(os.TempDir(), "reducer_test.catdb"),
			BatchSize: params.DefaultBatchSize,
			Buckets:   []Bucket{3, 4, 5},
			DefaultIndexerT: &cattrack.MyReducerT{
				VisitThreshold: params.S2DefaultVisitThreshold,
			},
			LevelIndexerT: nil,
			BucketKeyFn: func(track cattrack.CatTrack, bucket Bucket) (string, error) {
				b := track.MustTime().Unix() % 100
				return fmt.Sprintf("tmod100-%02d", b), nil
			},
			Logger: slog.With("reducer_test", "s2"),
		})
	if err != nil {
		t.Fatal(err)
	}
	return ci
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

	reducer := NewTestCellIndexer(t)
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

	dump, errs := reducer.DumpLevel(reducer.Config.Buckets[0])
	out := stream.Collect[cattrack.CatTrack](ctx, dump)
	// expect 100 b/c %100 keyfn, and enough tracks to have at least one in each bucket
	if len(out) != 100 {
		t.Errorf("expected 100, got %d", len(out))
	}
	if out[0].Properties.MustString("reducer_key", "") != "tmod100-00" {
		t.Errorf("expected 00, got %s", out[0].Properties.MustString("reducer_key", ""))
	}
	err = <-errs
	if err != nil {
		t.Fatal(err)
	}
}
