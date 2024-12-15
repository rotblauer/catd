package reducer

import (
	"context"
	"fmt"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func NewTestCellIndexer(t *testing.T) *CellIndexer {
	ci, err := NewCellIndexer(
		&CellIndexerConfig{
			CatID:     conceptual.CatID("rye"),
			DBPath:    filepath.Join(os.TempDir(), "reducer.catdb_test"),
			BatchSize: params.DefaultBatchSize,
			Buckets:   []Bucket{3, 4, 5},
			DefaultIndexerT: &cattrack.StackerV1{
				VisitThreshold: params.S2DefaultVisitThreshold,
			},
			LevelIndexerT: nil,
			BucketKeyFn: func(track cattrack.CatTrack, bucket Bucket) (string, error) {
				b := track.MustTime().Unix() % 100
				return fmt.Sprintf("%d", b), nil
			},
			Logger: slog.With("reducer_test", "s2"),
		})
	if err != nil {
		t.Fatal(err)
	}
	return ci
}

func TestCellIndexerIndex(t *testing.T) {
	testdataPathGZ := "../testing/testdata/private/2024-09-0_rye.json.gz"
	gzr, err := catz.NewGZFileReader(testdataPathGZ)
	if err != nil {
		t.Fatal(err)
	}
	defer gzr.Close()

	ctx := context.Background()
	tracks := stream.NDJSON[cattrack.CatTrack](ctx, gzr)

	first := <-tracks
	if first.IsEmpty() {
		t.Fatal("no tracks")
	}
	t.Log(first.StringPretty())

	reducer := NewTestCellIndexer(t)
	defer reducer.Close()

	err = reducer.Index(ctx, tracks)
	if err != nil {
		t.Fatal(err)
	}
}
