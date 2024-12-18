package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/testing/testdata"
	"github.com/rotblauer/catd/types/cattrack"
	"path/filepath"
	"testing"
)

func TestSinkStreamToJSONWriter(t *testing.T) {
	ctx := context.Background()
	tracks, errs := testdata.ReadSourceGZ[cattrack.CatTrack](ctx, testdata.Path(testdata.Source_EDGE20241217))

	first := <-tracks
	if first.IsEmpty() {
		t.Fatal("no tracks")
	}
	t.Log("stream ok", first.StringPretty())

	all := stream.Collect(ctx, tracks)
	want := len(all)

	buf := new(bytes.Buffer)
	n, err := sinkStreamToJSONWriter(ctx, buf, stream.Slice(ctx, all))
	if err != nil {
		t.Log(n)
		t.Error(err)
	}
	if n != want {
		t.Errorf("got %d, want %d", n, want)
	}
	if buf.Len() == 0 {
		t.Error("empty buffer")
	}
	err = <-errs
	if err != nil {
		t.Fatal(err)
	}
}

func TestSinkStreamToJSONGZWriter(t *testing.T) {
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

	all := stream.Collect(ctx, tracks)
	want := len(all)

	buf := new(bytes.Buffer)
	n, err := sinkStreamToJSONGZWriter(ctx, buf, stream.Slice(ctx, all))
	if err != nil {
		t.Log(n)
		t.Error(err)
	}
	if n != want {
		t.Errorf("got %d, want %d", n, want)
	}
	if buf.Len() == 0 {
		t.Error("empty buffer")
	}
	err = <-errs
	if err != nil {
		t.Fatal(err)
	}
}

func TestCat_Populate(t *testing.T) {
	//oldLevel := slog.SetLogLoggerLevel(slog.Level(slog.LevelWarn + 1))
	//defer slog.SetLogLoggerLevel(oldLevel)
	cases := []struct {
		cat      string
		gzSource string
		// storedCount is all the expected tracks (past validation, deduping, sorting, stamping).
		storedCount int
		// Expected values are read from SimpleIndexer, an example OffsetIndexerT implementation
		// plugged in to the Producers pipeline.
		// NOT ALL STORED TRACKS GO TO THE PRODUCER PIPELINE; only good ones.
		producersCount int
	}{
		{"rye", testdata.Path(testdata.Source_EDGE20241217), 23775, 23561},
		{"ia", testdata.Path(testdata.Source_EDGE20241217), 23194, 22422},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s-%s", filepath.Base(c.gzSource), c.cat), func(t *testing.T) {
			testCat_Populate(t, c.cat, c.gzSource, c.storedCount, c.producersCount)
		})
	}
}

func testCat_Populate(t *testing.T, cat, source string, wantStoreCount, wantProdCount int) {
	//opdbs := params.DefaultBatchSize
	//params.DefaultBatchSize = 100
	//defer func() {
	//	params.DefaultBatchSize = opdbs
	//}()

	tc := NewTestCatWriter(t, cat, nil)
	c := tc.Cat()
	defer tc.CloseAndDestroy()

	ctx := context.Background()
	tracks, errs := testdata.ReadSourceGZ[cattrack.CatTrack](ctx, source)
	stream.Blackhole(errs)

	// Collect and count cat's tracks.
	catTracks := stream.Filter(ctx, func(track cattrack.CatTrack) bool {
		return track.CatID() == conceptual.CatID(cat)
	}, tracks)
	catCollection := stream.Collect[cattrack.CatTrack](ctx, catTracks)
	catTracksLen := len(catCollection)
	if catTracksLen == 0 {
		t.Fatal("no tracks")
	}
	t.Log("Count testdata cat tracks", catTracksLen)

	// Populate cat with tracks.
	err := c.Populate(ctx, true, stream.Slice(ctx, catCollection))
	if err != nil {
		t.Fatal(err)
	}

	// Gotcha: Populate will have closed the state. Must reopen.
	_, err = c.WithState(true)
	if err != nil {
		t.Fatal(err)
	}

	defer c.State.Close()
	old := &cattrack.OffsetIndexT{}
	if err := c.State.ReadKVUnmarshalJSON([]byte("state"), []byte("offsetIndexer"), old); err != nil {
		c.logger.Warn("Did not read offsetIndexer state (new cat?)", "error", err)
	}
	j, _ := json.MarshalIndent(old, "", "  ")
	if old.Count != wantProdCount {
		t.Errorf("got %d, want %d", old.Count, wantProdCount)
		t.Log(string(j))
	}

	f, err := c.State.Flat.Joins("tracks").NamedGZReader("2024-12.geojson.gz")
	if err != nil {
		t.Fatal(err)
	}
	got, err := f.LineCount()
	if err != nil {
		t.Fatal(err)
	}
	if got != wantStoreCount {
		t.Errorf("got %d, want %d", got, wantStoreCount)
	}

}
