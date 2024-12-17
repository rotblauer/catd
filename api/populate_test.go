package api

import (
	"bytes"
	"context"
	"fmt"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/testing/testdata"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"path/filepath"
	"testing"
)

func TestSinkStreamToJSONWriter(t *testing.T) {
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
	oldLevel := slog.SetLogLoggerLevel(slog.Level(slog.LevelWarn + 1))
	defer slog.SetLogLoggerLevel(oldLevel)
	cases := []struct {
		cat      string
		gzSource string
	}{
		{"rye", testdata.Path(testdata.Source_EDGE20241217)},
		{"ia", testdata.Path(testdata.Source_EDGE20241217)},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s-%s", filepath.Base(c.gzSource), c.cat), func(t *testing.T) {
			testCat_Populate(t, c.cat, c.gzSource)
		})
	}
}

func testCat_Populate(t *testing.T, cat, source string) {
	gzr, err := catz.NewGZFileReader(source)
	if err != nil {
		t.Fatal(err)
	}
	defer gzr.Close()

	tc := NewTestCatWriter(t, cat, nil)
	c := tc.Cat()
	defer tc.CloseAndDestroy()

	ctx := context.Background()
	tracks, errs := stream.NDJSON[cattrack.CatTrack](ctx, gzr)
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

	// Populate cat with tracks.
	err = c.Populate(ctx, true, stream.Slice(ctx, catCollection))
	if err != nil {
		t.Fatal(err)
	}
}
