package api

import (
	"context"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/testing/testdata"
	"github.com/rotblauer/catd/types/cattrack"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCat_StoreTracks(t *testing.T) {
	tc := NewTestCatWriter(t, "rye", nil)
	c := tc.Cat()
	defer tc.CloseAndDestroy()

	ctx := context.Background()
	tracks, errs := testdata.ReadSourceJSONGZ[cattrack.CatTrack](ctx, testdata.Path(testdata.Source_RYE202412))

	first := <-tracks
	if first.IsEmpty() {
		t.Fatal("no tracks")
	}
	t.Log("stream ok", first.StringPretty())

	err := <-c.StoreTracks(ctx, tracks)
	if err != nil {
		t.Fatal(err)
	}
	err = <-errs
	if err != nil {
		t.Fatal(err)
	}

	assertGZFileValidTracks(t, filepath.Join(c.State.Flat.Path(), params.LastTracksGZFileName))
	assertGZFileValidTracks(t, filepath.Join(c.State.Flat.Path(), params.TracksGZFileName))
}

func TestCat_StoreTracksYYYYMM(t *testing.T) {
	tc := NewTestCatWriter(t, "rye", nil)
	c := tc.Cat()
	defer tc.CloseAndDestroy()

	ctx := context.Background()
	tracks, errs := testdata.ReadSourceJSONGZ[cattrack.CatTrack](ctx, testdata.Path(testdata.Source_RYE202412))

	first := <-tracks
	if first.IsEmpty() {
		t.Fatal("no tracks")
	}
	t.Log("stream ok", first.StringPretty())

	err := <-c.StoreTracksYYYYMM(ctx, tracks)
	if err != nil {
		t.Fatal(err)
	}
	err = <-errs
	if err != nil {
		t.Fatal(err)
	}

	assertGZFileValidTracks(t, filepath.Join(c.State.Flat.Path(), params.LastTracksGZFileName))
	assertGZFileValidTracks(t, filepath.Join(c.State.Flat.Path(), "tracks", "2024-12.geojson.gz"))
}

// TestCat_StoreTracksYYYYMM_Heavy pushes the track time of 444k real tracks ahead
// by an hour, drawing the YYYY-MM far into the future, causing lots of file write/closes.
func TestCat_StoreTracksYYYYMM_Heavy(t *testing.T) {
	tc := NewTestCatWriter(t, "rye", nil)
	c := tc.Cat()
	defer tc.CloseAndDestroy()

	ctx := context.Background()
	tracks, errs := testdata.ReadSourceJSONGZ[cattrack.CatTrack](ctx, testdata.Path(testdata.Source_RYE202412))

	firstFakeTime := time.Now().Round(time.Second)
	fakeTime := firstFakeTime
	tracksToFuture := stream.Transform(ctx, func(ct cattrack.CatTrack) cattrack.CatTrack {
		// Just a sanity check of our input tracks.
		if !ct.IsValid() {
			t.Fatal("invalid track", ct)
		}
		// Bump time ahead by 1 hour.
		// Must set both Time and UnixTime, cattrack.CatTrack.Time() will prefer the latter.
		ct.SetPropertySafe("Time", fakeTime)
		ct.SetPropertySafe("UnixTime", fakeTime.Unix())
		fakeTime = fakeTime.Add(1 * time.Hour)
		return ct
	}, tracks)

	first := <-tracksToFuture
	if first.IsEmpty() {
		t.Fatal("no tracks")
	}
	t.Log("stream ok", first.StringPretty())
	if !first.MustTime().Equal(firstFakeTime) {
		t.Fatal("time not set", first.MustTime(), firstFakeTime)
	}

	err := <-c.StoreTracksYYYYMM(ctx, tracksToFuture)
	if err != nil {
		t.Fatal(err)
	}
	err = <-errs
	if err != nil {
		t.Fatal(err)
	}

	assertGZFileValidTracks(t, filepath.Join(c.State.Flat.Path(), params.LastTracksGZFileName))
	matches, err := filepath.Glob(filepath.Join(c.State.Flat.Path(), "tracks", "*.geojson.gz"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) < 14 {
		t.Fatal("no track files")
	}
	foundFirst, foundFuture := false, false
	for _, path := range matches {
		if strings.Contains(path, filepath.Join("tracks", firstFakeTime.Format("2006-01")+".geojson.gz")) {
			foundFirst = true
		}
		if strings.Contains(path, filepath.Join("tracks", fakeTime.Format("2006-01")+".geojson.gz")) {
			foundFuture = true
		}
		assertGZFileValidTracks(t, path)
	}
	if !foundFirst || !foundFuture {
		t.Fatal("track files missing")
	}
	t.Log("found all track files",
		len(matches), matches[0], "...", matches[len(matches)-1])
}
