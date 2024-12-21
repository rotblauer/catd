package api

import (
	"context"
	"fmt"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/testing/testdata"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
)

func TestCat_Validate(t *testing.T) {
	defer common.SlogResetLevel(slog.Level(slog.LevelError + 1))()

	cases := []struct {
		cat      string
		gzSource string
	}{
		{"rye", testdata.Path(testdata.Source_EDGE20241217)},
		{"ia", testdata.Path(testdata.Source_EDGE20241217)},
		{"rye", testdata.Path(testdata.Source_RYE202412)},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s-%s", filepath.Base(c.gzSource), c.cat), func(t *testing.T) {
			testCat_Validate(t, c.cat, c.gzSource)
		})
	}
}

func testCat_Validate(t *testing.T, cat, gzSource string) {
	gzr, err := catz.NewGZFileReader(gzSource)
	if err != nil {
		t.Fatal(err)
	}
	defer gzr.Close()

	tc := NewTestCatWriter(t, cat, nil)
	c := tc.Cat()
	defer tc.CloseAndDestroy()

	ctx := context.Background()
	tracks, errs := stream.NDJSON[cattrack.CatTrack](ctx, gzr)

	// Collect and count cat's tracks.
	catTracks := stream.Filter(ctx, func(track cattrack.CatTrack) bool {
		return track.CatID() == conceptual.CatID(cat)
	}, tracks)
	catCollection := stream.Collect[cattrack.CatTrack](ctx, catTracks)
	catTracksLen := len(catCollection)
	if catTracksLen == 0 {
		t.Fatal("no tracks")
	}

	// Add some invalid tracks.
	empty := cattrack.NewCatTrack(orb.Point{42, -42})
	wrongCat := catCollection[0].Copy()
	wrongCat.SetPropertySafe("Name", "wrongCat")
	badUUID := catCollection[0].Copy()
	badUUID.SetPropertySafe("UUID", 42) // Must be string.
	badTime := catCollection[0].Copy()
	badTime.SetPropertySafe("Time", "badTime") // Must be time.Time.
	badGeo := catCollection[0].Copy()
	badGeo.Geometry = orb.LineString{{42, -42}, {42, -44}}

	addInvalids := []cattrack.CatTrack{*empty, *wrongCat, *badUUID, *badTime, *badGeo}
	catCollection = append(catCollection, addInvalids...)

	valid, invalid := c.Validate(ctx, stream.Slice(ctx, catCollection))

	wait := sync.WaitGroup{}
	wait.Add(2)
	go func() {
		defer wait.Done()
		invalidCollection := stream.Collect[cattrack.CatTrack](ctx, invalid)
		if len(invalidCollection) != len(addInvalids) {
			t.Errorf("expected %d invalid tracks, got %d", len(addInvalids), len(invalidCollection))
		}
	}()
	go func() {
		defer wait.Done()
		validCollection := stream.Collect[cattrack.CatTrack](ctx, valid)
		if len(validCollection) != catTracksLen {
			t.Errorf("expected %d valid tracks, got %d", catTracksLen, len(validCollection))
		} else {
			t.Logf("valid tracks: %d", len(validCollection))
		}
	}()
	stream.Sink(ctx, func(err error) {
		t.Errorf("unexpected error: %v", err)
	}, errs)
	wait.Wait()
}

func TestCat_dedupe(t *testing.T) {
	defer common.SlogResetLevel(slog.Level(slog.LevelWarn + 1))()

	source := testdata.Path(testdata.Source_EDGE20241217)
	gzr, err := catz.NewGZFileReader(source)
	if err != nil {
		t.Fatal(err)
	}
	defer gzr.Close()

	tc := NewTestCatWriter(t, "any", nil)
	c := tc.Cat()
	defer tc.CloseAndDestroy()

	ctx := context.Background()
	tracks, errs := stream.NDJSON[cattrack.CatTrack](ctx, gzr)
	go stream.Sink(ctx, nil, errs) // Ignore.

	raw := stream.Collect[cattrack.CatTrack](ctx, tracks)
	rawTracksN := len(raw)
	if rawTracksN == 0 {
		t.Fatal("no tracks")
	}

	// Tee into streams for
	// - raw tracks, which should not have any dupes (as-is)
	// - another stream to add some dupes to and make sure they get filtered
	aTracks, bTracks := stream.Tee(ctx, stream.Slice(ctx, raw))

	wait := sync.WaitGroup{}
	wait.Add(2)

	// Expect 0 dupes in raw/good/canonical source.
	// The deduped stream size should be equivalent to raw.
	go func() {
		defer wait.Done()
		deduped := c.dedupe(ctx, rawTracksN, aTracks)
		dedupedCollection := stream.Collect[cattrack.CatTrack](ctx, deduped)
		if len(dedupedCollection) != len(raw) {
			t.Errorf("expected %d deduped tracks, got %d", len(raw), len(dedupedCollection))
		}
	}()
	// Collect duping tracks.
	go func() {
		defer wait.Done()
		duplicated := stream.Collect[cattrack.CatTrack](ctx, bTracks)
		// Deduped stream size should be equal to half of duping stream.
		duplicated = append(duplicated, duplicated...)
		expected := len(duplicated) / 2

		deduped := c.dedupe(ctx, rawTracksN, stream.Slice(ctx, duplicated))
		dedupedCollection := stream.Collect[cattrack.CatTrack](ctx, deduped)
		if len(dedupedCollection) != expected {
			t.Errorf("expected %d deduped tracks, got %d (difference: %d)",
				expected, len(dedupedCollection), expected-len(dedupedCollection))
		}
	}()
	wait.Wait()
}
