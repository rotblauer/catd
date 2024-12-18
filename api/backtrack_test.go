package api

import (
	"context"
	"fmt"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/testing/testdata"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"path/filepath"
	"testing"
)

// TestCat_Unbacktrack_NoneMissing tests that, for a single stream,
// no tracks are missing when unbacktracking.
func TestCat_Unbacktrack_NoneMissing(t *testing.T) {
	oldLevel := slog.SetLogLoggerLevel(slog.Level(slog.LevelWarn + 1))
	defer slog.SetLogLoggerLevel(oldLevel)
	cases := []struct {
		gzSource string
		cat      string
	}{
		{cat: "rye", gzSource: testdata.Path(testdata.Source_EDGE20241217)},
		{cat: "ia", gzSource: testdata.Path(testdata.Source_EDGE20241217)},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s-%s", filepath.Base(c.gzSource), c.cat), func(t *testing.T) {
			testCat_Unbacktrack_NoneMissing(t, c.cat, c.gzSource)
		})
	}
}

func testCat_Unbacktrack_NoneMissing(t *testing.T, cat, gzSource string) {
	ctx := context.Background()
	tracks, errs := testdata.ReadSourceGZ[cattrack.CatTrack](ctx, gzSource)

	tc := NewTestCatWriter(t, cat, nil)
	c := tc.Cat()
	defer tc.CloseAndDestroy()

	// Collect and count cat's tracks.
	catTracks := stream.Filter(ctx, func(track cattrack.CatTrack) bool {
		return track.CatID() == conceptual.CatID(cat)
	}, tracks)
	catCollection := stream.Collect[cattrack.CatTrack](ctx, catTracks)
	catTracksLen := len(catCollection)
	if catTracksLen == 0 {
		t.Fatal("no tracks")
	}

	// Slice and send through unbacktracker.
	unbacktracked, _ := c.Unbacktrack(ctx, stream.Slice(ctx, catCollection))

	n := 0
loop:
	for {
		select {
		case _, ok := <-unbacktracked:
			if !ok {
				break loop
			}
			n++
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if n != catTracksLen {
		t.Errorf("expected %d unbacktracked tracks, got %d", catTracksLen, n)
	} else {
		t.Logf("cat: %s unbacktracked %d tracks", cat, n)
	}
}

// TestCat_Unbacktracked_ReInits tests that, for many streams --
// simulating many calls (ie Populate batches) -- that the unbacktracker
// produces an expected number of unbacktracked tracks.
// This tests the Unbacktracker's ability to load and store persistent state correctly.
func TestCat_Unbacktracked_ReInits(t *testing.T) {
	oldLevel := slog.SetLogLoggerLevel(slog.Level(slog.LevelWarn + 1))
	defer slog.SetLogLoggerLevel(oldLevel)
	cases := []struct {
		cat      string
		gzSource string
		// There are _some_ backtracks in the source. This is how many.
		backtracks int
	}{
		// rye has 23754/23775 valid tracks for this source; 23754/23775=99.9%
		{"rye", testdata.Path(testdata.Source_EDGE20241217), 21},
		{"ia", testdata.Path(testdata.Source_EDGE20241217), 0},
	}
	batchSizeMatrix := []int{1, 100, 1000, 10_000}
	for _, c := range cases {
		for _, batchSize := range batchSizeMatrix {
			t.Run(fmt.Sprintf("%s-%s-%d", filepath.Base(c.gzSource), c.cat, batchSize),
				func(t *testing.T) {
					testCat_Unbacktracked_ReInits(t, c.cat, c.gzSource, c.backtracks, batchSize)
				})
		}
	}
}

func testCat_Unbacktracked_ReInits(t *testing.T, cat, gzSource string, backtracks, batchSize int) {
	ctx := context.Background()
	tracks, errs := testdata.ReadSourceGZ[cattrack.CatTrack](ctx, gzSource)

	tc := NewTestCatWriter(t, cat, nil)
	c := tc.Cat()
	defer tc.CloseAndDestroy()

	// Collect and count cat's tracks.
	catTracks := stream.Filter(ctx, func(track cattrack.CatTrack) bool {
		return track.CatID() == conceptual.CatID(cat)
	}, tracks)
	catCollection := stream.Collect[cattrack.CatTrack](ctx, catTracks)
	catTracksLen := len(catCollection)
	if catTracksLen == 0 {
		t.Fatal("no tracks")
	}
	expectedOKTracks := catTracksLen - backtracks

	sl := stream.Slice(ctx, catCollection)
	batched := stream.Batch(ctx, nil, func(s []cattrack.CatTrack) bool {
		return len(s) == 100
	}, sl)

	okTracks := 0
	for batch := range batched {
		simulatedPush := stream.Slice(ctx, batch)
		unbacktracked, onClose := c.Unbacktrack(ctx, simulatedPush)
		for range unbacktracked {
			okTracks++
		}
		onClose()
	}
	if err, open := <-errs; err != nil || open {
		t.Fatal(err, open)
	}
	if okTracks != expectedOKTracks {
		t.Errorf("expected %d/%d ok tracks, got %d/%d",
			expectedOKTracks, catTracksLen, okTracks, catTracksLen)
	} else {
		//t.Logf("cat: %s batch-unbacktracked %d/%d tracks", cat, okTracks, catTracksLen)
	}
}
