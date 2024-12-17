package api

import (
	"context"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/testing/testdata"
	"github.com/rotblauer/catd/types/cattrack"
	"testing"
)

func testCat_Unbacktrack(t *testing.T, cat string) {
	testdataPathGZ := testdata.Path(testdata.Source_EDGE20241217)
	gzr, err := catz.NewGZFileReader(testdataPathGZ)
	if err != nil {
		t.Fatal(err)
	}
	defer gzr.Close()

	tc := NewTestCatWriter(t, cat, nil)
	c := tc.Cat()
	defer tc.CloseAndDestroy()

	ctx := context.Background()
	tracks, errs := stream.NDJSON[cattrack.CatTrack](ctx, gzr)

	// Collect and count rye's tracks.
	catTracks := stream.Filter(ctx, func(track cattrack.CatTrack) bool {
		return track.CatID() == conceptual.CatID(cat)
	}, tracks)
	catCollection := stream.Collect[cattrack.CatTrack](ctx, catTracks)
	catTracksLen := len(catCollection)
	if catTracksLen == 0 {
		t.Fatal("no tracks")
	}

	// Resend through unbacktracker.
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

func TestCat_Unbacktrack(t *testing.T) {
	t.Run("rye", func(t *testing.T) {
		testCat_Unbacktrack(t, "rye")
	})
	t.Run("ia", func(t *testing.T) {
		testCat_Unbacktrack(t, "ia")
	})
}
