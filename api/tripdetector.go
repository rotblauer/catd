package api

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/geo/tripdetector"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/state"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"time"
)

func storeTripDetector(catID conceptual.CatID, td *tripdetector.TripDetector) error {
	appCat := state.Cat{CatID: catID}
	wr, err := appCat.NewCatWriter()
	if err != nil {
		return err
	}
	b, err := json.Marshal(td)
	if err != nil {
		return err
	}
	if err := wr.WriteKV([]byte("tripdetector"), b); err != nil {
		return err
	}
	return wr.Close()
}

func readTripDetector(catID conceptual.CatID, td *tripdetector.TripDetector) error {
	appCat := state.Cat{CatID: catID}
	cr, err := appCat.NewCatReader()
	if err != nil {
		return err
	}
	read, err := cr.ReadKV([]byte("tripdetector"))
	if err != nil {
		return err
	}
	tmp := &tripdetector.TripDetector{}
	if err := json.Unmarshal(read, tmp); err != nil {
		return err
	}
	*td = *tmp
	return nil
}

// TripDetectTracks detects trips in incoming CatTracks.
func TripDetectTracks(ctx context.Context, catID conceptual.CatID, in <-chan *cattrack.CatTrack) <-chan *cattrack.CatTrack {
	out := make(chan *cattrack.CatTrack)

	td := tripdetector.NewTripDetector(params.DefaultTripDetectorConfig)

	// If possible, read persisted cat tripdetector-state.
	if err := readTripDetector(catID, td); err != nil {
		slog.Warn("Failed to read trip detector (new cat?)", "error", err)
	} else {
		var tdLatest time.Time
		last := td.LastPointN(0)
		if last != nil {
			tdLatest = last.MustTime()
		}
		slog.Info("Restored trip-detector state",
			"cat", catID, "last", tdLatest, "lap", td.Tripping)
	}

	go func() {
		defer close(out)

		// Persist the trip detector state on stream completion.
		defer func() {
			if err := storeTripDetector(catID, td); err != nil {
				slog.Error("Failed to store trip detector", "error", err)
			} else {
				var tdLatest time.Time
				last := td.LastPointN(0)
				if last != nil {
					tdLatest = last.MustTime()
				}
				slog.Debug("Stored trip detector state", "cat", catID, "last", tdLatest, "lap", td.Tripping)
			}
		}()

		res := stream.Filter(ctx,
			// Filter out the resets and inits.
			// Resets happen when the trip detector is reset after a signal loss.
			// Inits happen when the trip detector is initialized.
			func(ct *cattrack.CatTrack) bool {
				reason := ct.Properties.MustString("MotionStateReason")
				return reason != "init" && reason != "reset"
			},
			stream.Transform(ctx, func(ct *cattrack.CatTrack) *cattrack.CatTrack {
				slog.Debug("Detecting trips", "track", ct.StringPretty())
				if err := td.Add(ct); err != nil {
					slog.Error("Failed to add track to trip detector", "error", err)
					return nil
				}

				ct.Properties["IsTrip"] = td.Tripping
				ct.Properties["MotionStateReason"] = td.MotionStateReason

				// FIXME/FIXED: These property writes are causing a fatal concurrent map read and map write.
				// Can we use ID instead? Or some other hack?
				// Why hasn't this issue happened before? (e.g. Sanitized tracks)
				// ...
				// The issue was in teleportation.go; it held a variable *cattrack.CatTrack in memory
				// representing a last-seen track, cached outside scope of a go routine.
				// This got its Properties hit real fast by the stream and boom, fatal concurrent r/w.
				// This was fixed by using attribute variables for the only values I needed - time and coords.
				// No pointers, no properties map, no problems.

				return ct
			}, in))

		// Will block on send unless interrupted. Needs reader.
		for element := range res {
			select {
			case <-ctx.Done():
				return
			case out <- element:
			}
		}
	}()

	return out
}
