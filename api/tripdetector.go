package api

import (
	"context"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/geo/tripdetector"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
)

// TripDetectTracks detects trips in incoming CatTracks.
func TripDetectTracks(ctx context.Context, catID conceptual.CatID, in <-chan *cattrack.CatTrack) <-chan *cattrack.CatTrack {
	td := tripdetector.NewTripDetector(params.DefaultTripDetectorConfig)
	return stream.Filter(ctx,
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
}
