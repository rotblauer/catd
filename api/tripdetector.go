package api

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/geo/tripdetector"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"time"
)

func (c *Cat) storeTripDetector(td *tripdetector.TripDetector) error {
	b, err := json.Marshal(td)
	if err != nil {
		return err
	}
	if err := c.State.WriteKV([]byte("tripdetector"), b); err != nil {
		return err
	}
	return nil
}

func (c *Cat) readTripDetector(td *tripdetector.TripDetector) error {
	read, err := c.State.ReadKV([]byte("tripdetector"))
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
func (c *Cat) TripDetectTracks(ctx context.Context, in <-chan *cattrack.CatTrack) <-chan *cattrack.CatTrack {
	out := make(chan *cattrack.CatTrack)

	if c.State == nil {
		_, err := c.WithState(false)
		if err != nil {
			slog.Error("Failed to create cat state", "error", err)
			return nil
		}
	}
	c.State.Waiting.Add(1)

	td := tripdetector.NewTripDetector(params.DefaultTripDetectorConfig)

	// If possible, read persisted cat tripdetector-state.
	if err := c.readTripDetector(td); err != nil {
		slog.Warn("Failed to read trip detector (new cat?)", "error", err)
	} else {
		var tdLatest time.Time
		last := td.LastPointN(0)
		if last != nil {
			tdLatest = last.MustTime()
		}
		slog.Info("Restored trip-detector state",
			"cat", c.CatID, "last", tdLatest, "lap", td.Tripping)
	}

	go func() {

		// Persist the trip detector state on stream completion.
		defer func() {
			if err := c.storeTripDetector(td); err != nil {
				slog.Error("Failed to store trip detector", "error", err)
			} else {
				var tdLatest time.Time
				last := td.LastPointN(0)
				if last != nil {
					tdLatest = last.MustTime()
				}
				slog.Debug("Stored trip detector state", "cat", c.CatID, "last", tdLatest, "lap", td.Tripping)
			}
			c.State.Waiting.Done()
			defer close(out)
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
