package api

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/geo/tripdetector"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/state"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"time"
)

// TripDetectTracks detects trips in incoming CatTracks.
func (c *Cat) TripDetectTracks(ctx context.Context, in <-chan cattrack.CatTrack) <-chan cattrack.CatTrack {
	c.getOrInitState()

	out := make(chan cattrack.CatTrack)
	td := tripdetector.NewTripDetector(params.DefaultTripDetectorConfig)

	// If possible, read persisted cat tripdetector-state.
	if err := c.restoreTripDetector(td); err != nil {
		c.logger.Warn("Failed to read trip detector (new cat?)", "error", err)
	} else {
		last := td.LastPointN(0)
		if last != nil {
			tdLatest := last.MustTime()
			c.logger.Info("Restored trip-detector state",
				"last", tdLatest, "lap", td.Tripping)
		} else {
			c.logger.Info("Restored empty trip-detector state", "lap", td.Tripping)
		}
	}

	c.State.Waiting.Add(1)
	go func() {
		defer c.State.Waiting.Done()

		// Persist the trip detector state on stream completion.
		defer func() {
			if err := c.storeTripDetector(td); err != nil {
				c.logger.Error("Failed to store trip detector", "error", err)
			} else {
				var tdLatest time.Time
				last := td.LastPointN(0)
				if last != nil {
					tdLatest = last.MustTime()
				}
				c.logger.Debug("Stored trip detector state", "last", tdLatest, "lap", td.Tripping)
			}
			defer close(out)
		}()

		res := stream.Filter(ctx,
			// Filter out the resets and inits.
			// Resets happen when the trip detector is reset after a signal loss.
			// Inits happen when the trip detector is initialized.
			func(ct cattrack.CatTrack) bool {
				if ct.IsEmpty() {
					return false
				}
				reason := ct.Properties.MustString("MotionStateReason", "init")
				return reason != "init" && reason != "reset"
			},
			stream.Transform(ctx, func(ct cattrack.CatTrack) cattrack.CatTrack {
				c.logger.Debug("Detecting trips", "track", ct.StringPretty())

				cp := ct.Copy()

				if err := td.Add(cp); err != nil {
					c.logger.Error("Failed to add track to trip detector", "error", err)
					return cattrack.CatTrack{}
				}
				cp.SetPropertySafe("IsTrip", td.Tripping)
				cp.SetPropertySafe("MotionStateReason", td.MotionStateReason)

				return *cp
			}, in))

		// Will block on send unless interrupted. Needs reader.
		for element := range res {
			element := element
			select {
			case <-ctx.Done():
				return
			case out <- element:
			}
		}
	}()

	return out
}

func (c *Cat) storeTripDetector(td *tripdetector.TripDetector) error {
	b, err := json.Marshal(td)
	if err != nil {
		return err
	}
	if err := c.State.StoreKV(state.CatStateBucket, []byte("tripdetector"), b); err != nil {
		return err
	}
	return nil
}

func (c *Cat) restoreTripDetector(td *tripdetector.TripDetector) error {
	read, err := c.State.ReadKV(state.CatStateBucket, []byte("tripdetector"))
	if err != nil {
		return err
	}
	tmp := &tripdetector.TripDetector{
		LastNPoints:    make([]*cattrack.CatTrack, 0),
		IntervalPoints: make([]*cattrack.CatTrack, 0),
	}
	if err := json.Unmarshal(read, tmp); err != nil {
		c.logger.Error("Failed to unmarshal trip detector", "error", err)
		c.logger.Error(string(read))
		return err
	}
	*td = *tmp
	return nil
}
