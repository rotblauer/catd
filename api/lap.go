package api

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/geo/lap"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
)

func (c *Cat) LapTracks(ctx context.Context, in <-chan *cattrack.CatTrack) <-chan *cattrack.CatLap {
	out := make(chan *cattrack.CatLap)

	if c.State == nil {
		_, err := c.WithState(false)
		if err != nil {
			c.logger.Error("Failed to create cat state", "error", err)
			return nil
		}
	}
	c.State.Waiting.Add(1)

	ls := lap.NewState(params.DefaultTripDetectorConfig.DwellInterval)

	// Attempt to restore lap-builder state.
	if data, err := c.State.ReadKV([]byte("lapstate")); err == nil && data != nil {
		if err := json.Unmarshal(data, ls); err != nil {
			c.logger.Error("Failed to unmarshal lap state", "error", err)
		} else {
			if len(ls.Tracks) > 0 {
				last := ls.Tracks[len(ls.Tracks)-1].MustTime()
				c.logger.Info("Restored lap state", "len", len(ls.Tracks), "last", last)
			} else {
				c.logger.Info("Restored lap state", "len", len(ls.Tracks))
			}
		}
	}

	go func() {
		defer close(out)
		defer c.State.Waiting.Done()

		// Persist lap-builder state on completion.
		defer func() {
			data, err := json.Marshal(ls)
			if err != nil {
				c.logger.Error("Failed to marshal lap state", "error", err)
				return
			}
			if err := c.State.WriteKV([]byte("lapstate"), data); err != nil {
				c.logger.Error("Failed to write lap state", "error", err)
			}
		}()

		completed := ls.Stream(ctx, in)
		for complete := range completed {
			out <- complete
		}
	}()

	return out
}
