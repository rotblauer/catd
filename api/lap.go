package api

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/geo/lap"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
)

func (c *Cat) GetLapState() (*lap.State, error) {
	c.getOrInitState(true)

	ls := lap.NewState(params.DefaultLapConfig)

	// Attempt to restore lap-builder state.
	data, err := c.State.ReadKV(params.CatStateBucket, []byte("lapstate"))
	if err == nil && data != nil {
		if err := json.Unmarshal(data, ls); err != nil {
			return nil, err
		}
		return ls, nil
	}
	return nil, err
}

func (c *Cat) TrackLaps(ctx context.Context, in <-chan cattrack.CatTrack) (*lap.State, <-chan cattrack.CatLap) {
	c.getOrInitState(false)

	out := make(chan cattrack.CatLap)

	ls, err := c.GetLapState()
	if err != nil {
		c.logger.Warn("Failed to read lap state (new cat?)", "error", err)
		ls = lap.NewState(params.DefaultLapConfig)
	} else {
		c.logger.Info("Restored lap-builder state", "tracks", len(ls.Tracks), "last", ls.TimeLast)
	}

	c.State.Waiting.Add(1)
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
			if err := c.State.StoreKV(params.CatStateBucket, []byte("lapstate"), data); err != nil {
				c.logger.Error("Failed to write lap state", "error", err)
			}
		}()

		completed := ls.Stream(ctx, in)
		for complete := range completed {
			out <- complete
		}
	}()

	return ls, out
}
