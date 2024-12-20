package api

import (
	"context"
	"github.com/rotblauer/catd/geo/lap"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
)

func (c *Cat) MustGetLapState() *lap.State {
	c.getOrInitState(true)

	ls := lap.NewState(params.DefaultLapConfig)

	// Attempt to restore lap-builder state.
	err := c.State.ReadKVUnmarshalJSON(params.CatStateBucket, params.CatStateKey_Laps, ls)
	if err == nil {
		c.logger.Info("Restored lap-builder state", "tracks", len(ls.Tracks), "last", ls.TimeLast)
		return ls
	}
	c.logger.Warn("Did not read lap state (new cat?)", "error", err)
	return ls
}

func (c *Cat) StoreLapState(ls *lap.State) error {
	return c.State.StoreKVMarshalJSON(params.CatStateBucket, params.CatStateKey_Laps, ls)
}

func (c *Cat) TrackLaps(ctx context.Context, in <-chan cattrack.CatTrack) (*lap.State, <-chan cattrack.CatLap) {
	c.getOrInitState(false)

	out := make(chan cattrack.CatLap)
	ls := c.MustGetLapState()

	c.State.Waiting.Add(1)
	go func() {
		defer close(out)
		defer c.State.Waiting.Done()

		// Persist lap-builder state on completion.
		defer func() {
			if err := c.StoreLapState(ls); err != nil {
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
