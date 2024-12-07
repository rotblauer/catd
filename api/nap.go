package api

import (
	"context"
	"encoding/json"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/geo/nap"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
)

func (c *Cat) TrackNaps(ctx context.Context, in <-chan cattrack.CatTrack) <-chan cattrack.CatNap {
	c.getOrInitState(false)

	out := make(chan cattrack.CatNap)
	ns := nap.NewState(nil)

	// Attempt to restore lap-builder state.
	if data, err := c.State.ReadKV(params.CatStateBucket, []byte("napstate")); err == nil && data != nil {
		if err := json.Unmarshal(data, ns); err != nil {
			c.logger.Error("Failed to unmarshal nap state", "error", err)
		} else {
			if len(ns.Tracks) > 0 {
				ns.TimeLast = ns.Tracks[len(ns.Tracks)-1].MustTime()
				ns.Centroid = ns.Tracks[len(ns.Tracks)-1].Geometry.(orb.Point)
			}
			c.logger.Info("Restored nap state", "len", len(ns.Tracks), "last", ns.TimeLast)
		}
	}

	c.State.Waiting.Add(1)
	go func() {
		defer close(out)
		defer c.State.Waiting.Done()

		// Persist lap-builder state on completion.
		defer func() {
			data, err := json.Marshal(ns)
			if err != nil {
				c.logger.Error("Failed to marshal nap state", "error", err)
				return
			}
			if err := c.State.StoreKV(params.CatStateBucket, []byte("napstate"), data); err != nil {
				c.logger.Error("Failed to write nap state", "error", err)
			}
		}()

		completed := ns.Stream(ctx, in)
		for complete := range completed {
			out <- complete
		}
	}()

	return out
}
