package api

import (
	"context"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/geo/nap"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
)

func (c *Cat) MustGetNapState() *nap.State {
	c.getOrInitState(false)
	ns := nap.NewState(nil)
	err := c.State.ReadKVUnmarshalJSON(params.CatStateBucket, params.CatStateKey_Naps, ns)
	if err == nil {
		// FIXME (?) Not sure why do this.
		if len(ns.Tracks) > 0 {
			ns.TimeLast = ns.Tracks[len(ns.Tracks)-1].MustTime()
			ns.Centroid = ns.Tracks[len(ns.Tracks)-1].Geometry.(orb.Point)
		}
		c.logger.Info("Restored nap state", "len", len(ns.Tracks), "last", ns.TimeLast)
		return ns
	}
	c.logger.Warn("Failed to read nap state (new cat?)", "error", err)
	return ns
}

func (c *Cat) StoreNapState(ns *nap.State) error {
	return c.State.StoreKVMarshalJSON(params.CatStateBucket, params.CatStateKey_Naps, ns)
}

func (c *Cat) TrackNaps(ctx context.Context, in <-chan cattrack.CatTrack) <-chan cattrack.CatNap {
	c.getOrInitState(false)
	out := make(chan cattrack.CatNap)
	ns := c.MustGetNapState()

	c.State.Waiting.Add(1)
	go func() {
		defer close(out)
		defer c.State.Waiting.Done()

		// Persist lap-builder state on completion.
		defer func() {
			if err := c.StoreNapState(ns); err != nil {
				c.logger.Error("Failed to store nap state", "error", err)
			} else {
				c.logger.Debug("Stored nap state")
			}
		}()

		completed := ns.Stream(ctx, in)
		for complete := range completed {
			out <- complete
		}
	}()

	return out
}
