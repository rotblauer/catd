package api

import (
	"context"
	"github.com/rotblauer/catd/geo/act"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
)

func (c *Cat) storeActImprover(im *act.ProbableCat) error {
	return c.State.StoreKVMarshalJSON(params.CatStateBucket, params.CatStateKey_ActImprover, im)
}

func (c *Cat) restoreActImprover(im *act.ProbableCat) error {
	return c.State.ReadKVUnmarshalJSON(params.CatStateBucket, params.CatStateKey_ActImprover, im)
}

func (c *Cat) ImprovedActTracks(ctx context.Context, in <-chan cattrack.CatTrack) <-chan cattrack.CatTrack {
	c.getOrInitState(false)
	out := make(chan cattrack.CatTrack)

	im := act.NewProbableCat(params.DefaultActImproverConfig)
	if err := c.restoreActImprover(im); err != nil {
		c.logger.Warn("Did not read act improver (new cat?)", "error", err)
		im = act.NewProbableCat(params.DefaultActImproverConfig)
	} else {
		c.logger.Info("Restored act-improver state")
	}

	c.State.Waiting.Add(1)
	go func() {
		defer c.State.Waiting.Done()
		defer func() {
			if err := c.storeActImprover(im); err != nil {
				c.logger.Error("Failed to store act improver", "error", err)
			} else {
				c.logger.Debug("Stored act improver state")
			}
		}()
		defer close(out)

		for track := range in {
			if err := im.Add(track); err != nil {
				c.logger.Error("Failed to improve act track", "error", err)
				select {
				case <-ctx.Done():
					return
				case out <- track:
				}
				continue
			}

			if im.Pos.Activity != act.TrackerStateActivityUndetermined {
				track.SetPropertySafe("Activity", im.Pos.Activity.String())
			}

			track.SetPropertySafe("Acceleration", im.Pos.IReportedAccel)

			select {
			case <-ctx.Done():
				return
			case out <- track:
			}
		}

	}()

	return out
}
