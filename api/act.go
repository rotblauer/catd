package api

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/geo/act"
	"github.com/rotblauer/catd/state"
	"github.com/rotblauer/catd/types/cattrack"
)

func (c *Cat) ImprovedActTracks(ctx context.Context, in <-chan cattrack.CatTrack) <-chan cattrack.CatTrack {
	c.getOrInitState(false)
	out := make(chan cattrack.CatTrack)

	im := &act.Improver{}
	if err := c.restoreActImprover(im); err != nil {
		c.logger.Warn("Failed to read act improver (new cat?)", "error", err)
		im = act.NewImprover()
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
			if err := im.Improve(track); err != nil {
				c.logger.Error("Failed to improve act track", "error", err)
			}

			if im.Cat.ActivityState != act.TrackerStateActivityUndetermined {
				track.SetPropertySafe("Activity", im.Cat.ActivityState.String())
			}

			select {
			case <-ctx.Done():
				return
			case out <- track:
			}
		}

	}()

	return out
}

func (c *Cat) storeActImprover(im *act.Improver) error {
	b, err := json.Marshal(im)
	if err != nil {
		return err
	}
	return c.State.StoreKV(state.CatStateBucket, []byte("act-improver"), b)
}

func (c *Cat) restoreActImprover(im *act.Improver) error {
	b, err := c.State.ReadKV(state.CatStateBucket, []byte("act-improver"))
	if err != nil {
		return err
	}
	return json.Unmarshal(b, im)
}
