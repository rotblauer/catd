package api

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/geo/nap"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"time"
)

func (c *Cat) NapTracks(ctx context.Context, in <-chan *cattrack.CatTrack) <-chan *cattrack.CatNap {
	out := make(chan *cattrack.CatNap)

	if c.State == nil {
		_, err := c.WithState(false)
		if err != nil {
			slog.Error("Failed to create cat state", "error", err)
			return nil
		}
	}
	c.State.Waiting.Add(1)

	ns := nap.NewState(params.DefaultTripDetectorConfig.DwellInterval)

	// Attempt to restore lap-builder state.

	if data, err := c.State.ReadKV([]byte("napstate")); err == nil && data != nil {
		if err := json.Unmarshal(data, ns); err != nil {
			slog.Error("Failed to unmarshal nap state", "error", err)
		} else {
			var last time.Time
			if len(ns.Tracks) > 0 {
				last = ns.Tracks[len(ns.Tracks)-1].MustTime()
			}
			slog.Info("Restored nap state", "cat", c.CatID, "len", len(ns.Tracks), "last", last)
		}
	}

	go func() {
		defer close(out)
		defer c.State.Waiting.Done()

		// Persist lap-builder state on completion.
		defer func() {
			data, err := json.Marshal(ns)
			if err != nil {
				slog.Error("Failed to marshal nap state", "error", err)
				return
			}
			if err := c.State.WriteKV([]byte("napstate"), data); err != nil {
				slog.Error("Failed to write nap state", "error", err)
			}
		}()

		completed := ns.Stream(ctx, in)
		for complete := range completed {
			out <- complete
		}
	}()

	return out
}
