package api

import (
	"context"
	"github.com/rotblauer/catd/geo/cleaner"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
)

// CleanTracks probably doesn't need a cat.
// FIXME? Turn it loose (method to func).
func (c *Cat) CleanTracks(ctx context.Context, in <-chan cattrack.CatTrack) <-chan cattrack.CatTrack {
	out := make(chan cattrack.CatTrack)

	go func() {
		defer close(out)
		//wang := new(cleaner.WangUrbanCanyonFilter)
		teleportation := new(cleaner.TeleportationFilter)
		defer func() {
			c.logger.Info("CleanTracks filters done", "teleportation", teleportation.Filtered)
		}()

		accurate := stream.Filter(ctx, cleaner.FilterAccuracy, in)
		slow := stream.Filter(ctx, cleaner.FilterSpeed, accurate)
		low := stream.Filter(ctx, cleaner.FilterElevation, slow)

		//uncanyoned := wang.Filter(ctx, low)
		unteleported := teleportation.Filter(ctx, low)

		for element := range unteleported {

			// Will block on send. Needs reader.
			out <- element
		}
	}()

	return out
}
