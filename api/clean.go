package api

import (
	"context"
	"github.com/rotblauer/catd/geo/cleaner"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
)

func (c *Cat) CleanTracks(ctx context.Context, in <-chan *cattrack.CatTrack) <-chan *cattrack.CatTrack {
	out := make(chan *cattrack.CatTrack)

	go func() {
		defer close(out)
		wang := new(cleaner.WangUrbanCanyonFilter)
		teleportation := new(cleaner.TeleportationFilter)
		defer func() {
			c.logger.Info("CleanTracks filters done", "wang", wang.Filtered, "teleportation", teleportation.Filtered)
		}()

		accurate := stream.Filter(ctx, cleaner.FilterAccuracy, in)
		slow := stream.Filter(ctx, cleaner.FilterSpeed, accurate)
		low := stream.Filter(ctx, cleaner.FilterElevation, slow)
		uncanyoned := wang.Filter(ctx, low)
		unteleported := teleportation.Filter(ctx, uncanyoned)

		for element := range unteleported {
			element := element
			// Will block on send. Needs reader.
			out <- element
		}
	}()

	return out
}
