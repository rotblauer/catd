package api

import (
	"context"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/geo/cleaner"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
)

func CleanTracks(ctx context.Context, catID conceptual.CatID, in <-chan *cattrack.CatTrack) <-chan *cattrack.CatTrack {
	out := make(chan *cattrack.CatTrack)

	go func() {
		defer close(out)

		accurate := stream.Filter(ctx, cleaner.FilterAccuracy, in)
		slow := stream.Filter(ctx, cleaner.FilterSpeed, accurate)
		low := stream.Filter(ctx, cleaner.FilterElevation, slow)
		uncanyoned := cleaner.WangUrbanCanyonFilter(ctx, low)
		unteleported := cleaner.TeleportationFilter(ctx, uncanyoned)

		for element := range unteleported {
			// Will block on send. Needs reader.
			out <- element
		}
	}()

	return out
}
