package cleaner

import (
	"context"
	"github.com/paulmach/orb/geo"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
)

func TeleportationFilter(ctx context.Context, in <-chan *cattrack.CatTrack) <-chan *cattrack.CatTrack {
	out := make(chan *cattrack.CatTrack)

	go func() {
		defer close(out)

		var last *cattrack.CatTrack

		for track := range in {

			// The first track is always sent.
			if last == nil {
				out <- track
				last = track
				continue
			}

			// Signal loss is not teleportation.
			interval := track.MustTime().Sub(last.MustTime())
			if interval > params.DefaultCleanConfig.TeleportWindow {
				out <- track
				last = track
				continue
			}

			dist := geo.Distance(last.Point(), track.Point())

			// Compare the reported speed against the calculated speed.
			// If the calculated speed exceeds the reported speed by X factor, it's a teleportation point.
			calculatedSpeed := dist / interval.Seconds()
			reportedSpeed := track.Properties.MustFloat64("Speed")
			if calculatedSpeed > reportedSpeed*params.DefaultCleanConfig.TeleportSpeedFactor {
				continue
			}

			select {
			case <-ctx.Done():
				return
			case out <- track:
				last = track
			}
		}
	}()
	return out
}
