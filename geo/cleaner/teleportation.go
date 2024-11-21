package cleaner

import (
	"context"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"time"
)

func TeleportationFilter(ctx context.Context, in <-chan *cattrack.CatTrack) <-chan *cattrack.CatTrack {
	out := make(chan *cattrack.CatTrack)

	go func() {
		defer close(out)

		var lastTime time.Time
		var lastPoint orb.Point

		for track := range in {

			// The first track is always sent.
			if lastTime.IsZero() {
				lastTime = track.MustTime()
				lastPoint = track.Point()
				out <- track
				continue
			}

			// Signal loss is not teleportation.
			trackTime := track.MustTime()
			interval := trackTime.Sub(lastTime)
			if interval > params.DefaultCleanConfig.TeleportWindow {
				lastTime = trackTime
				lastPoint = track.Point()
				out <- track
				continue
			}

			dist := geo.Distance(lastPoint, track.Point())

			// Compare the reported speed against the calculated speed.
			// If the calculated speed exceeds the reported speed by X factor, it's a teleportation point.
			calculatedSpeed := dist / interval.Seconds()
			reportedSpeed := track.Properties.MustFloat64("Speed")
			if calculatedSpeed > reportedSpeed*params.DefaultCleanConfig.TeleportSpeedFactor {
				continue
			}

			lastTime = trackTime
			lastPoint = track.Point()
			select {
			case <-ctx.Done():
				return
			case out <- track:
			}
		}
	}()
	return out
}
