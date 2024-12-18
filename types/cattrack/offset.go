package cattrack

import (
	"context"
	"time"
)

func SetTimeOffset(old, next CatTrack) CatTrack {
	offset := MustContinuousTimeOffset(old, next)
	next.SetPropertySafe("TimeOffset", offset.Round(time.Second).Seconds())
	return next
}

func WithTimeOffset(ctx context.Context, in <-chan CatTrack) chan CatTrack {
	out := make(chan CatTrack)
	go func() {
		defer close(out)
		last := &CatTrack{}
		for track := range in {
			track = SetTimeOffset(*last, track)
			select {
			case <-ctx.Done():
				return
			case out <- track:
				*last = track
			}
		}
	}()
	return out
}
