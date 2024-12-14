package api

import (
	"context"
	"github.com/rotblauer/catd/types/cattrack"
	"time"
)

func TracksWithOffset(ctx context.Context, in <-chan cattrack.CatTrack) chan cattrack.CatTrack {
	out := make(chan cattrack.CatTrack)
	go func() {
		defer close(out)
		last := cattrack.CatTrack{}
		for track := range in {
			offset := cattrack.MustContinuousTimeOffset(last, track)
			t := &track
			t.SetPropertySafe("TimeOffset", offset.Round(time.Second).Seconds())
			select {
			case <-ctx.Done():
				return
			case out <- *t:
				last = *t
			}
		}
	}()
	return out
}
