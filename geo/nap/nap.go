package nap

import (
	"context"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
	"github.com/paulmach/orb/planar"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"time"
)

type State struct {
	Config   *params.NapConfig
	Tracks   []*cattrack.CatTrack // the points represented by the nap
	Centroid orb.Point
	TimeLast time.Time
	ch       chan cattrack.CatNap
}

func NewState(config *params.NapConfig) *State {
	if config == nil {
		config = params.DefaultNapConfig
	}
	return &State{
		Config: config,
		Tracks: make([]*cattrack.CatTrack, 0),
		ch:     make(chan cattrack.CatNap),
	}
}

func (s *State) Add(ct *cattrack.CatTrack) {
	defer func() {
		s.TimeLast = ct.MustTime()
		s.Tracks = append(s.Tracks, ct)
		mp := orb.MultiPoint{}
		for _, t := range s.Tracks {
			mp = append(mp, t.Geometry.(orb.Point))
		}
		s.Centroid, _ = planar.CentroidArea(mp)
	}()
	if s.IsDiscontinuous(ct) {
		s.Flush()
	}
}

func (s *State) IsDiscontinuous(ct *cattrack.CatTrack) bool {
	currentTime := ct.MustTime()
	if s.TimeLast.IsZero() || len(s.Tracks) == 0 {
		return false
	}

	// Time
	span := currentTime.Sub(s.TimeLast)
	s.TimeLast = currentTime

	discontinuous := span < -1*time.Second || span > s.Config.DwellInterval
	if discontinuous {
		return discontinuous
	}

	// Space
	dist := geo.Distance(s.Centroid, ct.Point())

	return dist > s.Config.DwellDistance
}

func (s *State) Flush() {
	if len(s.Tracks) >= 2 {
		nap := cattrack.NewCatNap(s.Tracks)
		if nap != nil {
			s.ch <- *nap
		}
	}
	s.Tracks = make([]*cattrack.CatTrack, 0)
	s.Centroid = orb.Point{}
	s.TimeLast = time.Time{}
}

func (s *State) Stream(ctx context.Context, in <-chan cattrack.CatTrack) <-chan cattrack.CatNap {
	go func() {
		defer close(s.ch)
		for ct := range in {
			s.Add(&ct)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		// Do not flush remaining tracks, these are an incomplete nap.
		// We'll depend on the caller to decide what to do with them.
	}()
	return s.ch
}

/*
fatal error: concurrent map read and map write

goroutine 37800 [running]:
github.com/rotblauer/catd/types/cattrack.(*CatTrack).Time(0xc001cff720)
        /home/ia/dev/rotblauer/catd/types/cattrack/cattrack.go:36 +0x7f
github.com/rotblauer/catd/types/cattrack.(*CatTrack).MustTime(...)
        /home/ia/dev/rotblauer/catd/types/cattrack/cattrack.go:48
github.com/rotblauer/catd/geo/nap.(*State).IsDiscontinuous(0xc004c077a0, 0x982620?)
        /home/ia/dev/rotblauer/catd/geo/nap/nap.go:34 +0x2e
github.com/rotblauer/catd/geo/nap.(*State).Add(0xc004c077a0, 0xc001cff720)
        /home/ia/dev/rotblauer/catd/geo/nap/nap.go:24 +0x25
github.com/rotblauer/catd/geo/nap.(*State).Stream.func1()
        /home/ia/dev/rotblauer/catd/geo/nap/nap.go:52 +0xa5
created by github.com/rotblauer/catd/geo/nap.(*State).Stream in goroutine 37832
        /home/ia/dev/rotblauer/catd/geo/nap/nap.go:49 +0x8f

*/

/*
fatal error: concurrent map read and map write

goroutine 2453 [running]:
github.com/rotblauer/catd/types/cattrack.(*CatTrack).Time(0x40c1e8?)
        /home/ia/dev/rotblauer/catd/types/cattrack/cattrack.go:33 +0x2e
github.com/rotblauer/catd/types/cattrack.(*CatTrack).MustTime(...)
        /home/ia/dev/rotblauer/catd/types/cattrack/cattrack.go:52
github.com/rotblauer/catd/geo/nap.(*State).IsDiscontinuous(0xc003c8ad00, 0x982640?)
        /home/ia/dev/rotblauer/catd/geo/nap/nap.go:32 +0x25
github.com/rotblauer/catd/geo/nap.(*State).Add(0xc003c8ad00, 0xc004988e60)
        /home/ia/dev/rotblauer/catd/geo/nap/nap.go:25 +0x25
github.com/rotblauer/catd/geo/nap.(*State).Stream.func1()
        /home/ia/dev/rotblauer/catd/geo/nap/nap.go:57 +0xa5
created by github.com/rotblauer/catd/geo/nap.(*State).Stream in goroutine 2535
        /home/ia/dev/rotblauer/catd/geo/nap/nap.go:54 +0x8f

*/
