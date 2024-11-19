package nap

import (
	"context"
	"github.com/rotblauer/catd/types/cattrack"
	"time"
)

type State struct {
	Interval time.Duration
	Tracks   []*cattrack.CatTrack // the points represented by the linestring
	ch       chan *cattrack.CatNap
}

func NewState(interval time.Duration) *State {
	return &State{
		Interval: interval,
		Tracks:   make([]*cattrack.CatTrack, 0),
		ch:       make(chan *cattrack.CatNap),
	}
}

func (s *State) Add(ct *cattrack.CatTrack) {
	if s.IsDiscontinuous(ct) {
		s.Flush()
	}
	s.Tracks = append(s.Tracks, ct)
}

func (s *State) IsDiscontinuous(ct *cattrack.CatTrack) bool {
	if len(s.Tracks) == 0 {
		return false
	}
	span := ct.MustTime().Sub(s.Tracks[len(s.Tracks)-1].MustTime())
	return span > s.Interval || span < -1
}

func (s *State) Flush() {
	if len(s.Tracks) >= 1 {
		s.ch <- cattrack.NewCatNap(s.Tracks)
	}
	s.Tracks = make([]*cattrack.CatTrack, 0)
}

func (s *State) Stream(ctx context.Context, in <-chan *cattrack.CatTrack) <-chan *cattrack.CatNap {
	go func() {
		defer close(s.ch)
		for ct := range in {
			s.Add(ct)
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
