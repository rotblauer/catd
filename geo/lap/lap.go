// Package lap is responsible for coercing a timeseries
// of CatTracks into "laps," represented as LineStrings of CatTracks (points).
// These are intended to represent times (and spaces) when the cat is moving.
//
// One of the main decisions is to decide when a cat is moving (vs. stopped),
// or even when one kind of moving is different from the other (e.g. Activity).

package lap

import (
	"context"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"time"
)

type State struct {
	Interval time.Duration
	Tracks   []*cattrack.CatTrack // the points represented by the linestring
	TimeLast time.Time
	ch       chan *cattrack.CatLap
}

func NewState(interval time.Duration) *State {
	return &State{
		Interval: interval,
		Tracks:   make([]*cattrack.CatTrack, 0),
		TimeLast: time.Time{},
		ch:       make(chan *cattrack.CatLap),
	}
}

func (s *State) Add(ct *cattrack.CatTrack) {
	if s.IsDiscontinuous(ct) {
		s.Flush()
	}
	s.Tracks = append(s.Tracks, ct)
}

func (s *State) IsDiscontinuous(ct *cattrack.CatTrack) bool {
	current := ct.MustTime()
	if s.TimeLast.IsZero() || len(s.Tracks) == 0 {
		s.TimeLast = current
		return false
	}
	span := current.Sub(s.TimeLast)
	s.TimeLast = current
	discontinuous := span > s.Interval || span < -1*time.Second
	if discontinuous {
		slog.Warn("Lap.IsDiscontinuous", "discontinuous", discontinuous, "span", span, "interval", s.Interval)
	}
	return discontinuous
}

func (s *State) Flush() {
	if len(s.Tracks) >= 2 {
		lap := cattrack.NewCatLap(s.Tracks)
		if lap != nil {
			s.ch <- lap
		}
	}
	s.TimeLast = time.Time{}
	s.Tracks = make([]*cattrack.CatTrack, 0)
}

// Stream consumes a channel of CatTracks and emits completed CatLaps.
// It will not flush any (last) incomplete lap.
func (s *State) Stream(ctx context.Context, in <-chan *cattrack.CatTrack) <-chan *cattrack.CatLap {
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
		// Do not flush remaining tracks, these are an incomplete lap.
		// We'll depend on the caller to decide what to do with them.
	}()
	return s.ch
}
