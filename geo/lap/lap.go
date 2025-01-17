// Package lap is responsible for coercing a timeseries
// of CatTracks into "laps," represented as LineStrings of CatTracks (points).
// These are intended to represent times (and spaces) when the cat is moving.
//
// One of the main decisions is to decide when a cat is moving (vs. stopped),
// or even when one kind of moving is different from the other (e.g. Activity).

package lap

import (
	"context"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/activity"
	"github.com/rotblauer/catd/types/cattrack"
	"time"
)

type State struct {
	Config      *params.ActDiscretionConfig
	Tracks      []*cattrack.CatTrack // the points represented by the linestring
	TimeLast    time.Time
	ModeTracker *activity.ModeTracker
	LastMode    activity.Activity
	ch          chan cattrack.CatLap
	bump        chan struct{}
}

func NewState(config *params.ActDiscretionConfig) *State {
	if config == nil {
		config = params.DefaultLapConfig
	}
	return &State{
		Config:      config,
		Tracks:      make([]*cattrack.CatTrack, 0),
		TimeLast:    time.Time{},
		ModeTracker: activity.NewModeTracker(config.Interval),
		LastMode:    activity.TrackerStateUnknown,
		ch:          make(chan cattrack.CatLap),
		bump:        make(chan struct{}, 1),
	}
}

// Add appends a CatTrack to the lap builder, and flushes the current lap if necessary (discontinuous).
func (s *State) Add(ct *cattrack.CatTrack) {
	defer func(t *cattrack.CatTrack) {
		s.ModeTracker.Push(ct.MustActivity(), ct.MustTime(), ct.Properties.MustFloat64("TimeOffset", 1))
		s.TimeLast = t.MustTime()
		s.Tracks = append(s.Tracks, t)
	}(ct)
	if s.IsDiscontinuous(ct) {
		s.Flush()
	}
}

// IsDiscontinuous returns true if the CatTrack is not contiguous with the last.
// Continuity is determined by the time of the CatTrack versus the permissible interval.
// Time is the only thing that matters here.
func (s *State) IsDiscontinuous(ct *cattrack.CatTrack) bool {
	current := ct.MustTime()
	if s.TimeLast.IsZero() || len(s.Tracks) == 0 {
		return false
	}
	span := current.Sub(s.TimeLast).Seconds()
	if span > s.Config.Interval.Seconds() || span <= -1 {
		return true
	}
	// Activity splitting is a hot topic.
	//
	if !s.Config.SplitActivities {
		return false
	}

	a := s.Tracks[len(s.Tracks)-1].MustActivity()
	//b := ct.MustActivity()
	//a := s.ModeTracker.Sorted(true).RelWeights()[0].Activity
	b := ct.MustActivity()

	if a.IsUnknown() || b.IsUnknown() {
		return false
	}
	if a.IsActive() != b.IsActive() {
		return true
	}
	// FIXME: This will screw up triathlons;
	// it blends common run+bike, walk+bike, and cycle+drive combos;
	// but it's a workaround for common schizo-activities.
	if a.IsActiveHuman() && b.IsActiveHuman() {
		return false
	}
	didStop := s.ModeTracker.Has(func(a activity.ActRecord) bool {
		// 10 seconds is how long it takes to get into a car and drive it.
		return a.A == activity.TrackerStateStationary && a.W > 5 /* seconds; how quick can cat get into/out of car */
	})
	if !didStop && a.IsActiveHuman() && b == activity.TrackerStateAutomotive {
		return false
	}
	return a.DeltaAbs(b) > 1
}

// Bump signals the lap builder to flush the current lap.
func (s *State) Bump() {
	s.bump <- struct{}{}
}

// Flush emits a CatLap if there are enough points to do so, and resets the lap builder.
func (s *State) Flush() {
	if len(s.Tracks) >= 2 {
		lap := cattrack.NewCatLap(s.Tracks)
		if lap != nil {
			s.ch <- *lap
		}
	}
	s.TimeLast = time.Time{}
	s.Tracks = make([]*cattrack.CatTrack, 0)
}

// Stream consumes a channel of CatTracks and emits completed CatLaps.
// It will not flush any (last) incomplete lap.
func (s *State) Stream(ctx context.Context, in <-chan cattrack.CatTrack) <-chan cattrack.CatLap {
	go func() {
		defer close(s.ch)
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.bump:
				s.Flush()
			case ct, open := <-in:
				if !open {
					return
				}
				s.Add(&ct)
			}
		}

		// Do not flush remaining tracks, these are an incomplete lap.
		// We'll depend on the caller to decide what to do with them.
	}()
	return s.ch
}
