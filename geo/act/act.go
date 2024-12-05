package act

import (
	"github.com/paulmach/orb/geo"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/types/activity"
	"github.com/rotblauer/catd/types/cattrack"
	"math"
	"slices"
	"time"
)

const TrackerStateActivityUndetermined = activity.TrackerStateUnknown - 1

type ActivityMode struct {
	Activity  activity.Activity
	Magnitude float64
}

type WrappedTrack struct {
	cattrack.CatTrack
	TimeOffset time.Duration

	SpeedCalculated float64

	AccelerationReported   float64
	AccelerationCalculated float64

	HeadingCalculated      float64
	HeadingDeltaReported   float64
	HeadingDeltaCalculated float64
}

func (w WrappedTrack) Speed() float64 {
	return w.Properties.MustFloat64("Speed", -1)
}

func (w WrappedTrack) Heading() float64 {
	return w.Properties.MustFloat64("Heading", -1)
}

type Cat struct {
	// Instantaneous raw gauges
	Start time.Time

	Last           WrappedTrack
	IntervalPoints []WrappedTrack

	WindowSpan time.Duration

	WindowAccelerationReportedSum   float64
	WindowAccelerationCalculatedSum float64

	WindowSpeedReportedSum   float64
	WindowSpeedCalculatedSum float64

	ActivityState          activity.Activity
	ActivityStateStart     time.Time
	ActivityStateLastCheck time.Time

	ActivityAlternateState      activity.Activity
	ActivityAlternateStateStart time.Time

	Unknown    ActivityMode
	Stationary ActivityMode
	Walking    ActivityMode
	Running    ActivityMode
	Cycling    ActivityMode
	Driving    ActivityMode
	Flying     ActivityMode
}

func NewCat() *Cat {
	return &Cat{
		Start:          time.Time{},
		IntervalPoints: make([]WrappedTrack, 0),

		ActivityState:      TrackerStateActivityUndetermined,
		ActivityStateStart: time.Time{},

		ActivityAlternateState:      TrackerStateActivityUndetermined,
		ActivityAlternateStateStart: time.Time{},

		Stationary: ActivityMode{Activity: activity.TrackerStateStationary},
		Walking:    ActivityMode{Activity: activity.TrackerStateWalking},
		Running:    ActivityMode{Activity: activity.TrackerStateRunning},
		Cycling:    ActivityMode{Activity: activity.TrackerStateCycling},
		Driving:    ActivityMode{Activity: activity.TrackerStateDriving},
		Flying:     ActivityMode{Activity: activity.TrackerStateFlying},
	}
}

func (c *Cat) Reset() {
	cc := NewCat()
	*c = *cc
}

func (c *Cat) IsUninitialized() bool {
	return c.Start.IsZero()
}

func (c *Cat) IsWindowEmpty() bool {
	return len(c.IntervalPoints) == 0
}

func (c *Cat) SortedActsKnown() []ActivityMode {
	modes := []ActivityMode{
		c.Stationary, c.Walking, c.Running, c.Cycling, c.Driving, c.Flying,
	}
	slices.SortFunc(modes, func(a, b ActivityMode) int {
		if a.Magnitude > b.Magnitude {
			return -1
		} else if a.Magnitude < b.Magnitude {
			return 1
		} else {
			return 0
		}
	})
	return modes
}

func (c *Cat) SortedActsAll() []ActivityMode {
	modes := []ActivityMode{
		c.Unknown, c.Stationary, c.Walking, c.Running, c.Cycling, c.Driving, c.Flying,
	}
	slices.SortFunc(modes, func(a, b ActivityMode) int {
		if a.Magnitude > b.Magnitude {
			return -1
		} else if a.Magnitude < b.Magnitude {
			return 1
		} else {
			return 0
		}
	})
	return modes
}

func IsActivityActive(act activity.Activity) bool {
	if act <= activity.TrackerStateStationary {
		return false
	}
	return true
}

func (c *Cat) push(ct WrappedTrack) {
	c.WindowSpan += ct.TimeOffset
	c.pushActivityMode(ct)
	c.WindowAccelerationReportedSum += ct.AccelerationReported
	c.WindowAccelerationCalculatedSum += ct.AccelerationCalculated
	c.WindowSpeedReportedSum += ct.Speed() * ct.TimeOffset.Seconds()
	c.WindowSpeedCalculatedSum += ct.SpeedCalculated * ct.TimeOffset.Seconds()
}

func (c *Cat) drop(ct WrappedTrack) {
	c.WindowAccelerationReportedSum -= ct.AccelerationReported
	c.WindowAccelerationCalculatedSum -= ct.AccelerationCalculated
	c.WindowSpeedReportedSum -= ct.Speed() * ct.TimeOffset.Seconds()
	c.WindowSpeedCalculatedSum -= ct.SpeedCalculated * ct.TimeOffset.Seconds()
	c.dropActivityMode(ct)
	c.WindowSpan -= ct.TimeOffset
}

func (c *Cat) pushActivityMode(ct WrappedTrack) {
	weight := ct.TimeOffset.Seconds()
	switch activity.FromAny(ct.Properties["Activity"]) {
	case activity.TrackerStateUnknown:
		c.Unknown.Magnitude += weight
	case activity.TrackerStateStationary:
		c.Stationary.Magnitude += weight
	case activity.TrackerStateWalking:
		c.Walking.Magnitude += weight
	case activity.TrackerStateRunning:
		c.Running.Magnitude += weight
	case activity.TrackerStateCycling:
		c.Cycling.Magnitude += weight
	case activity.TrackerStateDriving:
		c.Driving.Magnitude += weight
	case activity.TrackerStateFlying:
		c.Flying.Magnitude += weight
	default:
		panic("unhandled default case")
	}
}

func (c *Cat) dropActivityMode(ct WrappedTrack) {
	weight := ct.TimeOffset.Seconds()
	switch activity.FromAny(ct.Properties["Activity"]) {
	case activity.TrackerStateUnknown:
		c.Unknown.Magnitude -= weight
		c.Unknown.Magnitude = math.Max(c.Unknown.Magnitude, 0)
	case activity.TrackerStateStationary:
		c.Stationary.Magnitude -= weight
		c.Stationary.Magnitude = math.Max(c.Stationary.Magnitude, 0)
	case activity.TrackerStateWalking:
		c.Walking.Magnitude -= weight
		c.Walking.Magnitude = math.Max(c.Walking.Magnitude, 0)
	case activity.TrackerStateRunning:
		c.Running.Magnitude -= weight
		c.Running.Magnitude = math.Max(c.Running.Magnitude, 0)
	case activity.TrackerStateCycling:
		c.Cycling.Magnitude -= weight
		c.Cycling.Magnitude = math.Max(c.Cycling.Magnitude, 0)
	case activity.TrackerStateDriving:
		c.Driving.Magnitude -= weight
		c.Driving.Magnitude = math.Max(c.Driving.Magnitude, 0)
	case activity.TrackerStateFlying:
		c.Flying.Magnitude -= weight
		c.Flying.Magnitude = math.Max(c.Flying.Magnitude, 0)
	default:
		panic("unhandled default case")
	}
}

type Improver struct {
	TransitionWindow         time.Duration
	StationarySpeedThreshold float64
	Cat                      *Cat
}

func NewImprover() *Improver {
	return &Improver{
		TransitionWindow:         20 * time.Second,
		StationarySpeedThreshold: common.SpeedOfWalkingMin,
		Cat:                      NewCat(),
	}
}

func (p *Improver) dropExpiredTracks(ct WrappedTrack) error {
	if len(p.Cat.IntervalPoints) == 0 {
		return nil
	}
	// Drop any old tracks which are out of the transition window.
	outputI := -1
	for i, track := range p.Cat.IntervalPoints {
		span := ct.MustTime().Sub(track.MustTime())
		if span > p.TransitionWindow {
			p.Cat.drop(track)
			continue
		}
		// Index falls within timespan.
		outputI = i
		break
	}
	// If no index within timespan, all expired. Clear.
	if outputI < 0 {
		p.Cat.IntervalPoints = make([]WrappedTrack, 0)
		return nil
	}

	p.Cat.IntervalPoints = p.Cat.IntervalPoints[outputI:]
	return nil
}

// activityAccelerated returns true if the activity is accelerating.
// Use mul=-1 to check for deceleration.
func (p *Improver) activityAccelerated(act activity.Activity, mul float64) bool {
	if mul == 0 {
		mul = 1
	}
	var referenceSpeed float64 = p.StationarySpeedThreshold
	/*
		0.42 / 20 = 0.021
		1.4 / 20 = 0.07
		2.23 / 20 = 0.1115
		5.56 / 20 = 0.278
		6.7 / 20 = 0.335
	*/
	switch act {
	case activity.TrackerStateWalking:
		referenceSpeed = common.SpeedOfWalkingMin
	case activity.TrackerStateRunning:
		referenceSpeed = common.SpeedOfRunningMin
	case activity.TrackerStateCycling:
		referenceSpeed = common.SpeedOfCyclingMin
	case activity.TrackerStateDriving:
		referenceSpeed = common.SpeedOfDrivingMin
	case activity.TrackerStateFlying:
		referenceSpeed = common.SpeedOfCommercialFlight
	}
	return p.Cat.WindowAccelerationReportedSum/p.Cat.WindowSpan.Seconds() <
		mul*(referenceSpeed/p.Cat.WindowSpan.Seconds())
}

// isNapLapTransition returns true if the cat is transitioning from nap to lap or vice versa.
// It relies on the cat state and the current track, so callers
// should call p.push(ct) before calling this function.
func (p *Improver) isNapLapTransition(ct WrappedTrack) bool {
	sortedActsAll := p.Cat.SortedActsAll()

	// The cat is moving.
	// A transition is when the cat stops moving.
	if IsActivityActive(p.Cat.ActivityState) {
		tx := 0
		if p.activityAccelerated(p.Cat.ActivityState, -1) {
			tx++
		}
		if p.Cat.WindowSpeedCalculatedSum/p.Cat.WindowSpan.Seconds() < p.StationarySpeedThreshold &&
			p.Cat.WindowSpeedReportedSum/p.Cat.WindowSpan.Seconds() < p.StationarySpeedThreshold {
			tx++
		}
		if act := sortedActsAll[0]; act.Activity.IsKnown() && !act.Activity.IsActive() {
			tx++
		}
		if ct.Heading() < 0 {
			tx++
		}
		if math.Abs(ct.HeadingDeltaReported) > 90 || math.Abs(ct.HeadingDeltaCalculated) > 120 {
			tx++
		}
		if ct.Speed() < 0 {
			tx++
		}
		if tx >= 4 {
			return true
		}
		return false
	}

	// The cat is stationary.
	// A transition is when the cat starts moving.
	tx := 0
	if p.activityAccelerated(p.Cat.ActivityState, 1) {
		tx++
	}
	if p.Cat.WindowSpeedCalculatedSum/p.Cat.WindowSpan.Seconds() > p.StationarySpeedThreshold &&
		p.Cat.WindowSpeedReportedSum/p.Cat.WindowSpan.Seconds() > p.StationarySpeedThreshold {
		tx++
	}
	if p.Cat.WindowSpeedReportedSum/p.Cat.WindowSpan.Seconds() > common.SpeedOfWalkingMean {
		tx++
	}
	if sortedActsAll[0].Activity.IsActive() {
		tx++
	}
	if tx > 2 {
		return true
	}
	return false
}

func (p *Improver) improve(ct WrappedTrack) error {
	if err := p.dropExpiredTracks(ct); err != nil {
		return err
	}

	ctAct := activity.FromString(ct.Properties.MustString("Activity", ""))
	ctTime := ct.MustTime()
	ctPoint := ct.Point()
	ctReportedSpeed := ct.Properties.MustFloat64("Speed", -1)
	if ctReportedSpeed < 0 {
		ctReportedSpeed = 0
	}

	var timeOffset time.Duration
	if p.Cat.IsUninitialized() {
		timeOffset = 1 * time.Second
	} else {
		timeOffset = ctTime.Sub(p.Cat.Last.MustTime())
	}
	if timeOffset > p.TransitionWindow {
		timeOffset = p.TransitionWindow
	}

	var calculatedSpeed float64
	var accelerationReported float64
	var accelerationCalculated float64
	if !p.Cat.IsWindowEmpty() {
		dist := geo.Distance(p.Cat.Last.Point(), ctPoint)
		calculatedSpeed = dist / timeOffset.Seconds()
		accelerationReported = (ctReportedSpeed - p.Cat.Last.Speed()) / timeOffset.Seconds()
		accelerationCalculated = (calculatedSpeed - p.Cat.Last.SpeedCalculated) / timeOffset.Seconds()
	} else {
		calculatedSpeed = ctReportedSpeed
	}

	var calculatedHeading float64 = -1
	ctHeading := ct.Properties.MustFloat64("Heading", -1)
	if !p.Cat.IsWindowEmpty() {
		calculatedHeading = geo.Bearing(p.Cat.Last.Point(), ctPoint)
	}

	ct.TimeOffset = timeOffset
	ct.SpeedCalculated = calculatedSpeed
	ct.AccelerationReported = accelerationReported
	ct.AccelerationCalculated = accelerationCalculated
	ct.HeadingCalculated = calculatedHeading
	ct.HeadingDeltaReported = ctHeading - p.Cat.Last.Heading()

	defer func() {
		p.Cat.Last = ct
		p.Cat.IntervalPoints = append(p.Cat.IntervalPoints, ct)
	}()

	if p.Cat.IsUninitialized() {
		p.Cat.Start = ctTime
		act := ctAct
		if act == activity.TrackerStateUnknown {
			act = activity.TrackerStateStationary
		}
		p.Cat.setActivityState(ActivityMode{Activity: act}, ctTime)
	}

	// Do the maths.
	p.Cat.push(ct)
	sortedActsKnown := p.Cat.SortedActsKnown()
	sortedActsAll := p.Cat.SortedActsAll()

	if p.isNapLapTransition(ct) {
		// Nap -> Lap
		if !IsActivityActive(p.Cat.ActivityState) {
			for _, act := range sortedActsKnown {
				if act.Activity.IsActive() && act.Magnitude > 0 {
					p.Cat.setActivityState(act, ctTime)
					return nil
				}
			}

			// TODO/FIXME? Default nap -> lap.activity.
			p.Cat.setActivityState(p.Cat.Walking, ctTime)
			return nil
		}
		// Lap -> Nap
		p.Cat.setActivityState(p.Cat.Stationary, ctTime)
		return nil
	}

	// No transition: but is cat is acting as cat was acting...?
	// Maybe revise the activity state.
	activityStateExpired := ctTime.Sub(p.Cat.ActivityStateLastCheck) > p.TransitionWindow
	if !activityStateExpired {
		return nil
	}
	p.Cat.ActivityStateLastCheck = ctTime

	for i, act := range sortedActsAll {
		if act.Magnitude <= 0 {
			continue
		}

		// Same state? Continuity preferred. Return early.
		if act.Activity == p.Cat.ActivityState {
			// Clear any alternate realities.
			p.Cat.setActivityAlternateState(p.Cat.Unknown, ctTime)
			return nil
		}

		// Here we get to fix stationary-labeled tracks that are actually moving.
		// Empirically, stationary tracks are most often mislabeled for
		// driving, rafting, and flying. Sometimes cycling. Hardly ever walking or running.
		if i == 0 && !act.Activity.IsActive() && p.Cat.ActivityState.IsActive() {
			meanSpeed := p.Cat.WindowSpeedCalculatedSum / p.Cat.WindowSpan.Seconds()
			if meanSpeed > common.SpeedOfHighwayDriving*2 {
				p.Cat.setActivityState(p.Cat.Flying, ctTime)
				return nil
			} else if meanSpeed > common.SpeedOfDrivingMin {
				p.Cat.setActivityState(p.Cat.Driving, ctTime)
				return nil
			} else if meanSpeed > common.SpeedOfCyclingMin {
				p.Cat.setActivityState(p.Cat.Cycling, ctTime)
				return nil
			}
		}

		// Blend running:walking, driving:cycling, preferring a long-term incumbent
		// until a continuing alternative overtakes the majority of the incumbent.
		// Note that above, in the case of a repeat activity mode (same state as current),
		// the alternate state is cleared, so this measures only consecutive activities.
		//if act.Activity.IsActive() && p.Cat.ActivityState.IsActive() {
		//	p.Cat.setActivityAlternateState(act, ctTime)
		//	relativeAgeIncumbent := ctTime.Sub(p.Cat.ActivityStateStart)
		//	relativeAge := ctTime.Sub(p.Cat.ActivityAlternateStateStart)
		//	if relativeAge > relativeAgeIncumbent/2 {
		//		p.Cat.setActivityState(act, ctTime)
		//		return nil
		//	}
		//}

		if act.Activity == activity.TrackerStateUnknown {
			continue
		}

		// Different states.
		p.Cat.setActivityState(act, ctTime)
		return nil
	}

	return nil
}

func (c *Cat) setActivityState(act ActivityMode, t time.Time) {
	if act.Activity == c.ActivityState {
		// do something
		return
	}
	c.ActivityStateStart = t
	c.ActivityState = act.Activity
}

func (c *Cat) setActivityAlternateState(act ActivityMode, t time.Time) {
	if act.Activity == c.ActivityAlternateState {
		// do something
		return
	}
	c.ActivityAlternateStateStart = t
	c.ActivityAlternateState = act.Activity
}

func (p *Improver) Improve(ct cattrack.CatTrack) error {
	// Check non-chrneological tracks and reset if out of order.
	if !p.Cat.IsUninitialized() {
		span := ct.MustTime().Sub(p.Cat.Last.MustTime())
		if span < -1*time.Second || span > p.TransitionWindow*2 {
			p.Cat.Reset()
		}
		if span <= time.Second {
			return nil
		}
	}

	return p.improve(WrappedTrack{CatTrack: ct})
}
