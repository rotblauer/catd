package act

import (
	"github.com/paulmach/orb/geo"
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

	WindowAccelerationReported   float64
	WindowAccelerationCalculated float64

	WindowSpeedReported   float64
	WindowSpeedCalculated float64

	ActivityState         activity.Activity
	ActivityStateStart    time.Time
	ActivityStateIsAction bool

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

		ActivityState:         TrackerStateActivityUndetermined,
		ActivityStateStart:    time.Time{},
		ActivityStateIsAction: false,

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
	cc.ActivityState = c.ActivityState
	*c = *cc
}

func (c *Cat) IsUninitialized() bool {
	return c.Start.IsZero()
}

func (c *Cat) IsWindowEmpty() bool {
	return len(c.IntervalPoints) == 0
}

func (c *Cat) SortedActs() []ActivityMode {
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

func ActivityIsAction(act activity.Activity) bool {
	if act <= activity.TrackerStateStationary {
		return false
	}
	return true
}

func (c *Cat) push(ct WrappedTrack) {
	c.WindowSpan += ct.TimeOffset
	c.pushActivityMode(ct)
	c.WindowAccelerationReported += ct.AccelerationReported
	c.WindowAccelerationCalculated += ct.AccelerationCalculated
	c.WindowSpeedReported += ct.Speed() * ct.TimeOffset.Seconds()
	c.WindowSpeedCalculated += ct.SpeedCalculated * ct.TimeOffset.Seconds()
}

func (c *Cat) drop(ct WrappedTrack) {
	c.WindowAccelerationReported -= ct.AccelerationReported
	c.WindowAccelerationCalculated -= ct.AccelerationCalculated
	c.WindowSpeedReported -= ct.Speed() * ct.TimeOffset.Seconds()
	c.WindowSpeedCalculated -= ct.SpeedCalculated * ct.TimeOffset.Seconds()
	c.dropActivityMode(ct)
	c.WindowSpan -= ct.TimeOffset
}

func (c *Cat) pushActivityMode(ct WrappedTrack) {
	weight := ct.TimeOffset.Seconds()
	switch activity.FromAny(ct.Properties["Activity"]) {
	case activity.TrackerStateUnknown:
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
		TransitionWindow:         30 * time.Second,
		StationarySpeedThreshold: 0.42,
		Cat:                      NewCat(),
	}
}

func (p *Improver) dropExpiredTracks(ct WrappedTrack) error {
	if len(p.Cat.IntervalPoints) == 0 {
		return nil
	}
	// Drop any old tracks which are out of the transition window.
	outputI := 0
	for _, track := range p.Cat.IntervalPoints {
		span := ct.MustTime().Sub(track.MustTime())
		if span > p.TransitionWindow {
			p.Cat.drop(track)
		} else {
			p.Cat.IntervalPoints[outputI] = track
			outputI++
		}
	}

	// Prevent memory leak by erasing truncated values
	// (not needed if values don'track contain pointers, directly or indirectly)
	//for j := i; j < len(s); j++ {
	//	s[j] = nil
	//}
	p.Cat.IntervalPoints = p.Cat.IntervalPoints[:outputI]
	return nil
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
		actState := ctAct
		if actState == activity.TrackerStateUnknown {
			actState = activity.TrackerStateStationary
		}
		p.Cat.setActivityState(actState, ctTime)
	}

	p.Cat.push(ct)
	sortedActs := p.Cat.SortedActs()

	isTransition := false
	if ActivityIsAction(p.Cat.ActivityState) {
		// The cat is moving.
		// A transition is when the cat stops moving.
		tx := 0
		if p.Cat.WindowAccelerationReported/p.Cat.WindowSpan.Seconds() < -(p.StationarySpeedThreshold / p.Cat.WindowSpan.Seconds()) {
			tx++
		}
		if p.Cat.WindowSpeedCalculated < p.StationarySpeedThreshold &&
			p.Cat.WindowSpeedReported < p.StationarySpeedThreshold {
			tx++
		}
		if sortedActs[0].Activity == activity.TrackerStateStationary {
			tx++
		}
		if ct.Heading() < 0 {
			tx++
		}
		if math.Abs(ct.HeadingDeltaReported) > 90 {
			tx++
		}
		if tx > 3 {
			isTransition = true
		}
	} else {
		// The cat is stationary.
		// A transition is when the cat starts moving.
		tx := 0
		if p.Cat.WindowAccelerationReported/p.Cat.WindowSpan.Seconds() > p.StationarySpeedThreshold/p.Cat.WindowSpan.Seconds() {
			tx++
		}
		if p.Cat.WindowSpeedCalculated > p.StationarySpeedThreshold &&
			p.Cat.WindowSpeedReported > p.StationarySpeedThreshold {
			tx++
		}
		if sortedActs[0].Activity > activity.TrackerStateStationary {
			tx++
		}
		if tx >= 2 {
			isTransition = true
		}
	}
	if isTransition {
		if !ActivityIsAction(p.Cat.ActivityState) {
			ok := false
			for _, act := range sortedActs {
				if act.Activity > activity.TrackerStateStationary {
					p.Cat.setActivityState(act.Activity, ctTime)
					ok = true
					break
				}
			}
			if !ok {
				// TODO
			}
		} else {
			p.Cat.setActivityState(activity.TrackerStateStationary, ctTime)
		}
	}

	activityStateExpired := ctTime.Sub(p.Cat.ActivityStateStart) > p.TransitionWindow
	if activityStateExpired {
		mostAct := sortedActs[0]
		// TODO: Smooth this by checking the previous state.
		// Driving:Walking (esp. urban commuters)
		// Walking:Driving (esp. urban commuters)
		// Running:Cycling (rye runs)
		// Cycling:Driving (ia bikes)
		// Stationary:Driving (ferry, raft)
		// Stationary:Flying
		if mostAct.Magnitude > 0 {
			p.Cat.setActivityState(mostAct.Activity, ctTime)
		} else {
			//p.Cat.setActivityState(p.Cat.ActivityState, ctTime)
		}
	}

	return nil
}

func (c *Cat) setActivityState(act activity.Activity, t time.Time) {
	c.ActivityState = act
	c.ActivityStateStart = t
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
