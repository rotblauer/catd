package act

import (
	//"github.com/VividCortex/ewma"
	"github.com/paulmach/orb"
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
}

type Cat struct {
	// Instantaneous raw gauges
	Start           time.Time
	Time            time.Time
	Location        orb.Point
	ReportedSpeed   float64
	ReportedHeading float64

	// Instantaneous derived gauges
	CalculatedSpeed   float64
	CalculatedHeading float64

	AccelerationReported   float64
	AccelerationCalculated float64

	IntervalPoints []WrappedTrack

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
		Time:           time.Time{},
		IntervalPoints: make([]WrappedTrack, 0),

		ReportedSpeed:     -1,
		CalculatedSpeed:   -1,
		ReportedHeading:   -1,
		CalculatedHeading: -1,

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
		TransitionWindow:         1 * time.Minute,
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
			p.Cat.dropActivityMode(track)
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
		timeOffset = ctTime.Sub(p.Cat.Time)
	}
	if timeOffset > p.TransitionWindow {
		timeOffset = p.TransitionWindow
	}
	ct.TimeOffset = timeOffset

	var calculatedSpeed float64
	var accelerationReported float64
	var accelerationCalculated float64
	if !p.Cat.IsWindowEmpty() {
		dist := geo.Distance(p.Cat.Location, ctPoint)
		calculatedSpeed = dist / timeOffset.Seconds()
		accelerationReported = (ctReportedSpeed - p.Cat.ReportedSpeed) / timeOffset.Seconds()
		accelerationCalculated = (calculatedSpeed - p.Cat.CalculatedSpeed) / timeOffset.Seconds()
	} else {
		calculatedSpeed = ctReportedSpeed
	}

	var calculatedHeading float64 = -1
	ctHeading := ct.Properties.MustFloat64("Heading", -1)
	if !p.Cat.IsWindowEmpty() {
		calculatedHeading = geo.Bearing(p.Cat.Location, ctPoint)
	}

	defer func() {
		if p.Cat.IsUninitialized() {
			p.Cat.Start = ctTime
		}
		p.Cat.Time = ctTime
		p.Cat.Location = ctPoint
		p.Cat.ReportedSpeed = ctReportedSpeed
		p.Cat.CalculatedSpeed = calculatedSpeed
		p.Cat.ReportedHeading = ctHeading
		p.Cat.CalculatedHeading = calculatedHeading
		p.Cat.AccelerationReported = accelerationReported
		p.Cat.AccelerationCalculated = accelerationCalculated
		p.Cat.IntervalPoints = append(p.Cat.IntervalPoints, ct)
	}()

	if p.Cat.IsUninitialized() {
		actState := ctAct
		if actState == activity.TrackerStateUnknown {
			actState = activity.TrackerStateStationary
		}
		p.Cat.setActivityState(actState, ctTime)
	}

	p.Cat.pushActivityMode(ct)

	activityStateExpired := ctTime.Sub(p.Cat.ActivityStateStart) > p.TransitionWindow
	if activityStateExpired {
		mostAct := p.Cat.SortedActs()[0]
		if mostAct.Magnitude > 0 {
			p.Cat.setActivityState(mostAct.Activity, ctTime)
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
	span := ct.MustTime().Sub(p.Cat.Time)
	if span < -1*time.Second || span > p.TransitionWindow*2 {
		p.Cat.Reset()
	}
	if span <= time.Second {
		return nil
	}

	return p.improve(WrappedTrack{CatTrack: ct})
}
