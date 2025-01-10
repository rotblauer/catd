/*
Package act improves reported activities by cats.
*/

package act

import (
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/metrics"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/activity"
	"github.com/rotblauer/catd/types/cattrack"
	"math"
	"sync/atomic"
	"time"
)

const TrackerStateActivityUndetermined = activity.TrackerStateUnknown - 1

type Pos struct {
	observed int32
	interval time.Duration
	distance float64

	// First and Last are the first and last times the cat was observed.
	// The First field will be filled with the Init method. Last will be updated with each Observe.
	First, Last time.Time
	// LastTrack is the last track observed.
	LastTrack cattrack.CatTrack

	// ProbablePt is the probable point of the cat,
	// perhaps representing a filtered or otherwise improved position.
	ProbablePt orb.Point

	// KalmanFilter is a Kalman filter.
	//kalmanFilter *rkalman.GeoFilter
	//KalmanSpeed  float64

	// NonStandardEWMAs are non-standard because they permit the tick interval to be set.
	speed           *metrics.NonStandardEWMA
	speedCalculated *metrics.NonStandardEWMA
	accuracy        *metrics.NonStandardEWMA
	elevation       *metrics.NonStandardEWMA
	lastHeading     float64
	headingDelta    *metrics.NonStandardEWMA
	gyroSum         *metrics.NonStandardEWMA
	gyroOk          bool

	IReportedAccel float64

	// NapPt is the last observed stationary point.
	// This value is assigned when the cat becomes stationary,
	// and can be reassigned as the cat wiggles while napping (slightly).
	// It may be reset when the cat is inferred to be active.
	NapPt orb.Point

	// Activity is the last known, derived, activity of the cat.
	// This is the canonical activity as judged by this state machine.
	Activity activity.Activity

	// StationaryStart is the time the cat was first inferred to be stationary.
	// This time value will be reset when the cat is inferred to be active.
	StationaryStart time.Time

	// ActiveStart is the time the cat was first inferred to be active.
	ActiveStart time.Time

	// ReportedModes memoizes the last activity modes across the interval.
	// Weighting is by time offset.
	ReportedModes *activity.ModeTracker

	// CanonModes memoizes the last canonical activity modes across the interval.
	// These are the "improved" activities.
	CanonModes *activity.ModeTracker
}

type ProbableCat struct {
	Config *params.ActDiscretionConfig
	Pos    *Pos
}

func (p *ProbableCat) IsEmpty() bool {
	return (p.Pos.First.IsZero() && p.Pos.Last.IsZero()) || p.Pos.speed == nil
}

func NewProbableCat(config *params.ActDiscretionConfig) *ProbableCat {
	if config == nil {
		config = params.DefaultActImproverConfig
	}
	return &ProbableCat{
		Config: config,
		Pos: &Pos{
			Activity:      activity.TrackerStateUnknown,
			ReportedModes: activity.NewModeTracker(config.Interval),
			CanonModes:    activity.NewModeTracker(config.Interval),
		},
	}
}

type wt cattrack.CatTrack

// SafeSpeed always reports a valid speed, defaulting to 0.
func (wt wt) SafeSpeed() float64 {
	speed := wt.Properties.MustFloat64("Speed", 0)
	if math.IsNaN(speed) || math.IsInf(speed, 0) {
		return 0
	}
	return math.Max(0, speed)
}

// UnsafeSpeed reports the speed, defaulting to -1 (unknown).
func (wt wt) UnsafeSpeed() float64 {
	speed := wt.Properties.MustFloat64("Speed", -1)
	if math.IsNaN(speed) || math.IsInf(speed, 0) {
		return -1
	}
	return speed
}

func (wt wt) SafeAccuracy() float64 {
	accuracy := wt.Properties.MustFloat64("Accuracy", 100)
	if math.IsNaN(accuracy) || math.IsInf(accuracy, 0) {
		return 100
	}
	return math.Max(1, accuracy)
}

// SafeHeading always reports a valid heading, defaulting to 0 (north).
func (wt wt) SafeHeading() float64 {
	heading := wt.Properties.MustFloat64("Heading", 0)
	if math.IsNaN(heading) || math.IsInf(heading, 0) {
		return 0
	}
	return math.Max(0, heading)
}

// UnsafeHeading reports the heading, defaulting to -1 (unknown).
func (wt wt) UnsafeHeading() float64 {
	heading := wt.Properties.MustFloat64("Heading", -1)
	if math.IsNaN(heading) || math.IsInf(heading, 0) {
		return -1
	}
	return heading
}

func (p *Pos) resetKalmanFilter() {
	//p.kalmanFilter = NewRKalmanFilter(
	//	p.ProbablePt.Lat(),
	//	p.speed.Snapshot().Rate(),
	//	0.1,
	//)
}

// Init initializes the position data with the given (wrapped) CatTrack.
func NewPos(wt wt, config *params.ActDiscretionConfig) *Pos {
	ct := (*cattrack.CatTrack)(&wt)
	ctAct := ct.MustActivity()
	p := &Pos{
		interval:        config.Interval,
		distance:        config.Distance,
		First:           ct.MustTime(),
		Last:            ct.MustTime(),
		LastTrack:       *ct,
		Activity:        ctAct,
		ProbablePt:      ct.Point(),
		ReportedModes:   activity.NewModeTracker(config.Interval),
		CanonModes:      activity.NewModeTracker(config.Interval),
		speed:           metrics.NewNonStandardEWMA(metrics.AlphaEWMA(config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
		speedCalculated: metrics.NewNonStandardEWMA(metrics.AlphaEWMA(config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
		accuracy:        metrics.NewNonStandardEWMA(metrics.AlphaEWMA(config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
		elevation:       metrics.NewNonStandardEWMA(metrics.AlphaEWMA(config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
		headingDelta:    metrics.NewNonStandardEWMA(metrics.AlphaEWMA(config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
		gyroSum:         metrics.NewNonStandardEWMA(metrics.AlphaEWMA(config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
	}

	//p.resetKalmanFilter()

	if !p.Activity.IsActive() {
		p.NapPt = cattrack.CatTrack(wt).Point()
	}
	p.speed.Update(int64(math.Round(wt.SafeSpeed() * 100)))
	p.speed.SetInterval(time.Second)
	p.speed.Tick()
	p.accuracy.Update(int64(math.Round(wt.SafeAccuracy())))
	p.accuracy.SetInterval(time.Second)
	p.accuracy.Tick()
	p.elevation.Update(int64(math.Round(wt.Properties.MustFloat64("Elevation", 0))))
	p.elevation.SetInterval(time.Second)
	p.elevation.Tick()

	p.lastHeading = wt.SafeHeading()

	p.ReportedModes.Push(ctAct, p.Last, ct.Properties.MustFloat64("TimeOffset", 1))

	atomic.AddInt32(&p.observed, 1)
	return p
}

// Observe updates the position data with the given (wrapped) CatTrack.
func (p *Pos) Observe(offset float64, w wt) error {
	ct := (*cattrack.CatTrack)(&w)
	atomic.AddInt32(&p.observed, 1)

	//// Observe and handle kalman filter estimates.
	//// Do this first because the function MAY use a workaround for a
	//// possibly buggy library, re-Init-ing the whole Pos.
	//if err := p.filterObserve(offset, wt); err != nil {
	//	return err
	//}

	p.speed.Update(int64(math.Round(w.SafeSpeed() * 100)))
	p.speed.SetInterval(time.Duration(offset) * time.Second)
	p.speed.Tick()

	if offset > 0 {
		lastSpeed := wt(p.LastTrack).SafeSpeed()
		p.IReportedAccel = (w.SafeSpeed() - lastSpeed) / offset
	}

	distance := geo.Distance(p.ProbablePt, ct.Point())
	calculatedSpeed := distance / offset

	p.speedCalculated.Update(int64(math.Round(calculatedSpeed * 100)))
	p.speedCalculated.SetInterval(time.Duration(offset) * time.Second)
	p.speedCalculated.Tick()

	p.accuracy.Update(int64(math.Round(w.SafeAccuracy())))
	p.accuracy.SetInterval(time.Duration(offset) * time.Second)
	p.accuracy.Tick()

	p.elevation.Update(int64(math.Round(w.Properties.MustFloat64("Elevation", 0))))
	p.elevation.SetInterval(time.Duration(offset) * time.Second)
	p.elevation.Tick()

	headingDelta := math.Abs(w.SafeHeading() - p.lastHeading)
	p.headingDelta.Update(int64(math.Round(headingDelta)))
	p.headingDelta.SetInterval(time.Duration(offset) * time.Second)
	p.headingDelta.Tick()
	p.lastHeading = w.SafeHeading()

	gyro, ok := gyroSum(ct)
	p.gyroOk = ok
	if p.gyroOk {
		p.gyroSum.Update(int64(math.Round(gyro * 1000)))
		p.gyroSum.SetInterval(time.Duration(offset) * time.Second)
		p.gyroSum.Tick()
	}

	if w.SafeAccuracy() < p.distance {
		p.ProbablePt = ct.Point()
	}
	p.Last = ct.MustTime()
	p.LastTrack = *ct
	p.ReportedModes.Push(ct.MustActivity(), ct.MustTime(), offset)
	return nil
}

// Add adds a CatTrack to the probable cat.
func (p *ProbableCat) Add(ct cattrack.CatTrack) error {
	if p.IsEmpty() {
		p.Pos = NewPos(wt(ct), p.Config)
		return nil
	}
	if !p.Pos.LastTrack.IsEmpty() && !cattrack.IsCatContinuous(p.Pos.LastTrack, ct) {
		p.Pos = NewPos(wt(ct), p.Config)
		return nil
	}
	span := ct.Properties.MustFloat64("TimeOffset", ct.MustTime().Sub(p.Pos.Last).Seconds())
	if span == 0 {
		return nil
	}
	if span > p.Config.ResetInterval.Seconds() || span <= -1 {
		p.Pos = NewPos(wt(ct), p.Config)
		return nil
	}
	if err := p.Pos.Observe(span, wt(ct)); err != nil {
		p.Pos = NewPos(wt(ct), p.Config)
	}
	prop, _ := p.Propose(ct)
	p.onResolveActivity(prop, ct)
	return nil
}

//func (p *Pos) filterObserve(seconds float64, wt wt) error {
//	err := p.kalmanFilter.Observe(seconds, &rkalman.GeoObserved{
//		Lat:      cattrack.CatTrack(wt).Point().Lat(),
//		Lng:      cattrack.CatTrack(wt).Point().Lon(),
//		Altitude: wt.Properties.MustFloat64("Elevation", 0),
//		Speed:    wt.SafeSpeed(),
//		// GCPS (only) reports speed_accuracy.
//		SpeedAccuracy:      wt.Properties.MustFloat64("speed_accuracy", 0.42),
//		Direction:          wt.SafeHeading(),
//		DirectionAccuracy:  0,
//		HorizontalAccuracy: wt.SafeAccuracy(),
//		VerticalAccuracy:   2.0,
//	})
//	if err != nil {
//		slog.Error("Kalman.Observe failed", "error", err)
//		return errors.New("Kalman.Observe failed")
//	}
//
//	filterEstimate := p.kalmanFilter.Estimate()
//
//	// Workaround a wonky kalman filter.
//	// If the speed is too high, reset the filter.
//	// What happens is the kalman filter goes haywire, and starts reporting
//	// speeds that are way too high, possibly related to poor heading_accuracy input?
//	// This catches it, roughly, as/before that happens.
//	speedRate := p.speed.Snapshot().Rate() / 100
//	if filterEstimate != nil {
//		if filterEstimate.Speed > 3 && filterEstimate.Speed > speedRate*10 {
//			// Could be slog.Error, but noisy
//			slog.Debug("Kalman.Estimate.speed is 10x the reported 1-minute EWMA rate", "speed", filterEstimate.Speed, "speedRate", speedRate)
//			p.resetKalmanFilter()
//		} else {
//			p.ProbablePt = orb.Point{filterEstimate.Lng, filterEstimate.Lat}
//			p.KalmanSpeed = filterEstimate.Speed
//			return nil
//		}
//	} else {
//		// Estimate was nil. Reset the filter.
//		// Hopefully this will be rare; the above workaround for the Kalman filter
//		// should catch most of the cases where the filter goes haywire.
//		slog.Error("Kalman.Estimate was nil. Resetting everything.", "cat", (*cattrack.CatTrack)(&wt).CatID())
//		return errors.New("Kalman.Estimate was nil")
//	}
//	if wt.SafeAccuracy() < p.distance {
//		p.ProbablePt = cattrack.CatTrack(wt).Point()
//	}
//	return nil
//}

// Propose proposes a KNOWN activity based on the state of the cat and the current track.
// It is intended primarily to handle high-confidence and bad reported-data cases.
// It tries to use the probable state of the cat instead of the reported data;
// leaving resolving report vs. proposed to the Resolve function.
func (p *ProbableCat) Propose(ct cattrack.CatTrack) (proposed activity.Activity, stationaryConfidence float64) {

	ctAct := ct.MustActivity()
	//ctAccuracy := wt(ct).SafeAccuracy()
	speedRate := p.Pos.speed.Snapshot().Rate() / 100
	speedCalculatedRate := p.Pos.speedCalculated.Snapshot().Rate() / 100
	gyroRate := p.Pos.gyroSum.Snapshot().Rate() / 1000
	//meanSpeedRate := (speedRate + speedCalculatedRate) / 2

	minSpeed := math.Min(speedRate, speedCalculatedRate)
	minSpeed = math.Min(minSpeed, wt(ct).SafeSpeed())
	//minSpeed = math.Min(minSpeed, p.Pos.KalmanSpeed)
	maxSpeed := math.Max(speedRate, speedCalculatedRate)
	maxSpeed = math.Max(maxSpeed, wt(ct).SafeSpeed())

	// Quick returns, strong data.
	// Android friendly; quick stop.
	if p.Pos.gyroOk {
		if gyroRate < GyroscopeStableThresholdReading {
			if ctAct.IsStationary() {
				return ctAct, 9000
			}
			if minSpeed < p.Config.SpeedThreshold {
				return activity.TrackerStateStationary, 1.0
			}
		}
	}
	// iOS friendly; quick stop.
	w := wt(ct)
	if w.UnsafeHeading() < 0 && w.UnsafeSpeed() < 0 && (ctAct.IsStationary() || (minSpeed < p.Config.SpeedThreshold)) {
		return activity.TrackerStateStationary, 0.99
	}
	// Return acceptable reported data.
	if ctAct.IsKnown() && activity.IsActivityReasonableForSpeed(ctAct, minSpeed) {
		return ctAct, 0.9
	}
	// Unknown is "better" client data than unreasonable data.
	// So iterate the assumed-trustworthy reported modes, returning first OK.
	// If reported modes fail, fallback to canon modes, ultimately falling back to speed inference.
	if !ctAct.IsKnown() {
		// Use reported speed because we trust the reporter.
		refSpeed := w.SafeSpeed()

		reportedMode := p.Pos.ReportedModes.Sorted(false).RelWeights()[0]
		if reportedMode.Activity == activity.TrackerStateWalking &&
			!activity.IsActivityReasonableForSpeed(reportedMode.Activity, refSpeed) &&
			activity.IsActivityReasonableForSpeed(activity.TrackerStateAutomotive, minSpeed) {
			return activity.TrackerStateAutomotive, 0.9
		}

		for _, mode := range p.Pos.ReportedModes.Sorted(true).RelWeights() {
			if mode.Activity.IsKnown() && mode.Scalar > 0 && activity.IsActivityReasonableForSpeed(mode.Activity, refSpeed) {
				return mode.Activity, 0.9
			}
		}
		for _, mode := range p.Pos.CanonModes.Sorted(true).RelWeights() {
			if mode.Activity.IsKnown() && mode.Scalar > 0 && activity.IsActivityReasonableForSpeed(mode.Activity, refSpeed) {
				return mode.Activity, 0.9
			}
		}

		// Patch for walk -> run -> bike -> auto sequence.
		prop := activity.InferSpeedFromClosest(refSpeed, false)
		//canonMode := p.Pos.CanonModes.Sorted(true).RelWeights()[0]
		//if prop == activity.TrackerStateBike && canonMode.Activity == activity.TrackerStateRunning {
		//	prop = activity.TrackerStateAutomotive
		//}
		return prop, 0.9
	}
	// Is unreasonable. This is bad data.
	// Dont trust the reports.
	// Fixes unreasonable reporting patterns:
	// - stationary is recorded for obviously non-stationary tracks, eg. driving, flying, in particular
	// - flying is not recorded as flying
	if max := math.Max(speedRate, speedCalculatedRate); max > common.SpeedOfDrivingPrettyDamnFast {
		return activity.TrackerStateFlying, 0.7
	}
	refSpeed := speedCalculatedRate
	for _, mode := range p.Pos.CanonModes.Sorted(true).RelWeights() {
		if mode.Activity.IsKnown() && mode.Scalar > 0 && activity.IsActivityReasonableForSpeed(mode.Activity, refSpeed) {
			return mode.Activity, 0.9
		}
	}
	return activity.InferSpeedFromClosest(refSpeed, false), 0.6
}

func (p *ProbableCat) onResolveActivity(act activity.Activity, ct cattrack.CatTrack) {
	p.Pos.Activity = act
	p.Pos.CanonModes.Push(act, ct.MustTime(), ct.Properties.MustFloat64("TimeOffset", 1))

	mode := p.Pos.CanonModes.Sorted(true).RelWeights()[0]
	if mode.Activity.IsStationary() {

		// Once the stationary timer expires (and the cat's stationary state has not been interrupted,
		// resetting the timer)...
		timerExpired := p.Pos.Last.Sub(p.Pos.StationaryStart) > p.Config.Interval
		if !p.Pos.StationaryStart.IsZero() && timerExpired {
			// Reset competing timer.
			p.Pos.ActiveStart = time.Time{}
		} else if p.Pos.StationaryStart.IsZero() {
			// Start timer.
			p.Pos.StationaryStart = ct.MustTime()
		}
		if p.Pos.NapPt.Equal(orb.Point{}) {
			p.Pos.NapPt = p.Pos.ProbablePt
		}
	} else
	{
		timerExpired := p.Pos.Last.Sub(p.Pos.ActiveStart) > p.Config.Interval
		if !p.Pos.StationaryStart.IsZero() && !p.Pos.ActiveStart.IsZero() && timerExpired {
			// Reset competing timer.
			p.Pos.StationaryStart = time.Time{}
		} else if p.Pos.ActiveStart.IsZero() {
			// Start timer.
			p.Pos.ActiveStart = ct.MustTime()
		}
		if !p.Pos.NapPt.Equal(orb.Point{}) {
			p.Pos.NapPt = orb.Point{}
		}
	}
}
