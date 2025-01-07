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
	return (p.Pos.First.IsZero() && p.Pos.Last.IsZero())
}

func NewProbableCat(config *params.ActDiscretionConfig) *ProbableCat {
	if config == nil {
		config = params.DefaultActImproverConfig
	}
	return &ProbableCat{
		Config: config,
		Pos: &Pos{
			interval:      config.Interval,
			distance:      config.Distance,
			ReportedModes: activity.NewModeTracker(config.Interval),
			CanonModes:    activity.NewModeTracker(config.Interval),
			speed: metrics.NewNonStandardEWMA(
				metrics.AlphaEWMA(config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
			speedCalculated: metrics.NewNonStandardEWMA(
				metrics.AlphaEWMA(config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
			accuracy: metrics.NewNonStandardEWMA(
				metrics.AlphaEWMA(config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
			elevation: metrics.NewNonStandardEWMA(
				metrics.AlphaEWMA(config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
			headingDelta: metrics.NewNonStandardEWMA(
				metrics.AlphaEWMA(config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
			Activity: activity.TrackerStateUnknown,
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
func (p *Pos) Observe(offset float64, wt wt) error {
	ct := (*cattrack.CatTrack)(&wt)
	atomic.AddInt32(&p.observed, 1)

	//// Observe and handle kalman filter estimates.
	//// Do this first because the function MAY use a workaround for a
	//// possibly buggy library, re-Init-ing the whole Pos.
	//if err := p.filterObserve(offset, wt); err != nil {
	//	return err
	//}

	p.speed.Update(int64(math.Round(wt.SafeSpeed() * 100)))
	p.speed.SetInterval(time.Duration(offset) * time.Second)
	p.speed.Tick()

	distance := geo.Distance(p.ProbablePt, ct.Point())
	calculatedSpeed := distance / offset

	p.speedCalculated.Update(int64(math.Round(calculatedSpeed * 100)))
	p.speedCalculated.SetInterval(time.Duration(offset) * time.Second)
	p.speedCalculated.Tick()

	p.accuracy.Update(int64(math.Round(wt.SafeAccuracy())))
	p.accuracy.SetInterval(time.Duration(offset) * time.Second)
	p.accuracy.Tick()

	p.elevation.Update(int64(math.Round(wt.Properties.MustFloat64("Elevation", 0))))
	p.elevation.SetInterval(time.Duration(offset) * time.Second)
	p.elevation.Tick()

	headingDelta := math.Abs(wt.SafeHeading() - p.lastHeading)
	p.headingDelta.Update(int64(math.Round(headingDelta)))
	p.headingDelta.SetInterval(time.Duration(offset) * time.Second)
	p.headingDelta.Tick()
	p.lastHeading = wt.SafeHeading()

	p.Last = ct.MustTime()
	p.LastTrack = *ct
	p.ReportedModes.Push(ct.MustActivity(), ct.MustTime(), offset)
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
	prop, conf := p.Propose(ct)
	return p.Resolve(ct, prop, conf)
}

// Propose proposes a KNOWN activity based on the state of the cat and the current track.
// It is intended primarily to handle high-confidence and bad reported-data cases.
// It tries to use the probable state of the cat instead of the reported data;
// leaving resolving report vs. proposed to the Resolve function.
func (p *ProbableCat) Propose(ct cattrack.CatTrack) (proposed activity.Activity, stationaryConfidence float64) {

	ctAct := ct.MustActivity()
	ctAccuracy := wt(ct).SafeAccuracy()
	speedRate := p.Pos.speed.Snapshot().Rate() / 100
	speedCalculatedRate := p.Pos.speedCalculated.Snapshot().Rate() / 100
	//meanSpeedRate := (speedRate + speedCalculatedRate) / 2

	minSpeed := math.Min(speedRate, speedCalculatedRate)
	minSpeed = math.Min(minSpeed, wt(ct).SafeSpeed())
	//minSpeed = math.Min(minSpeed, p.Pos.KalmanSpeed)

	// stationaryConfidence is the confidence weight we assign to the cat being stationary (or not).
	// A value > 1 will indicate actionable confidence that the cat is in a stationary state,
	// while a value < -1 will indicate actionable confidence that the cat is in an active state.
	if stable, valid := isGyroscopicallyStable(&ct); valid && stable {
		stationaryConfidence += 100.0 // if metered... ok
	}
	if !p.Pos.NapPt.Equal(orb.Point{}) {
		distNap := geo.Distance(p.Pos.NapPt, p.Pos.ProbablePt)
		walkingThreshold := p.Config.Interval.Seconds() * common.SpeedOfWalkingMean
		if distNap+ctAccuracy < walkingThreshold {
			stationaryConfidence += 1.0
		} else if distNap-ctAccuracy > walkingThreshold {
			stationaryConfidence -= 1.0
		}
	}
	if minSpeed < p.Config.SpeedThreshold {
		stationaryConfidence += 1.0
	} else {
		stationaryConfidence -= (minSpeed - p.Config.SpeedThreshold) / p.Config.SpeedThreshold
	}
	rModes := p.Pos.ReportedModes.Sorted(true).RelWeights()
	for i := 0; i < 2; i++ {
		if rModes[i].Activity.IsActive() {
			stationaryConfidence -= rModes[i].Scalar
		}
	}
	if ctAct.IsActive() {
		stationaryConfidence -= float64(int(ctAct))
	} else
	if speedRate < common.SpeedOfWalkingMean {
		if ctAct.IsStationary() {
			stationaryConfidence += 1.0
		}
		if w := wt(ct); w.UnsafeHeading() < 0 && w.UnsafeSpeed() < 0 {
			stationaryConfidence += 1.0
		}
		headingRate := p.Pos.headingDelta.Snapshot().Rate()
		if headingRate > 30 {
			stationaryConfidence += 1.0
		}
	}

	// Returns define actionable confidence thresholds.
	proposed = activity.TrackerStateUnknown
	if stationaryConfidence > 1 {
		proposed = activity.TrackerStateStationary
		return proposed, stationaryConfidence
	}
	if stationaryConfidence < -1 {
		if ctAct.IsActive() {
			proposed = ctAct
			return proposed, stationaryConfidence
		}
		canonMode := p.Pos.CanonModes.Sorted(true).RelWeights()[0]
		reportedMode := p.Pos.ReportedModes.Sorted(true).RelWeights()[0]
		if reportedMode.Activity.IsActive() {
			proposed = reportedMode.Activity
		} else
		if canonMode.Activity.IsActive() {
			proposed = canonMode.Activity
		}
		if proposed.IsKnown() && activity.IsActivityReasonableForSpeed(proposed, speedRate) {
			return proposed, stationaryConfidence
		}
		proposed = activity.InferSpeedFromClosest(speedRate, 1.0, false)
		return proposed, stationaryConfidence
	}
	// Indeterminate confidence; use reported data if reasonable.
	if ctAct.IsKnown() && activity.IsActivityReasonableForSpeed(ctAct, speedRate) {
		return ctAct, stationaryConfidence
	}
	// No confidence in reported data; try use historical data.
	proposed = p.Pos.ReportedModes.Sorted(true).RelWeights()[0].Activity
	if proposed.IsKnown() && activity.IsActivityReasonableForSpeed(proposed, speedRate) {
		return proposed, stationaryConfidence
	}
	proposed = p.Pos.CanonModes.Sorted(true).RelWeights()[0].Activity
	if proposed.IsKnown() && activity.IsActivityReasonableForSpeed(proposed, speedRate) {
		return proposed, stationaryConfidence
	}
	// All methods exhausted; infer from speed.
	return activity.InferSpeedFromClosest(speedRate, 1.0, false), stationaryConfidence
}

// Resolve resolves a canonical outcome given disparities between reported and proposed activities.
// It assumes proposed is a known activity, and call a stateful callback before returning, updating
// the probable cat state.
func (p *ProbableCat) Resolve(ct cattrack.CatTrack, proposed activity.Activity, stationaryConfidence float64) error {
	ctAct := ct.MustActivity()
	speedRate := p.Pos.speed.Snapshot().Rate() / 100

	act := activity.TrackerStateUnknown
	defer func() {
		p.onResolveActivity(act, ct)
	}()

	// Resolve same.
	if ct.MustActivity() == proposed {
		act = proposed
		return nil
	}
	// Resolve proposed when cat act is unknown (bad data).
	if ctAct.IsUnknown() {
		act = proposed
		return nil
	}
	// Resolve in favor of proposed in cases of active::stationary.
	if (ctAct.IsActive() != proposed.IsActive()) && (stationaryConfidence > 1 || stationaryConfidence < -1) {
		act = proposed
		return nil
	}

	// Handle "upshifting" cases. These are common ones.
	// Here, we expect
	// - where a run turns in a bike
	// - where a bike turns into a drive
	// - where a walk turns in a bike
	// It's really hard to transition between any of these without stopping at least briefly.
	if ctAct.IsActive() && proposed.IsActive() {
		stopMin := p.Config.Interval.Seconds() / 2
		didStop := p.Pos.CanonModes.Has(func(a activity.ActRecord) bool {
			return a.A.IsStationary() && a.W > stopMin
		})
		if !didStop {
			// So if there was no recorded canonical stop (however brief), we should prefer
			// the lesser of the two activities.
			if int(ctAct) < int(proposed) {
				act = ctAct
			}
			if int(proposed) < int(ctAct) {
				act = proposed
			}
		} else {
			// Else there WAS a stop, and we should prefer the greater activity.
			if int(ctAct) > int(proposed) {
				act = ctAct
			}
			if int(proposed) > int(ctAct) {
				act = proposed
			}
		}
		if act.IsKnown() && activity.IsActivityReasonableForSpeed(act, speedRate) {
			act = proposed
			return nil
		}
	}

	// Resolve on decent reported data.
	if ctAct.IsKnown() && activity.IsActivityReasonableForSpeed(ctAct, speedRate) {
		act = ctAct
		return nil
	}
	// Resolve proposed data.
	act = proposed
	return nil
}

// onResolveActivity is called when an activity is finally resolved.
// It installs the
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
