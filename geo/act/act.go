/*
Package act improves reported activities by cats.
*/

package act

import (
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
	rkalman "github.com/regnull/kalman"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/metrics"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/activity"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"math"
	"sync/atomic"
	"time"
)

const TrackerStateActivityUndetermined = activity.TrackerStateUnknown - 1

type Pos struct {
	First, Last time.Time
	LastTrack   cattrack.CatTrack

	ProbablePt orb.Point

	filter      *rkalman.GeoFilter
	KalmanSpeed float64

	speed        *metrics.NonStandardEWMA
	accuracy     *metrics.NonStandardEWMA
	elevation    *metrics.NonStandardEWMA
	lastHeading  float64
	headingDelta *metrics.NonStandardEWMA

	Activity activity.Activity
	NapPt    orb.Point

	StationaryStart time.Time
	ActiveStart     time.Time
	LastActiveAct   activity.Activity

	// ActivityModeTracker memoizes the last activity modes across the interval.
	// Weighting is by time offset.
	ActivityModeTracker *activity.ModeTracker
	// ProbableActivityModeTracker memoizes the last _probable_ activity modes across the interval.
	// These are the "improved" activities.
	ProbableActivityModeTracker *activity.ModeTracker

	observed int32
}

func (p *Pos) isNapPtEmpty() bool {
	return p.NapPt == orb.Point{}
}

type ProbableCat struct {
	Config *params.ActDiscretionConfig
	Pos    *Pos
}

func NewProbableCat(config *params.ActDiscretionConfig) *ProbableCat {
	if config == nil {
		config = params.DefaultActImproverConfig
	}
	return &ProbableCat{
		Config: config,
		Pos: &Pos{
			ActivityModeTracker: activity.NewModeTracker(config.Interval),
			speed: metrics.NewNonStandardEWMA(
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

func (p *ProbableCat) resetKalmanFilter() {
	p.Pos.filter = NewRKalmanFilter(
		p.Pos.ProbablePt.Lat(),
		p.Pos.speed.Snapshot().Rate(),
		0.1,
	)
}

func (p *ProbableCat) Reset(wt wt) {
	ct := (*cattrack.CatTrack)(&wt)
	p.Pos = &Pos{
		First:                       ct.MustTime(),
		Last:                        ct.MustTime(),
		LastTrack:                   *ct,
		Activity:                    ct.MustActivity(),
		ProbablePt:                  ct.Point(),
		ActivityModeTracker:         activity.NewModeTracker(p.Config.Interval),
		ProbableActivityModeTracker: activity.NewModeTracker(p.Config.Interval),
		speed:                       metrics.NewNonStandardEWMA(metrics.AlphaEWMA(p.Config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
		accuracy:                    metrics.NewNonStandardEWMA(metrics.AlphaEWMA(p.Config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
		elevation:                   metrics.NewNonStandardEWMA(metrics.AlphaEWMA(p.Config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
		headingDelta:                metrics.NewNonStandardEWMA(metrics.AlphaEWMA(p.Config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
	}

	p.resetKalmanFilter()

	if !p.Pos.Activity.IsActive() {
		p.Pos.NapPt = cattrack.CatTrack(wt).Point()
	}
	p.Pos.speed.Update(int64(math.Round(wt.SafeSpeed())))
	p.Pos.speed.SetInterval(time.Second)
	p.Pos.speed.Tick()
	p.Pos.accuracy.Update(int64(math.Round(wt.SafeAccuracy())))
	p.Pos.accuracy.SetInterval(time.Second)
	p.Pos.accuracy.Tick()
	p.Pos.elevation.Update(int64(math.Round(wt.Properties.MustFloat64("Elevation", 0))))
	p.Pos.elevation.SetInterval(time.Second)
	p.Pos.elevation.Tick()

	p.Pos.lastHeading = wt.SafeHeading()

	p.Pos.ActivityModeTracker.Push(ct.MustActivity(), p.Pos.Last, ct.Properties.MustFloat64("TimeOffset", 1))

	if ct.MustActivity().IsActive() {
		p.Pos.LastActiveAct = p.Pos.Activity
	}

	atomic.AddInt32(&p.Pos.observed, 1)
}

// Observe updates the position data with the given (wrapped) CatTrack.
func (p *Pos) Observe(seconds float64, wt wt) {
	atomic.AddInt32(&p.observed, 1)

	p.filterObserve(seconds, wt)

	p.speed.Update(int64(math.Round(wt.SafeSpeed())))
	p.speed.SetInterval(time.Duration(seconds) * time.Second)
	p.speed.Tick()

	p.accuracy.Update(int64(math.Round(wt.SafeAccuracy())))
	p.accuracy.SetInterval(time.Duration(seconds) * time.Second)
	p.accuracy.Tick()

	p.elevation.Update(int64(math.Round(wt.Properties.MustFloat64("Elevation", 0))))
	p.elevation.SetInterval(time.Duration(seconds) * time.Second)
	p.elevation.Tick()

	headingDelta := math.Abs(wt.SafeHeading() - p.lastHeading)
	p.headingDelta.Update(int64(math.Round(headingDelta)))
	p.headingDelta.SetInterval(time.Duration(seconds) * time.Second)
	p.headingDelta.Tick()
	p.lastHeading = wt.SafeHeading()

	ct := (*cattrack.CatTrack)(&wt)
	offset := ct.MustTime().Sub(p.Last)

	p.Last = ct.MustTime()
	p.LastTrack = *ct
	act := ct.MustActivity()
	if act.IsActive() {
		p.LastActiveAct = act
	}
	p.ActivityModeTracker.Push(act, ct.MustTime(), offset.Seconds())
}

func (p *Pos) filterObserve(seconds float64, wt wt) {
	err := p.filter.Observe(seconds, &rkalman.GeoObserved{
		Lat:      cattrack.CatTrack(wt).Point().Lat(),
		Lng:      cattrack.CatTrack(wt).Point().Lon(),
		Altitude: wt.Properties.MustFloat64("Elevation", 0),
		Speed:    wt.SafeSpeed(),
		// GCPS (only) reports speed_accuracy.
		SpeedAccuracy:      wt.Properties.MustFloat64("speed_accuracy", 0.42),
		Direction:          wt.SafeHeading(),
		DirectionAccuracy:  0,
		HorizontalAccuracy: wt.SafeAccuracy(),
		VerticalAccuracy:   2.0,
	})
	if err != nil {
		slog.Error("Kalman.Observe failed", "error", err)
	}
}

func (p *ProbableCat) IsEmpty() bool {
	return p.Pos.observed == 0 || (p.Pos.First.IsZero() && p.Pos.Last.IsZero())
}

func (p *ProbableCat) Add(ct cattrack.CatTrack) error {
	if p.IsEmpty() {
		p.Reset(wt(ct))
		return nil
	}
	if !p.Pos.LastTrack.IsEmpty() && !cattrack.IsCatContinuous(p.Pos.LastTrack, ct) {
		p.Reset(wt(ct))
		return nil
	}
	span := ct.MustTime().Sub(p.Pos.Last)
	if span == 0 {
		return nil
	}
	if span > p.Config.ResetInterval || span < -1*time.Second {
		p.Reset(wt(ct))
		return nil
	}

	p.Pos.Observe(span.Seconds(), wt(ct))
	speedRate := p.Pos.speed.Snapshot().Rate()

	filterEstimate := p.Pos.filter.Estimate()
	if filterEstimate != nil {

		// Workaround a wonky kalman filter.
		// If the speed is too high, reset the filter.
		// What happens is the kalman filter goes haywire, and starts reporting
		// speeds that are way too high, possibly related to poor heading_accuracy input?
		// This catches it, roughly, as/before that happens.
		if filterEstimate.Speed > 3 && filterEstimate.Speed > speedRate*10 {
			// Could be slog.Error, but noisy
			slog.Debug("Kalman.Estimate.speed is 10x the reported 1-minute EWMA rate", "speed", filterEstimate.Speed, "speedRate", speedRate)
			p.resetKalmanFilter()
		} else {
			p.Pos.ProbablePt = orb.Point{filterEstimate.Lng, filterEstimate.Lat}
			p.Pos.KalmanSpeed = filterEstimate.Speed
		}

	} else {
		// Estimate was nil. Reset the filter.
		// Hopefully this will be rare; the above workaround for the Kalman filter
		// should catch most of the cases where the filter goes haywire.
		slog.Error("Kalman.Estimate was nil. Resetting everything.", "cat", ct.CatID())
		p.Reset(wt(ct))
		return nil
	}

	ctAct := ct.MustActivity()
	ctAccuracy := wt(ct).SafeAccuracy()
	minSpeed := math.Min(speedRate, p.Pos.KalmanSpeed)
	minSpeed = math.Min(minSpeed, wt(ct).SafeSpeed())

	inferStationary := 0.0
	if stable, valid := isGyroscopicallyStable(&ct); valid && stable {
		inferStationary += 1000.0
	}
	if !p.Pos.isNapPtEmpty() {
		distNap := geo.Distance(p.Pos.NapPt, p.Pos.ProbablePt)
		if distNap-ctAccuracy < p.Config.Interval.Seconds()*p.Config.Distance {
			inferStationary += 1.0
		} else if distNap+ctAccuracy > p.Config.Interval.Seconds()*p.Config.Distance {
			inferStationary -= 1.0
		}
	}
	if minSpeed < p.Config.SpeedThreshold {
		inferStationary += 1.0
	} else {
		inferStationary -= (minSpeed - p.Config.SpeedThreshold) / p.Config.SpeedThreshold
	}
	if ctAct.IsActive() {
		inferStationary -= 1.0
	} else if ctAct.IsKnown() {
		inferStationary += 1.0
	}
	if w := wt(ct); w.UnsafeHeading() < 0 && w.UnsafeSpeed() < 0 {
		inferStationary += 1.0
	}

	proposal := activity.TrackerStateUnknown
	if inferStationary > 1 {
		p.Pos.NapPt = p.Pos.ProbablePt

		if p.Pos.StationaryStart.IsZero() {
			p.Pos.StationaryStart = ct.MustTime()
		} else if ct.MustTime().Sub(p.Pos.StationaryStart) > p.Config.Interval {
			// Enforcing stationary; timeout elapsed.
			p.Pos.ActiveStart = time.Time{}
			proposal = activity.TrackerStateStationary
		}
	} else if inferStationary < -1 {
		if p.Pos.ActiveStart.IsZero() {
			p.Pos.ActiveStart = ct.MustTime()
		} else if ct.MustTime().Sub(p.Pos.ActiveStart) > p.Config.Interval {
			// Enforcing active; timeout elapsed.
			p.Pos.StationaryStart = time.Time{}
			proposal = p.mustActiveActivity(ct.MustTime(), ctAct, speedRate)
		}
	}
	if !proposal.IsKnown() && ctAct.IsKnown() {
		proposal = ctAct
	}
	if !proposal.IsKnown() {
		if minSpeed > p.Config.SpeedThreshold {
			proposal = p.mustActiveActivity(ct.MustTime(), proposal, speedRate)
		} else {
			proposal = activity.TrackerStateStationary
		}
	}
	// audit proposal to make sure speed vs. activity is logical
	if proposal.IsActive() {
		if minSpeed > common.SpeedOfDrivingFreeway*1.5 {
			proposal = activity.TrackerStateFlying
		}
		if proposal == activity.TrackerStateWalking {
			if minSpeed > common.SpeedOfDrivingMin {
				proposal = activity.TrackerStateAutomotive
			}
		}
	}
	p.decideActivity(proposal, ct)
	return nil
}

func (p *ProbableCat) decideActivity(act activity.Activity, ct cattrack.CatTrack) {
	p.Pos.Activity = act
	p.Pos.ProbableActivityModeTracker.Push(p.Pos.Activity, ct.MustTime(), ct.Properties.MustFloat64("TimeOffset", 1))
}

func (p *ProbableCat) mustActiveActivity(t time.Time, act activity.Activity, speed float64) activity.Activity {
	if act.IsActive() {
		return act
	}
	//if !p.Pos.ActiveStart.IsZero() || t.Sub(p.Pos.StationaryStart) < p.Config.Interval {
	//	return p.Pos.LastActiveAct
	//}
	reportedActModes := p.Pos.ActivityModeTracker.Sorted(true)
	for i := 0; i < 2; i++ {
		if i == 0 && reportedActModes[i].Activity == activity.TrackerStateStationary {
			// This is a heuristic for  bad data: Stationary leads an active cat.
			// Discard this approach.
			break
		}
		mode := reportedActModes[i]
		if mode.Activity.IsActive() {
			return mode.Activity
		}
	}
	//decidedActModes := p.Pos.ProbableActivityModeTracker.Sorted(true)
	//if decidedActModes[0].Activity.IsActive() && decidedActModes[0].Scalar*0.6 > decidedActModes[1].Scalar {
	//	return decidedActModes[0].Activity
	//}
	return activity.InferSpeedFromClosest(speed, 1.0, true)
}
