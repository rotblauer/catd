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
	filter      *rkalman.GeoFilter
	KalmanPt    orb.Point
	KalmanSpeed float64

	speed        *metrics.NonStandardEWMA
	accuracy     *metrics.NonStandardEWMA
	lastHeading  float64
	headingDelta *metrics.NonStandardEWMA

	Activity activity.Activity
	NapPt    orb.Point

	observed int32
}

func (p *Pos) isNapPtEmpty() bool {
	return p.NapPt == orb.Point{}
}

func (p *Pos) filterObserve(seconds float64, wt wt) {
	err := p.filter.Observe(seconds, &rkalman.GeoObserved{
		Lat:                cattrack.CatTrack(wt).Point().Lat(),
		Lng:                cattrack.CatTrack(wt).Point().Lon(),
		Altitude:           wt.Properties.MustFloat64("Elevation", 0),
		Speed:              wt.Speed(),
		SpeedAccuracy:      0.2,
		Direction:          wt.Heading(),
		DirectionAccuracy:  0,
		HorizontalAccuracy: wt.Accuracy(),
		VerticalAccuracy:   2.0,
	})
	if err != nil {
		slog.Error("Kalman.Observe failed", "error", err)
	}
}

func (p *Pos) Observe(seconds float64, wt wt) {
	atomic.AddInt32(&p.observed, 1)

	p.filterObserve(seconds, wt)

	p.speed.Update(int64(math.Round(wt.Speed())))
	p.speed.SetInterval(time.Duration(seconds) * time.Second)
	p.speed.Tick()

	p.accuracy.Update(int64(math.Round(wt.Accuracy())))
	p.accuracy.SetInterval(time.Duration(seconds) * time.Second)
	p.accuracy.Tick()

	headingDelta := math.Abs(wt.Heading() - p.lastHeading)
	p.headingDelta.Update(int64(math.Round(headingDelta)))
	p.headingDelta.SetInterval(time.Duration(seconds) * time.Second)
	p.headingDelta.Tick()
	p.lastHeading = wt.Heading()

	p.Last = (*cattrack.CatTrack)(&wt).MustTime()
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
			speed: metrics.NewNonStandardEWMA(
				metrics.AlphaEWMA(config.Interval.Seconds()/60), time.Second).
			(*metrics.NonStandardEWMA),
			accuracy: metrics.NewNonStandardEWMA(
				metrics.AlphaEWMA(config.Interval.Seconds()/60), time.Second).
			(*metrics.NonStandardEWMA),
			Activity: activity.TrackerStateUnknown,
		},
	}
}

type wt cattrack.CatTrack

func (wt wt) Speed() float64 {
	speed := wt.Properties.MustFloat64("Speed", 0)
	if math.IsNaN(speed) || math.IsInf(speed, 0) {
		return 0
	}
	return math.Max(0, speed)
}

func (wt wt) Accuracy() float64 {
	accuracy := wt.Properties.MustFloat64("Accuracy", 100)
	if math.IsNaN(accuracy) || math.IsInf(accuracy, 0) {
		return 100
	}
	return math.Max(1, accuracy)
}

func (wt wt) Heading() float64 {
	heading := wt.Properties.MustFloat64("Heading", 0)
	if math.IsNaN(heading) || math.IsInf(heading, 0) {
		return 0
	}
	return math.Max(0, heading)
}

func (p *ProbableCat) Reset(wt wt) {
	p.Pos = &Pos{
		First:    (*cattrack.CatTrack)(&wt).MustTime(),
		Last:     (*cattrack.CatTrack)(&wt).MustTime(),
		Activity: (*cattrack.CatTrack)(&wt).MustActivity(),
		filter: NewRKalmanFilter(
			cattrack.CatTrack(wt).Point().Lat(),
			wt.Speed(),
			0.1,
		),
		KalmanPt:     cattrack.CatTrack(wt).Point(),
		speed:        metrics.NewNonStandardEWMA(metrics.AlphaEWMA(p.Config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
		accuracy:     metrics.NewNonStandardEWMA(metrics.AlphaEWMA(p.Config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
		headingDelta: metrics.NewNonStandardEWMA(metrics.AlphaEWMA(p.Config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
	}
	p.Pos.speed.Update(int64(math.Round(wt.Speed())))
	p.Pos.speed.SetInterval(time.Second)
	p.Pos.speed.Tick()
	p.Pos.accuracy.Update(int64(math.Round(wt.Accuracy())))
	p.Pos.accuracy.SetInterval(time.Second)
	p.Pos.accuracy.Tick()
	p.Pos.lastHeading = wt.Heading()
	atomic.AddInt32(&p.Pos.observed, 1)
}

func (p *ProbableCat) IsEmpty() bool {
	return p.Pos.observed == 0 || (p.Pos.First.IsZero() && p.Pos.Last.IsZero())
}

func (p *ProbableCat) Add(ct cattrack.CatTrack) error {
	if p.IsEmpty() {
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
	filterEstimate := p.Pos.filter.Estimate()
	if filterEstimate != nil {
		p.Pos.KalmanPt = orb.Point{filterEstimate.Lng, filterEstimate.Lat}
		p.Pos.KalmanSpeed = filterEstimate.Speed
	}

	speedRate := p.Pos.speed.Snapshot().Rate()
	//accuracyRate := p.Pos.accuracy.Snapshot().Rate()

	ctAct := ct.MustActivity()
	ctAccuracy := wt(ct).Accuracy()
	minSpeed := math.Min(speedRate, p.Pos.KalmanSpeed)
	minSpeed = math.Min(minSpeed, wt(ct).Speed())
	isStationary := 0.0
	if stable, valid := isGyroscopicallyStable(&ct); valid && stable {
		isStationary += 1000.0
	}
	if !p.Pos.isNapPtEmpty() {
		distNap := geo.Distance(p.Pos.NapPt, p.Pos.KalmanPt)
		if distNap-ctAccuracy < p.Config.Interval.Seconds()*p.Config.Distance {
			isStationary += 1.0
		} else if distNap+ctAccuracy > p.Config.Interval.Seconds()*p.Config.Distance {
			isStationary -= 1.0
		}
	}
	if minSpeed < p.Config.SpeedThreshold {
		isStationary += 1.0
	} else {
		isStationary -= (minSpeed - p.Config.SpeedThreshold)
	}
	if ctAct.IsKnown() && !ctAct.IsActive() {
		isStationary += 1.0
	} else if ctAct.IsKnown() && ctAct.IsActive() {
		isStationary -= 1.0
	}

	if isStationary > 1 {
		p.Pos.NapPt = p.Pos.KalmanPt
		p.Pos.Activity = activity.TrackerStateStationary
		return nil
	} else if isStationary < -1 {
		if !ctAct.IsKnown() || !ctAct.IsActive() {
			p.Pos.Activity = fallbackActForSpeed(minSpeed)
			return nil
		}
		p.Pos.Activity = ctAct
		return nil
	}
	p.Pos.Activity = ct.MustActivity()

	return nil
}

func fallbackActForSpeed(speed float64) activity.Activity {
	if speed > common.SpeedOfDrivingAutobahn {
		return activity.TrackerStateFlying
	}
	if speed > common.SpeedOfCyclingMax {
		return activity.TrackerStateAutomotive
	}
	if speed > common.SpeedOfRunningMax {
		return activity.TrackerStateBike
	}
	if speed > common.SpeedOfWalkingMax {
		return activity.TrackerStateRunning
	}
	return activity.TrackerStateWalking
}
