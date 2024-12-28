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
	SpeedRate    float64
	accuracy     *metrics.NonStandardEWMA
	accuracyRate float64

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
		SpeedAccuracy:      0.1,
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
		KalmanPt: cattrack.CatTrack(wt).Point(),
		speed:    metrics.NewNonStandardEWMA(metrics.AlphaEWMA(p.Config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
		accuracy: metrics.NewNonStandardEWMA(metrics.AlphaEWMA(p.Config.Interval.Seconds()/60), time.Second).(*metrics.NonStandardEWMA),
	}
}

func (p *ProbableCat) IsEmpty() bool {
	return p.Pos.observed == 0 || p.Pos.First.IsZero() && p.Pos.Last.IsZero()
}

func (p *ProbableCat) Add(ct cattrack.CatTrack) error {
	if p.IsEmpty() {
		p.Reset(wt(ct))
		p.Pos.Observe(1, wt(ct))
		return nil
	}
	span := ct.MustTime().Sub(p.Pos.Last)
	if span == 0 {
		return nil
	}
	if span > p.Config.ResetInterval || span < -1*time.Second {
		p.Reset(wt(ct))
		p.Pos.Observe(1, wt(ct))
		return nil
	}

	p.Pos.Observe(span.Seconds(), wt(ct))
	/*
		type GeoEstimated struct {
		    Lat, Lng, Altitude float64
		    Speed              float64
		    Direction          float64
		    HorizontalAccuracy float64
	*/
	filterEstimate := p.Pos.filter.Estimate()
	if filterEstimate != nil {
		p.Pos.KalmanPt = orb.Point{filterEstimate.Lng, filterEstimate.Lat}
		p.Pos.KalmanSpeed = filterEstimate.Speed
	}

	p.Pos.SpeedRate = p.Pos.speed.Snapshot().Rate()
	p.Pos.accuracyRate = p.Pos.accuracy.Snapshot().Rate()

	minSpeed := math.Min(p.Pos.SpeedRate, p.Pos.KalmanSpeed)
	if minSpeed < p.Config.SpeedThreshold {
		if !p.Pos.KalmanPt.Equal(orb.Point{}) {
			p.Pos.NapPt = p.Pos.KalmanPt
		}
		p.Pos.Activity = activity.TrackerStateStationary
		return nil
	}
	if !p.Pos.isNapPtEmpty() {
		if geo.Distance(p.Pos.NapPt, p.Pos.KalmanPt) <= p.Config.Interval.Seconds()*p.Config.SpeedThreshold {
			p.Pos.Activity = activity.TrackerStateStationary
			return nil
		}
	}

	//p.Pos.NapPt = orb.Point{}
	act := ct.MustActivity()
	if !act.IsKnown() {
		p.Pos.Activity = fallbackActForSpeed(minSpeed)
		return nil
	}
	if !act.IsActive() {
		p.Pos.Activity = fallbackActForSpeed(minSpeed)
		return nil
	}
	p.Pos.Activity = act

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
