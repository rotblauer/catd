package s2

import (
	"github.com/rotblauer/catd/types/activity"
	"github.com/rotblauer/catd/types/cattrack"
	"time"
)

// TrackStackerV1 is an Indexer implementation that tracks the number of times
// a cat has entered and left a cell, as well as the total time spent in the cell.
// It also tracks the activity mode of the cat while in the cell,
// and the first and last times the cat was in the cell.
type TrackStackerV1 struct {
	Count int

	// VisitCount is the number of times the cat has entered and left this cell.
	// It is bounded by a time threshold which the cat needs to exceed
	// in order to be considered as "having left".
	VisitCount     int
	VisitThreshold time.Duration

	FirstTime time.Time
	LastTime  time.Time

	// TotalTimeOffset is the sum of the time (track offsets) spent in the cell.
	TotalTimeOffset time.Duration

	// Activity is the primary activity mode of the cat while in the cell.
	Activity activity.Activity

	// AMx are the activity tallies measured in cumulative time offset seconds.
	AMUnknown    float64
	AMStationary float64
	AMWalking    float64
	AMRunning    float64
	AMBike       float64
	AMAutomotive float64
	AMFly        float64
}

func (ix *TrackStackerV1) IsEmpty() bool {
	return ix.Count == 0
}

func (*TrackStackerV1) ApplyToCatTrack(idxr Indexer, ct cattrack.CatTrack) cattrack.CatTrack {
	pct := &ct
	ixr := idxr.(*TrackStackerV1)
	props := map[string]interface{}{
		"Count":                   ixr.Count,
		"VisitCount":              ixr.VisitCount,
		"FirstTime":               ixr.FirstTime.Format(time.RFC3339),
		"LastTime":                ixr.LastTime.Format(time.RFC3339),
		"TotalTimeOffset":         ixr.TotalTimeOffset.Seconds(),
		"Activity":                ixr.Activity.String(),
		"ActivityMode.Unknown":    ixr.AMUnknown,
		"ActivityMode.Stationary": ixr.AMStationary,
		"ActivityMode.Walking":    ixr.AMWalking,
		"ActivityMode.Running":    ixr.AMRunning,
		"ActivityMode.Bike":       ixr.AMBike,
		"ActivityMode.Automotive": ixr.AMAutomotive,
		"ActivityMode.Fly":        ixr.AMFly,
	}
	pct.SetPropertiesSafe(props)
	return *pct
}

func (*TrackStackerV1) FromCatTrack(ct cattrack.CatTrack) Indexer {
	first, err := time.Parse(time.RFC3339, ct.Properties.MustString("FirstTime", ""))
	if err != nil {
		first = ct.MustTime()
	}
	last, err := time.Parse(time.RFC3339, ct.Properties.MustString("LastTime", ""))
	if err != nil {
		last = ct.MustTime()
	}

	totalOffset := time.Duration(ct.Properties.MustFloat64("TotalTimeOffset", 0)) * time.Second
	if totalOffset == 0 {
		totalOffset = time.Duration(ct.Properties.MustFloat64("TimeOffset", 1)) * time.Second
	}

	out := &TrackStackerV1{
		Count:           ct.Properties.MustInt("Count", 1),
		VisitCount:      ct.Properties.MustInt("VisitCount", 0),
		FirstTime:       first,
		LastTime:        last,
		TotalTimeOffset: totalOffset,
		Activity:        activity.FromString(ct.Properties.MustString("Activity", "Unknown")),
	}

	if v := ct.Properties.MustFloat64("ActivityMode.Unknown", -1); v > 0 {
		out.AMUnknown = v
	} else if out.Activity == activity.TrackerStateUnknown {
		out.AMUnknown = totalOffset.Round(time.Second).Seconds()
	}

	if v := ct.Properties.MustFloat64("ActivityMode.Stationary", -1); v > 0 {
		out.AMStationary = v
	} else if out.Activity == activity.TrackerStateStationary {
		out.AMStationary = totalOffset.Round(time.Second).Seconds()
	}

	if v := ct.Properties.MustFloat64("ActivityMode.Walking", -1); v > 0 {
		out.AMWalking = v
	} else if out.Activity == activity.TrackerStateWalking {
		out.AMWalking = totalOffset.Round(time.Second).Seconds()
	}

	if v := ct.Properties.MustFloat64("ActivityMode.Running", -1); v > 0 {
		out.AMRunning = v
	} else if out.Activity == activity.TrackerStateRunning {
		out.AMRunning = totalOffset.Round(time.Second).Seconds()
	}

	if v := ct.Properties.MustFloat64("ActivityMode.Bike", -1); v > 0 {
		out.AMBike = v
	} else if out.Activity == activity.TrackerStateBike {
		out.AMBike = totalOffset.Round(time.Second).Seconds()
	}

	if v := ct.Properties.MustFloat64("ActivityMode.Automotive", -1); v > 0 {
		out.AMAutomotive = v
	} else if out.Activity == activity.TrackerStateAutomotive {
		out.AMAutomotive = totalOffset.Round(time.Second).Seconds()
	}

	if v := ct.Properties.MustFloat64("ActivityMode.Fly", -1); v > 0 {
		out.AMFly = v
	} else if out.Activity == activity.TrackerStateFlying {
		out.AMFly = totalOffset.Round(time.Second).Seconds()
	}

	return out
}

func (ix *TrackStackerV1) Index(old, next Indexer) Indexer {
	if old == nil || old.IsEmpty() {
		out := next.(*TrackStackerV1)
		if out.VisitCount == 0 {
			out.VisitCount++
		}
		return out
	}

	oldT, nextT := old.(*TrackStackerV1), next.(*TrackStackerV1)

	out := &TrackStackerV1{
		// Relatively sane defaults only for concision.
		FirstTime: oldT.FirstTime,
		LastTime:  nextT.LastTime,

		VisitCount: oldT.VisitCount,

		Count:           oldT.Count + nextT.Count,
		TotalTimeOffset: oldT.TotalTimeOffset + nextT.TotalTimeOffset,
		AMUnknown:       oldT.AMUnknown + nextT.AMUnknown,
		AMStationary:    oldT.AMStationary + nextT.AMStationary,
		AMWalking:       oldT.AMWalking + nextT.AMWalking,
		AMRunning:       oldT.AMRunning + nextT.AMRunning,
		AMBike:          oldT.AMBike + nextT.AMBike,
		AMAutomotive:    oldT.AMAutomotive + nextT.AMAutomotive,
		AMFly:           oldT.AMFly + nextT.AMFly,
	}

	if nextT.FirstTime.Sub(oldT.LastTime) > ix.VisitThreshold {
		if nextT.VisitCount > 0 {
			out.VisitCount += nextT.VisitCount
		} else {
			out.VisitCount++
		}
	} else if oldT.FirstTime.Sub(nextT.LastTime) > ix.VisitThreshold {
		if oldT.VisitCount > 0 {
			out.VisitCount += oldT.VisitCount
		} else {
			out.VisitCount++
		}
	}

	// Correct incorrect defaults, maybe.
	if nextT.FirstTime.Before(oldT.FirstTime) {
		out.FirstTime = nextT.FirstTime
	}
	if oldT.LastTime.After(nextT.LastTime) {
		out.LastTime = oldT.LastTime
	}

	// Find highest-magnitude AMx.
	amMode := activity.TrackerStateUnknown
	amMax := 0.0
	if out.AMUnknown > amMax {
		amMax = out.AMUnknown
		amMode = activity.TrackerStateUnknown
	}
	if out.AMStationary > amMax {
		amMax = out.AMStationary
		amMode = activity.TrackerStateStationary
	}
	if out.AMWalking > amMax {
		amMax = out.AMWalking
		amMode = activity.TrackerStateWalking
	}
	if out.AMRunning > amMax {
		amMax = out.AMRunning
		amMode = activity.TrackerStateRunning
	}
	if out.AMBike > amMax {
		amMax = out.AMBike
		amMode = activity.TrackerStateBike
	}
	if out.AMAutomotive > amMax {
		amMax = out.AMAutomotive
		amMode = activity.TrackerStateAutomotive
	}
	if out.AMFly > amMax {
		amMax = out.AMFly
		amMode = activity.TrackerStateFlying
	}
	out.Activity = amMode

	return out
}
