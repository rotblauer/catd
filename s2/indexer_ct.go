package s2

import (
	"github.com/rotblauer/catd/types/activity"
	"github.com/rotblauer/catd/types/cattrack"
	"time"
)

type ICT struct {
	Count int

	// VisitCount is the number of times the cat has entered and left this cell.
	// It is bounded by a time threshold which the cat needs to exceed
	// in order to be considered as "having left".
	VisitCount     int
	visitThreshold time.Duration

	FirstTime       time.Time
	LastTime        time.Time
	TotalTimeOffset time.Duration
	Activity        activity.Activity
	AMUnknown       int
	AMStationary    int
	AMWalking       int
	AMRunning       int
	AMBike          int
	AMAutomotive    int
	AMFly           int
}

func (ict *ICT) IsEmpty() bool {
	return ict.Count == 0
}

func (ict *ICT) ApplyToCatTrack(idxr Indexer, ct cattrack.CatTrack) cattrack.CatTrack {
	pct := &ct
	ix := idxr.(*ICT)
	props := map[string]interface{}{
		"Count":                   ix.Count,
		"VisitCount":              ix.VisitCount,
		"FirstTime":               ix.FirstTime.Format(time.RFC3339),
		"LastTime":                ix.LastTime.Format(time.RFC3339),
		"TotalTimeOffset":         ix.TotalTimeOffset.Seconds(),
		"Activity":                ix.Activity.String(),
		"ActivityMode.Unknown":    ix.AMUnknown,
		"ActivityMode.Stationary": ix.AMStationary,
		"ActivityMode.Walking":    ix.AMWalking,
		"ActivityMode.Running":    ix.AMRunning,
		"ActivityMode.Bike":       ix.AMBike,
		"ActivityMode.Automotive": ix.AMAutomotive,
		"ActivityMode.Fly":        ix.AMFly,
	}
	pct.SetPropertiesSafe(props)
	return *pct
}

func (*ICT) FromCatTrack(ct cattrack.CatTrack) Indexer {
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

	out := &ICT{
		Count:           ct.Properties.MustInt("Count", 1),
		VisitCount:      ct.Properties.MustInt("VisitCount", 0),
		FirstTime:       first,
		LastTime:        last,
		TotalTimeOffset: totalOffset,
		Activity:        activity.FromString(ct.Properties.MustString("Activity", "Unknown")),
	}

	if v, ok := ct.Properties["ActivityMode.Unknown"]; ok {
		out.AMUnknown = int(v.(float64))
	} else if out.Activity == activity.TrackerStateUnknown {
		out.AMUnknown = 1
	}

	if v, ok := ct.Properties["ActivityMode.Stationary"]; ok {
		out.AMStationary = int(v.(float64))
	} else if out.Activity == activity.TrackerStateStationary {
		out.AMStationary = 1
	}

	if v, ok := ct.Properties["ActivityMode.Walking"]; ok {
		out.AMWalking = int(v.(float64))
	} else if out.Activity == activity.TrackerStateWalking {
		out.AMWalking = 1
	}

	if v, ok := ct.Properties["ActivityMode.Running"]; ok {
		out.AMRunning = int(v.(float64))
	} else if out.Activity == activity.TrackerStateRunning {
		out.AMRunning = 1
	}

	if v, ok := ct.Properties["ActivityMode.Bike"]; ok {
		out.AMBike = int(v.(float64))
	} else if out.Activity == activity.TrackerStateBike {
		out.AMBike = 1
	}

	if v, ok := ct.Properties["ActivityMode.Automotive"]; ok {
		out.AMAutomotive = int(v.(float64))
	} else if out.Activity == activity.TrackerStateAutomotive {
		out.AMAutomotive = 1
	}

	if v, ok := ct.Properties["ActivityMode.Fly"]; ok {
		out.AMFly = int(v.(float64))
	} else if out.Activity == activity.TrackerStateFlying {
		out.AMFly = 1
	}

	return out
}

func (ict *ICT) Index(old, next Indexer) Indexer {
	if old == nil || old.IsEmpty() {
		out := next.(*ICT)
		if out.VisitCount == 0 {
			out.VisitCount++
		}
		return out
	}

	oldT, nextT := old.(*ICT), next.(*ICT)

	out := &ICT{
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

	if nextT.FirstTime.Sub(oldT.LastTime) > ict.visitThreshold {
		if nextT.VisitCount > 0 {
			out.VisitCount += nextT.VisitCount
		} else {
			out.VisitCount++
		}
	} else if oldT.FirstTime.Sub(nextT.LastTime) > ict.visitThreshold {
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
	amMax := 0
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
