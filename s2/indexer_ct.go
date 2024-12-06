package s2

import (
	"github.com/rotblauer/catd/types/activity"
	"github.com/rotblauer/catd/types/cattrack"
	"time"
)

type ICT struct {
	//Indexer
	Count        int
	FirstTime    time.Time
	LastTime     time.Time
	Activity     activity.Activity
	AMUnknown    int `json:"ActivityMode.Unknown"`
	AMStationary int `json:"ActivityMode.Stationary"`
	AMWalking    int `json:"ActivityMode.Walking"`
	AMRunning    int `json:"ActivityMode.Running"`
	AMBike       int `json:"ActivityMode.Bike"`
	AMAutomotive int `json:"ActivityMode.Automotive"`
	AMFly        int `json:"ActivityMode.Fly"`
}

func (ict *ICT) IsEmpty() bool {
	return ict.Count == 0
}

func (ict *ICT) ApplyToCatTrack(idxr Indexer, ct cattrack.CatTrack) cattrack.CatTrack {
	pct := &ct
	ix := idxr.(*ICT)
	pct.SetPropertySafe("Count", ix.Count)
	pct.SetPropertySafe("FirstTime", ix.FirstTime.Format(time.RFC3339))
	pct.SetPropertySafe("LastTime", ix.LastTime.Format(time.RFC3339))
	pct.SetPropertySafe("Activity", ix.Activity.String())
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

	out := &ICT{
		Count:     ct.Properties.MustInt("Count", 1),
		FirstTime: first,
		LastTime:  last,
		Activity:  activity.FromString(ct.Properties.MustString("Activity", "Unknown")),
	}

	if v, ok := ct.Properties["ActivityMode.Unknown"]; ok {
		out.AMUnknown = v.(int)
	} else if out.Activity == activity.TrackerStateUnknown {
		out.AMUnknown = 1
	}

	if v, ok := ct.Properties["ActivityMode.Stationary"]; ok {
		out.AMStationary = v.(int)
	} else if out.Activity == activity.TrackerStateStationary {
		out.AMStationary = 1
	}

	if v, ok := ct.Properties["ActivityMode.Walking"]; ok {
		out.AMWalking = v.(int)
	} else if out.Activity == activity.TrackerStateWalking {
		out.AMWalking = 1
	}

	if v, ok := ct.Properties["ActivityMode.Running"]; ok {
		out.AMRunning = v.(int)
	} else if out.Activity == activity.TrackerStateRunning {
		out.AMRunning = 1
	}

	if v, ok := ct.Properties["ActivityMode.Bike"]; ok {
		out.AMBike = v.(int)
	} else if out.Activity == activity.TrackerStateBike {
		out.AMBike = 1
	}

	if v, ok := ct.Properties["ActivityMode.Automotive"]; ok {
		out.AMAutomotive = v.(int)
	} else if out.Activity == activity.TrackerStateAutomotive {
		out.AMAutomotive = 1
	}

	if v, ok := ct.Properties["ActivityMode.Fly"]; ok {
		out.AMFly = v.(int)
	} else if out.Activity == activity.TrackerStateFlying {
		out.AMFly = 1
	}

	return out
}

func (*ICT) Index(old, next Indexer) Indexer {
	if old == nil || old.IsEmpty() {
		return next.(*ICT)
	}

	oldCT, nextCT := old.(*ICT), next.(*ICT)

	out := &ICT{
		// Relatively sane defaults only for concision.
		FirstTime: oldCT.FirstTime,
		LastTime:  nextCT.LastTime,

		// Sums
		Count:        oldCT.Count + nextCT.Count,
		AMUnknown:    oldCT.AMUnknown + nextCT.AMUnknown,
		AMStationary: oldCT.AMStationary + nextCT.AMStationary,
		AMWalking:    oldCT.AMWalking + nextCT.AMWalking,
		AMRunning:    oldCT.AMRunning + nextCT.AMRunning,
		AMBike:       oldCT.AMBike + nextCT.AMBike,
		AMAutomotive: oldCT.AMAutomotive + nextCT.AMAutomotive,
		AMFly:        oldCT.AMFly + nextCT.AMFly,
	}

	// Correct incorrect defaults, maybe.
	if nextCT.FirstTime.Before(oldCT.FirstTime) {
		out.FirstTime = nextCT.FirstTime
	}
	if oldCT.LastTime.After(nextCT.LastTime) {
		out.LastTime = oldCT.LastTime
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
