package cattrack

import (
	"github.com/montanaflynn/stats"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
	"github.com/paulmach/orb/geojson"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/types/activity"
	"math"
	"time"
)

type CatLap geojson.Feature

func (cl *CatLap) MarshalJSON() ([]byte, error) {
	return (*geojson.Feature)(cl).MarshalJSON()
}

func (cl *CatLap) UnmarshalJSON(data []byte) error {
	return (*geojson.Feature)(cl).UnmarshalJSON(data)
}

func (cl *CatLap) IsValid() bool {
	_, ok := cl.Geometry.(orb.LineString)
	return ok
}

func NewCatLap(tracks []*CatTrack) *CatLap {
	if len(tracks) < 2 {
		return nil
	}

	f := geojson.NewFeature(orb.LineString{})

	// FIXME: Another list iteration and awkward type assertions.
	f.Properties["Activity"] = ActivityModeNotUnknownNorStationary(tracks).String()

	first, last := tracks[0], tracks[len(tracks)-1]
	firstTime, lastTime := first.MustTime(), last.MustTime()
	f.Properties["Name"] = first.Properties.MustString("Name")
	f.Properties["UUID"] = first.Properties.MustString("UUID")
	f.Properties["RawPointCount"] = len(tracks) // unsimplified
	f.Properties["Time"] = map[string]any{
		"Start": map[string]any{
			"Unix":    firstTime.Unix(),
			"RFC3339": firstTime.Format(time.RFC3339),
		},
		"End": map[string]any{
			"Unix":    lastTime.Unix(),
			"RFC3339": lastTime.Format(time.RFC3339),
		},
		"Duration": lastTime.Sub(firstTime).Round(time.Second).Seconds(),
	}

	accuracies := make([]float64, 0, len(tracks))
	activities := make([]activity.Activity, 0, len(tracks))
	elevations := make([]float64, 0, len(tracks))
	reportedSpeeds := make([]float64, 0, len(tracks))
	calculatedSpeeds := make([]float64, 0, len(tracks)-1)

	distanceTraversed := 0.0
	elevationGain, elevationLoss := 0.0, 0.0

	for i := 0; i < len(tracks); i++ {
		track := tracks[i]

		f.Geometry = append(f.Geometry.(orb.LineString), track.Point())

		accuracies = append(accuracies, track.Properties.MustFloat64("Accuracy", 0))
		activities = append(activities, activity.FromAny(track.Properties["Activity"]))
		elevation := track.Properties.MustFloat64("Elevation", 0)
		elevations = append(elevations, math.Round(elevation))
		reportedSpeeds = append(reportedSpeeds, track.Properties.MustFloat64("Speed", 0))

		if i == 0 {
			continue
		}

		prev := tracks[i-1]
		meters := geo.Distance(track.Point(), prev.Point())
		distanceTraversed += meters

		seconds := track.MustTime().Sub(prev.MustTime()).Seconds()
		calculatedSpeeds = append(calculatedSpeeds, meters/seconds)

		elevationDelta := elevation - elevations[i-1]
		if elevationDelta > 0 {
			elevationGain += math.Abs(elevationDelta)
		} else {
			elevationLoss += math.Abs(elevationDelta)
		}
	}

	statsMustFloat := func(fn func() (float64, error)) float64 {
		out, _ := fn()
		return out
	}

	mustGetStats := func(data []float64, precision int) map[string]float64 {
		statsData := stats.Float64Data(data)
		return map[string]float64{
			"Mean":   common.DecimalToFixed(statsMustFloat(statsData.Mean), precision),
			"Median": common.DecimalToFixed(statsMustFloat(statsData.Median), precision),
			"Min":    common.DecimalToFixed(statsMustFloat(statsData.Min), precision),
			"Max":    common.DecimalToFixed(statsMustFloat(statsData.Max), precision),
		}
	}

	f.Properties["Accuracy"] = mustGetStats(accuracies, 0)
	f.Properties["Speed"] = map[string]any{
		"Reported":   mustGetStats(reportedSpeeds, 2),
		"Calculated": mustGetStats(calculatedSpeeds, 2),
	}
	f.Properties["Distance"] = map[string]float64{
		"Traversed": math.Round(distanceTraversed),
		"Absolute":  math.Round(geo.Distance(tracks[0].Point(), tracks[len(tracks)-1].Point())),
	}
	f.Properties["Elevation"] = map[string]float64{
		"Gain": math.Floor(elevationGain),
		"Loss": math.Floor(elevationLoss),
	}

	return (*CatLap)(f)
}

func ActivityMode(list []*CatTrack) activity.Activity {
	activities := make([]float64, len(list))
	for i, f := range list {
		act := activity.FromAny(f.Properties["Activity"])
		activities[i] = float64(act)
	}
	activitiesStats := stats.Float64Data(activities)
	mode, _ := activitiesStats.Mode()
	if len(mode) == 0 {
		return activity.TrackerStateUnknown
	}
	return activity.Activity(mode[0])
}

func ActivityModeNotUnknownNorStationary(list []*CatTrack) activity.Activity {
	activities := []float64{}
	for _, f := range list {
		act := activity.FromAny(f.Properties["Activity"])
		if act > activity.TrackerStateStationary {
			activities = append(activities, float64(act))
		}
	}
	activitiesStats := stats.Float64Data(activities)
	mode, _ := activitiesStats.Mode()
	for _, m := range mode {
		if m > float64(activity.TrackerStateStationary) {
			return activity.Activity(m)
		}
	}

	// At this point there are NO activities that are not either stationary or unknown.
	// This may be a client bug (cough Android cough) where it doesn't report activity.
	// So instead we'll use reported speed.
	speeds := []float64{}
	for _, f := range list {
		speeds = append(speeds, f.Properties.MustFloat64("Speed"))
	}
	speedsStats := stats.Float64Data(speeds)

	// Remember, these are meters per second.
	mean, _ := speedsStats.Mean()

	// Using common walking speeds, running speeds, bicycling, and driving speeds,
	// we'll return the matching activity.
	if mean < 1.78816 /* 4 mph */ {
		return activity.TrackerStateWalking
	} else if mean < 4.87274 /* 10.9 mph == 5.5 min / mile */ {
		return activity.TrackerStateRunning
	} else if mean < 8.04672 /* 18 mph */ {
		return activity.TrackerStateCycling
	}
	return activity.TrackerStateDriving
}
