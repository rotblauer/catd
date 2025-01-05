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

func Lap2Track(lap CatLap) CatTrack {
	return CatTrack(lap)
}

func Lap2TrackP(lap *CatLap) *CatTrack {
	return (*CatTrack)(lap)
}

func (cl CatLap) MarshalJSON() ([]byte, error) {
	return (geojson.Feature)(cl).MarshalJSON()
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

	ff := geojson.NewFeature(orb.LineString{})
	f := (*CatLap)(ff)

	first, last := tracks[0], tracks[len(tracks)-1]

	f.Properties["Alias"] = first.CatID().String()
	f.Properties["UUID"] = first.Properties.MustString("UUID")
	f.Properties["RawPointCount"] = len(tracks) // unsimplified

	firstTime, lastTime := first.MustTime(), last.MustTime()
	f.Properties["Time_Start_Unix"] = firstTime.Unix()
	f.Properties["Time_Start_RFC3339"] = firstTime.Format(time.RFC3339)
	f.Properties["Time_End_Unix"] = lastTime.Unix()
	f.Properties["Time_End_RFC3339"] = lastTime.Format(time.RFC3339)
	f.Properties["Duration"] = lastTime.Sub(firstTime).Round(time.Second).Seconds()

	accuracies := make([]float64, 0, len(tracks))
	elevations := make([]float64, 0, len(tracks))
	reportedSpeeds := make([]float64, 0, len(tracks))
	calculatedSpeeds := make([]float64, 0, len(tracks)-1)

	distanceTraversed := 0.0
	elevationGain, elevationLoss := 0.0, 0.0

	for i := 0; i < len(tracks); i++ {
		track := tracks[i]

		f.Geometry = append(f.Geometry.(orb.LineString), track.Point())

		accuracies = append(accuracies, track.Properties.MustFloat64("Accuracy", 0))
		elevation := track.Properties.MustFloat64("Elevation", 0)
		elevations = append(elevations, math.Round(elevation))
		reportedSpeeds = append(reportedSpeeds, track.Properties.MustFloat64("Speed", 0))

		if i == 0 {
			continue
		}

		prev := tracks[i-1]
		meters := geo.Distance(prev.Point(), track.Point())
		distanceTraversed += meters

		seconds := MustContinuousTimeOffset(*prev, *track).Seconds()
		if seconds == 0 {
			continue
		}
		calculatedSpeeds = append(calculatedSpeeds, meters/seconds)

		elevationDelta := elevation - elevations[i-1]
		if elevationDelta > 0 {
			elevationGain += math.Abs(elevationDelta)
		} else {
			elevationLoss += math.Abs(elevationDelta)
		}
	}

	statsMustFloat := func(fn func() (float64, error), def float64) float64 {
		out, err := fn()
		if err != nil {
			return def
		}
		return out
	}

	installStats := func(key string, data []float64, def float64, precision int) {
		statsData := stats.Float64Data(data)
		f.Properties[key+"_Mean"] = common.DecimalToFixed(statsMustFloat(statsData.Mean, def), precision)
		f.Properties[key+"_Median"] = common.DecimalToFixed(statsMustFloat(statsData.Median, def), precision)
		f.Properties[key+"_Min"] = common.DecimalToFixed(statsMustFloat(statsData.Min, def), precision)
		f.Properties[key+"_Max"] = common.DecimalToFixed(statsMustFloat(statsData.Max, def), precision)
	}

	installStats("Accuracy", accuracies, 50, 0)
	installStats("Elevation", elevations, 0, 0)
	installStats("Speed_Reported", reportedSpeeds, 0, 2)
	installStats("Speed_Calculated", calculatedSpeeds, 0, 2)

	f.Properties["Distance_Traversed"] = math.Round(distanceTraversed)
	f.Properties["Distance_Absolute"] = math.Round(geo.Distance(tracks[0].Point(), tracks[len(tracks)-1].Point()))
	f.Properties["Elevation_Gain"] = math.Floor(elevationGain)
	f.Properties["Elevation_Loss"] = math.Floor(elevationLoss)

	// FIXME: Another list iteration and awkward type assertions.
	f.Properties["Activity"] = inferLapActivity(tracks, f.Properties.MustFloat64("Speed_Reported_Mean", 0)).String()

	return f
}

func (cl *CatLap) DistanceTraversed() float64 {
	return cl.Properties.MustFloat64("Distance_Traversed")
}

func (cl *CatLap) Duration() time.Duration {
	return time.Duration(cl.Properties.MustFloat64("Duration")) * time.Second
}

func inferLapActivity(list []*CatTrack, meanSpeed float64) activity.Activity {
	interval := list[len(list)-1].MustTime().Sub(list[0].MustTime())
	actTracker := activity.NewModeTracker(interval)
	for _, track := range list {
		actTracker.Push(track.MustActivity(), track.MustTime(), track.Properties.MustFloat64("TimeOffset", 1))
	}

	// Problem: rye runs too fast, gets cycle laps.
	// Solution: use speed to infer activity, comparing first two sorted-mode activities iff they're relatively close in mode.
	// ie. If the top-two modes are *roughly* co-occurring, try match either of the two to the lap's mean speed.
	sorted := actTracker.Sorted(true)
	first, second := sorted[0], sorted[1]
	if first.Activity.IsActive() && second.Activity.IsActive() && first.Scalar < second.Scalar*1.5 {
		guess := activity.InferSpeedFromClosest(meanSpeed, 0.9, true)
		if guess == first.Activity || guess == second.Activity {
			return guess
		}
	}
	for _, act := range sorted {
		if act.Activity.IsActive() {
			return act.Activity
		}
	}

	// At this point there are NO activities that are not either stationary or unknown.
	// This may be a client bug (cough Android cough) where it doesn't report activity.
	// So instead we'll use reported speed.

	// Using common walking speeds, running speeds, bicycling, and driving speeds,
	// we'll return the matching activity.
	return activity.InferSpeedFromClosest(meanSpeed, 1.0, true)
}
