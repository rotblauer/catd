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

	//f.Properties["BearingDeltaRate"] = f.BearingDeltaRate()
	//f.Properties["SelfIntersectionRate"] = f.SelfIntersectionRate()

	return f
}

func (cl *CatLap) Duration() time.Duration {
	return time.Duration(cl.Properties.MustFloat64("Duration")) * time.Second
}

func (cl *CatLap) DistanceTraversed() (distance float64) {
	for i := 0; i < len(cl.Geometry.(orb.LineString)); i++ {
		if i == 0 {
			continue
		}
		// Get the distance between the last and next point.
		lastPt, thisPt := cl.Geometry.(orb.LineString)[i-1], cl.Geometry.(orb.LineString)[i]
		distance += geo.Distance(lastPt, thisPt)
	}
	return
}

// BearingDeltaRate is an experiment in spikeball identification.
func (cl *CatLap) BearingDeltaRate() float64 {
	ls := cl.Geometry.(orb.LineString)
	rr := 0.0
	for i := range ls {
		if i == 0 || i == len(ls)-1 {
			continue
		}
		// Get the angle of the last and next segment.
		lastPt, thisPt, nextPt := ls[i-1], ls[i], ls[i+1]
		lastDistance := geo.Distance(lastPt, thisPt)
		nextDistance := geo.Distance(thisPt, nextPt)
		lastBearing := geo.Bearing(lastPt, thisPt)
		nextBearing := geo.Bearing(thisPt, nextPt)
		bearingDelta := math.Abs(lastBearing - nextBearing)
		r := bearingDelta * (lastDistance + nextDistance)
		rr += r
	}
	return rr / cl.DistanceTraversed()
}

func (cl *CatLap) SelfIntersectionRate() float64 {
	//common.SegmentsIntersect()
	ls := cl.Geometry.(orb.LineString)
	intersectingDistances := 0.0
	segments := []orb.LineString{}
	for i := 0; i < len(ls); i++ {
		if i == 0 {
			continue
		}
		seg := orb.LineString{ls[i-1], ls[i]}
		for j := 0; j < len(segments); j++ {
			intersects, _, _ := common.SegmentsIntersect(seg, segments[j])
			if intersects {
				intersectingDistances += geo.Distance(seg[0], seg[1])
			}
		}
		segments = append(segments, seg)
	}
	// Iterate over the segments.
	for i := 0; i < len(segments); i++ {
		if i == len(segments)-1 {
			break
		}
		for j := i + 1; j < len(segments); j++ {
			intersects, _, _ := common.SegmentsIntersect(segments[i], segments[j])
			if intersects {
				intersectingDistances += geo.Distance(segments[i][0], segments[i][1])
			}
		}
	}

	for i := range ls {
		if i == 0 || i == len(ls)-1 {
			continue
		}
		// Get the angle of the last and next segment.
		lastPt, thisPt, nextPt := ls[i-1], ls[i], ls[i+1]
		lastDistance := geo.Distance(lastPt, thisPt)
		nextDistance := geo.Distance(thisPt, nextPt)

		lastSeg := orb.LineString{lastPt, thisPt}
		nextSeg := orb.LineString{thisPt, nextPt}
		intersects, _, _ := common.SegmentsIntersect(lastSeg, nextSeg)
		if intersects {
			intersectingDistances += lastDistance + nextDistance
		}
	}
	return intersectingDistances / cl.DistanceTraversed()
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
	sorted := actTracker.Sorted(true).RelWeights()
	mode1, mode2 := sorted[0], sorted[1]
	if mode1.Activity.IsActive() && mode2.Activity.IsActive() &&
		mode1.Scalar > 0 && mode2.Scalar > 0 && mode1.Scalar < mode2.Scalar*1.61 {

		// walking:running, running:biking, NOT walking:biking
		if mode1.Activity.IsActiveHuman() && mode2.Activity.IsActiveHuman() &&
			mode1.Activity.DeltaAbs(mode2.Activity) < 2 {
			// return lesser of two
			if int(mode1.Activity) < int(mode2.Activity) {
				return mode1.Activity
			} else {
				return mode2.Activity
			}
		}
		// walking:biking, biking:driving, walking:driving
		// return greater of two
		if int(mode1.Activity) > int(mode2.Activity) {
			return mode1.Activity
		} else {
			return mode2.Activity
		}
	}
	for _, act := range sorted {
		if act.Activity.IsActive() && act.Scalar > 0 {
			return act.Activity
		}
	}

	// At this point there are NO activities that are not either stationary or unknown.
	// This may be a client bug (cough Android cough) where it doesn't report activity.
	// So instead we'll use reported speed.

	// Using common walking speeds, running speeds, bicycling, and driving speeds,
	// we'll return the matching activity.
	return activity.InferFromSpeedMax(meanSpeed, 1.0, true)
}
