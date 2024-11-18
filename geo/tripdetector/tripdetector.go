package tripdetector

import (
	"encoding/json"
	"fmt"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
	"github.com/paulmach/orb/planar"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/activity"
	"github.com/rotblauer/catd/types/cattrack"
	"math"
	"time"
)

// TODO: Add persistent state.
// LoadState, SyncState.

// TripDetector normally takes Points and turns them into either a Trip or a Stop.
// It is a state machine that takes in a series of points and returns a series of Trips and Stops.
// Trips are defined as a series of points that are moving, structured as LineStrings.
// Stops are defined as a series of points that are stationary, structured as Points.
type TripDetector struct {
	DwellTime                                time.Duration
	SpeedThreshold                           float64
	DwellDistance                            float64
	LastNPoints                              []*cattrack.CatTrack
	IntervalPoints                           []*cattrack.CatTrack
	Tripping                                 bool
	continuousGyroscopicStabilityGaugeCursor time.Time
	continuousGyroscopicStabilityGauge       float64

	// MotionStateReason is a string that describes the reason for the current state of the TripDetector.
	MotionStateReason string

	// intervalPtsCentroid is the centroid of the last dwell interval points.
	intervalPtsCentroid orb.Point

	segmentIntersectionGauge float64
}

func timespan(pts []*cattrack.CatTrack) time.Duration {
	if len(pts) < 2 {
		return 0
	}
	return pts[len(pts)-1].MustTime().Sub(pts[0].MustTime())
}

func NewTripDetector(config *params.TripDetectorConfig) *TripDetector {
	return &TripDetector{
		DwellTime:      config.DwellInterval,
		SpeedThreshold: config.SpeedThreshold,
		DwellDistance:  config.DwellDistance,

		LastNPoints:    []*cattrack.CatTrack{},
		IntervalPoints: []*cattrack.CatTrack{},

		Tripping:          false,
		MotionStateReason: "init",
	}
}

func (d *TripDetector) ResetState() {
	d.Tripping = false
	d.MotionStateReason = "reset"
	d.intervalPtsCentroid = orb.Point{}
	d.LastNPoints = []*cattrack.CatTrack{}
	d.IntervalPoints = []*cattrack.CatTrack{}
	d.segmentIntersectionGauge = 0
	d.continuousGyroscopicStabilityGauge = 0
}

func (d *TripDetector) AddFeatureToState(f *cattrack.CatTrack) {
	d.IntervalPoints = append(d.IntervalPoints, f)
	for i := len(d.IntervalPoints) - 1; i > 0; i-- {
		// Use a double-size window to hold the interval points.
		// We'll want to bound certain measurements on the dwell time (the interval),
		// so we want some wiggle room in there.
		if timespan(d.IntervalPoints) > d.DwellTime*2 {
			d.IntervalPoints = d.IntervalPoints[i:]
			break
		}
	}

	d.LastNPoints = append(d.LastNPoints, f)
	for len(d.LastNPoints) > 10 {
		d.LastNPoints = d.LastNPoints[1:]
	}

	// Degrade the segment intersection gauge.
	n := float64(0)
	for _, t := range d.IntervalPoints {
		if t.MustTime().Before(t.MustTime().Add(-d.DwellTime)) {
			return
		}
		n += 1.0
	}
	d.segmentIntersectionGauge *= 1 - (1.0 / n)
}

func (d *TripDetector) LastPointN(n int) *cattrack.CatTrack {
	if len(d.LastNPoints) == 0 {
		return nil
	}
	return d.LastNPoints[len(d.LastNPoints)-1-n]
}

func (d *TripDetector) IntervalPointsWhere(predicate func(t *cattrack.CatTrack) bool) []*cattrack.CatTrack {
	out := []*cattrack.CatTrack{}
	for i := range d.IntervalPoints {
		if predicate(d.IntervalPoints[i]) {
			out = append(out, d.IntervalPoints[i])
		}
	}
	return out
}

// AddFeature takes a cattrack.CatTrack and adds it to the state of the TripDetector.
// The TripDetector will then decide if the feature determines a TripDetector state of Tripping or not,
// and will update its state accordingly.
func (d *TripDetector) AddFeature(ct *cattrack.CatTrack) error {
	defer d.AddFeatureToState(ct)

	last := d.LastPointN(0)
	if last != nil {
		// Ensure chronology or reset and return.
		if last.MustTime().After(ct.MustTime()) {
			d.ResetState()
			return nil
		}

		// Short-circuit if signal loss is detected.
		// The tracker went off or lost signal for an appreciable length of time.
		// A discontinuity by absence of record.
		if d.IsDetectSignalLoss(ct) {
			d.Tripping = false
			d.MotionStateReason = "signal loss"
			return nil
		}
	} else {
		//// Last was nil, so we are starting over.
		//// This results in the first point
		//// having "init" as the motionstatereason.
		//d.MotionStateReason = "init"
		//return nil
	}

	weight := detectedNeutral
	idPC := d.DetectStopPointClustering(ct)
	idPCC := d.DetectStopPointClusteringCentroid(ct)
	idX := d.DetectStopIntersection(ct)
	idRS := d.DetectStopReportedSpeeds(ct)
	idO := d.DetectStopOverlaps(ct)
	idA := d.DetectStopReportedActivities(ct)
	idG := d.DetectStopGyroscope(ct)
	idNI := d.DetectStopNetworkInfo(ct)

	d.MotionStateReason = fmt.Sprintf(`idPC: %v, idPCC: %v, idX: %v, idO: %v, idRS: %v, idA: %v, idG: %v, idNI: %v`,
		idPC, idPCC, idX, idO, idRS, idA, idG, idNI)

	weight += idPC
	weight += idPCC
	weight += idX
	weight += idRS
	weight += idO
	weight += idA
	weight += idG
	weight += idNI

	// TODO: tinker?
	if weight < detectedStop {
		d.Tripping = false
		return nil
	}
	if weight > detectedTrip {
		d.Tripping = true
		return nil
	}

	// If we are here, we are UNDECIDED.
	// The TripDetector maintains its state unchanged.

	return nil
}

type DetectedT float64

const (
	detectedStop    DetectedT = -1
	detectedNeutral DetectedT = 0
	detectedTrip    DetectedT = 1
)

// IsDetectSignalLoss is a method that identifies trip ends with signal loss.
/*
		Identifying trip ends with signal loss.

		The dwell time is most frequently used in the existing researches to infer
		trip ends with signal loss. If the time difference between
		two consecutive GPS points exceeds a certain threshold,
	 	we suppose that a potential trip end will occur.
		Based on the previous studies, 120 s is usually employed
		to represent the minimum time gap that an activity
		would reasonably take place. We select GPS records
		with time difference for more than 120 s as the potential
		trip ends. As has been mentioned before, signal loss
		generally occurs due to the signal blocking when volunteers
		are in the indoor buildings or underground. To
		remove the pseudo trip ends, we compare the average
		speed of the signal loss segment (equal to the distance
		traveled divided by time length of the signal loss period)
		with the lower bound of walking with 0.5 m/s. If the
		average speed of the signal loss segment is less than this
		value, then a real trip end is flagged, while if not, we
		consider it as the pseudo one and remove it.
*/
func (d *TripDetector) IsDetectSignalLoss(ct *cattrack.CatTrack) (signalLossDetected bool) {

	last := d.LastPointN(0)
	if last == nil {
		return false
	}

	if dwell := ct.MustTime().Sub(last.MustTime()); dwell > d.DwellTime {
		distance := geo.Distance(ct.Point(), last.Point())
		speed := distance / dwell.Seconds() // m/s
		if speed < d.SpeedThreshold {
			return true
		}
	}
	return false
}

//
//DetectStopPointClustering is a method that identifies trip ends during normal GPS recording.
/*
	Identifying trip ends during normal GPS recording...

	During
	the normal GPS recording, every point is recorded
	chronologically. Trip ends usually perform with the
	point clustering, where sequential GPS points close to
	each other are in an approximate circle area. To infer
	this type of trip ends, we adopt k-means clustering algorithm
	by calculating the maximum distance between
	any two points in the cluster. We define the diameter of
	10 m of the circular cluster. If the maximum distance
	does not exceed this value, the whole cluster will be
	detected as a potential trip end.

	The first point in the
	cluster in the order of time is the starting of the trip end
	and the last point is the terminal of the trip end. In this
	situation, the dwell time also indicates the minimum
	duration that a real activity should occur. A proper
	dwell time should significantly distinguish real trip ends
	from pseudo ones such as waiting for the traffic signal
	or greeting the acquaintance during the trip. Based on
	the specific traffic situations in Shanghai, we assume
	that a vehicle should be less likely to remain absolutely
	stationary for a traffic signal or traffic congestion for
	more than 120 s. Therefore, we take the dwell time of
	120 s to remove the pseudo trip ends. It is assumed that
	there does exist a trip end if the duration of the point
	clustering exceeds 120 s; otherwise, it is treated as the
	pseudo one and will be removed.
*/
func (d *TripDetector) DetectStopPointClustering(ct *cattrack.CatTrack) (result DetectedT) {
	currentTime := ct.MustTime()
	dwellStartMin := currentTime.Add(-d.DwellTime)

	maxDist := 0.0
	dwellExceeded := false
outer:
	for i := len(d.IntervalPoints) - 1; i >= 0; i-- {
		p := d.IntervalPoints[i]
		// ...if the duration of the point clustering exceeds 120s...
		if p.MustTime().Before(dwellStartMin) {
			dwellExceeded = true
			break
		}
		dist := geo.Distance(p.Point(), ct.Point())
		if dist > maxDist {
			maxDist = dist
		}
		if maxDist > d.DwellDistance {
			// If we ever (within the dwell window) exceed the stop cluster distance, we are done.
			// The trip is not yet stopped.
			break outer
		}
	}
	if dwellExceeded && maxDist <= d.DwellDistance /* && reportedMeanSpeeds < d.SpeedThreshold */ {
		return detectedStop
	}
	return detectedTrip
}

func (d *TripDetector) DetectStopPointClusteringCentroid(ct *cattrack.CatTrack) (result DetectedT) {
	dwellExceeded := false

	// Update the d.intervalPtsCentroid value to reflect the centroid of d.IntervalPoints.
	// NOTE We INCLUDE the cursor point in the centroid calculation.
	//
	pts := []orb.Point{ct.Point()}
	// traverse IntervalPoints backwards.
	for i := len(d.IntervalPoints) - 1; i >= 0; i-- {
		p := d.IntervalPoints[i]
		if p.MustTime().Before(ct.MustTime().Add(-d.DwellTime)) {
			dwellExceeded = true
			break
		}
		pts = append(pts, p.Point())
	}
	d.intervalPtsCentroid, _ = planar.CentroidArea(orb.MultiPoint(pts))

	dist := geo.Distance(d.intervalPtsCentroid, ct.Point())
	if dist > d.DwellDistance {
		return detectedTrip
	} else if dwellExceeded {
		return detectedStop
	}
	return detectedNeutral
}

// DetectStopIntersection is a method that identifies trip ends with track point segment intersections.
// This function returns RESULT values that are usually LESS THAN 1 (or the STANDARD unit).
// This will mean that this function will usually be a "tie-breaker," but
/*
	Experimental: identifying trip ends with track point segment intersections.
	When knots are introduced to our lines, interpret this as a trip end.
*/
func (d *TripDetector) DetectStopIntersection(ct *cattrack.CatTrack) (result DetectedT) {
	// Experimental: identifying trip ends with track point segment intersections.
	// When knots are introduced to our lines, interpret this as a trip end.
	dwellIntervalPts := d.IntervalPointsWhere(func(tt *cattrack.CatTrack) bool {
		return tt.MustTime().After(ct.MustTime().Add(-d.DwellTime))
	})

	if len(dwellIntervalPts) > 0 {
		currentSegment := orb.LineString{
			dwellIntervalPts[len(dwellIntervalPts)-1].Point(),
			ct.Point(),
		}

		// Each of the segments iterated in the dwell interval list will be compared
		// with the "current" segment, which is composed by the two latest points.
		// A linestring with 100 coordinates will have 99 segments that get iterated.
		for i := len(dwellIntervalPts) - 1; i > 0; i-- {
			segment := orb.LineString{dwellIntervalPts[i-1].Point(), dwellIntervalPts[i].Point()}
			if isIntersection, _, _ := common.SegmentsIntersect(segment, currentSegment); isIntersection {
				// A hard delta value of 0.025 seemed to work decently.
				// Experimenting with an estimated normal 100-120 points in the ~2minute dwellInterval
				// to approximate this, we can estimate the delta as 1/100 = 0.01.
				delta := 1.0 / float64(len(dwellIntervalPts))

				// Take the min of the proportional value and 0.025
				// to prevent short intervals from being strongly opinionated.
				delta = math.Min(delta, 0.025)

				d.segmentIntersectionGauge += delta
				//break
			}
		}
	}

	return DetectedT(d.segmentIntersectionGauge * float64(detectedStop))
}

// DetectStopOverlaps is a method that identifies trip ends with track point segment overlaps.
/*
	In addition, some short trip ends may take less than
	2 min such as ‘‘picking up or dropping off somebody.’’
	Most existing researches identify this type of trip end by
	examining the change in direction to determine whether
	there exists a trip end. However, only considering the
	change in direction may misidentify turning at the inter-
	sections as the trip ends. Actually, drivers usually take
	the same road links before and after picking up/drop-
	ping off somebody. Thus, we calculate the length of
	overlapped links before and after an abrupt change in
	direction. If the overlapped length exceeds the value of
	50 m (considering the physical size of intersections in
	Shanghai), a trip end is flagged.
*/
func (d *TripDetector) DetectStopOverlaps(ct *cattrack.CatTrack) (result DetectedT) {

	// Experimental: identifying trip ends with track point segment intersections.
	// When knots are introduced to our lines, interpret this as a trip end.
	dwellIntervalPts := d.IntervalPointsWhere(func(tt *cattrack.CatTrack) bool {
		return tt.MustTime().After(ct.MustTime().Add(-d.DwellTime))
	})

	if len(dwellIntervalPts) > 0 {
		currentSegment := orb.LineString{
			dwellIntervalPts[len(dwellIntervalPts)-1].Point(),
			ct.Point(),
		}

		for i := len(dwellIntervalPts) - 1; i > 0; i-- {
			segment := orb.LineString{dwellIntervalPts[i-1].Point(), dwellIntervalPts[i].Point()}

			// I want the length of the segment on the linestring between the intersection.
			// It's a ring.
			ringLen := 0.0
			if isIntersection, _, _ := common.SegmentsIntersect(segment, currentSegment); isIntersection {

				// We were stepping backwards through the dwell-interval points.
				// Now we're going to walk foraward through it, since we know
				// that the points are chronological and all points between 'then' and 'now'
				// are considered part of a self-overlapping segment; a knot.
				// We want to know: How long is the loop in the knot?
				for j := i; j < len(dwellIntervalPts); j++ {
					ringLen += geo.Distance(dwellIntervalPts[j-1].Point(), dwellIntervalPts[j].Point())
					// Note that this overcounts the length by the distance between the intersection and the first point
					// and the distance from the intersection and the last point,
					// which is probably about the length of an average segment.
					// Since I'm working in the context of about 120s dwell intervals
					// and 1pt/second GPS records, this seems fine.
				}

				// We weight the documented 50 meter standard by the length of the ring.
				// A 50m ring returns '-1', but larger rings will not return greater values.
				// Condition on rings at least half the standard size to avoid 'small' knots
				// which can happen when wandering messy forests and urban canyons.
				if ringLen > 25 {
					return DetectedT(math.Min(float64(ringLen)*0.02, 1.0)) * detectedStop
				}
			}
		}
	}

	return detectedNeutral
}

func (d *TripDetector) DetectStopReportedSpeeds(ct *cattrack.CatTrack) (result DetectedT) {

	speed := ct.Properties.MustFloat64("Speed")
	if speed < d.SpeedThreshold*0.8 {
		return detectedStop
	}
	if speed > d.SpeedThreshold*1.2 {
		return detectedTrip
	}
	return detectedNeutral

	//referenceSpeeds := d.IntervalPoints.ReportedSpeeds(ct.MustTime().Add(-d.TripStartInterval))
	//if len(referenceSpeeds) == 0 {
	//	return detectedNeutral
	//}
	//// Get the statistics for this range.
	//referenceStats := stats.Float64Data(referenceSpeeds)
	//mean, _ := referenceStats.Mean()
	//median, _ := referenceStats.Median()
	//if mean > d.SpeedThreshold {
	//	return detectedTrip
	//}
	//if median < d.SpeedThreshold {
	//	return detectedStop
	//}
	//return detectedNeutral
}

func (d *TripDetector) DetectStopReportedActivities(f *cattrack.CatTrack) (result DetectedT) {
	act, ok := f.Properties["Activity"]
	if !ok {
		return detectedNeutral
	}
	activityStr, ok := act.(string)
	if !ok {
		return detectedNeutral
	}
	switch activity.FromString(activityStr) {
	case activity.TrackerStateStationary:
		return detectedStop
	case activity.TrackerStateWalking, activity.TrackerStateRunning, activity.TrackerStateCycling, activity.TrackerStateDriving:
		return detectedTrip
	case activity.TrackerStateUnknown:
		return detectedNeutral
	default:
		panic("unhandled default case")
	}
	return detectedNeutral
}

var gyroscopeProps = []string{"GyroscopeX", "GyroscopeY", "GyroscopeZ"}
var GyroscopeStableThresholdReading = 0.01
var GyroscopeStableThresholdTime = 30 * time.Second

// isGyroscopicallyStable returns true if the feature is considered stable by the gyroscope.
// Valid is returned true only if all gyroscope attributes exist on the feature.
// Only gcps (the Android cat tracker) will have gyroscope readings.
func isGyroscopicallyStable(f *cattrack.CatTrack) (stable, valid bool) {
	sum := 0.0
	for _, prop := range gyroscopeProps {
		v, ok := f.Properties[prop]
		if !ok {
			return false, false
		}
		fl, ok := v.(float64)
		if !ok {
			return false, false
		}
		sum += math.Abs(fl)
	}
	return sum < GyroscopeStableThresholdReading, true
}

// DetectStopGyroscope is a method that identifies trip ends with gyroscope readings.
// We assume that a stable gyroscope reading indicates a stationary cat.
// This function uses atomic measurements; it does not consider the time dimension (ie how long the gyroscope has been stable),
// although that implementation is commented below because it might be better than atomic
// because there are "flashes" of stability that might be noisy. The time dimension would smooth this out.
// Theoretically I don't believe that there is any way for a good zero-sum gyroscope reading to come from
// anything besides a completely resting cat, so the result of this function might do well with a confidence weight.
func (d *TripDetector) DetectStopGyroscope(f *cattrack.CatTrack) (result DetectedT) {

	// If gyroscopically stable points are detected and span 30s continuously,
	// we consider the trip certainly stopped.

	//t := &TrackGeoJSON{f}
	//tt := t.MustTime()
	//if delta := tt.Sub(d.continuousGyroscopicStabilityGaugeCursor).Seconds(); delta > 0 {
	//	d.continuousGyroscopicStabilityGaugeCursor = tt
	//
	//	stable, valid := isGyroscopicallyStable(f)
	//	if stable && valid {
	//		d.continuousGyroscopicStabilityGauge += delta
	//	} else {
	//		d.continuousGyroscopicStabilityGauge = 0
	//	}
	//}
	//if d.continuousGyroscopicStabilityGauge >= GyroscopeStableThresholdTime.Seconds() {
	//	return detectedStop
	//}
	//return detectedNeutral

	// Or, a simpler way.?
	if stable, valid := isGyroscopicallyStable(f); stable && valid {
		return detectedStop
	}
	return detectedNeutral
}

type NetworkInfo struct {
	SSID string `json:"ssid"`
}

// DetectStopNetworkInfo is a method that identifies trip ends with network (Wi-Fi) information.
// We assume that if a cat connects to Wi-Fi it is stationary.
// This assumption could fail...
// - if a cat is using a mobile hotspot on the road,
// - or if a cat is connected to a wide-area Wi-Fi network, like city Wi-Fi or even airport Wi-Fi.
func (d *TripDetector) DetectStopNetworkInfo(f *cattrack.CatTrack) (result DetectedT) {
	if v, ok := f.Properties["NetworkInfo"]; ok {
		if s, ok := v.(string); ok {
			netInfo := NetworkInfo{}
			if err := json.Unmarshal([]byte(s), &netInfo); err == nil && netInfo.SSID != "" {
				return detectedStop
			}
		}
	}
	return detectedNeutral
}
