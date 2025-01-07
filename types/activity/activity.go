package activity

import (
	"github.com/rotblauer/catd/common"
	"math"
	"regexp"
	"slices"
	"time"
)

type Activity int

const (
	TrackerStateStationary Activity = iota
	TrackerStateWalking
	TrackerStateRunning
	TrackerStateBike
	TrackerStateAutomotive
	TrackerStateFlying
	TrackerStateUnknown Activity = -1
)

var AllActivityNames = []string{
	TrackerStateUnknown.String(),
	TrackerStateStationary.String(),
	TrackerStateWalking.String(),
	TrackerStateRunning.String(),
	TrackerStateBike.String(),
	TrackerStateAutomotive.String(),
	TrackerStateFlying.String(),
}

var (
	activityStationary = regexp.MustCompile(`(?i)stationary|still`)
	activityWalking    = regexp.MustCompile(`(?i)walk`)
	activityRunning    = regexp.MustCompile(`(?i)run`)
	activityCycling    = regexp.MustCompile(`(?i)cycle|bike|biking`)
	activityDriving    = regexp.MustCompile(`(?i)drive|driving|automotive`)
	activityFly        = regexp.MustCompile(`(?i)^fly|^air`)
)

// IsActive returns whether the activity is moving. (Yoga is NOT "Active".)
func (a Activity) IsActive() bool {
	return a > TrackerStateStationary && a <= TrackerStateFlying
}

// IsStationary returns whether the activity is stationary.
func (a Activity) IsStationary() bool { return a == TrackerStateStationary }

// IsKnown returns true if the activity is not Unknown.
func (a Activity) IsKnown() bool {
	return a != TrackerStateUnknown
}

// IsUnknown returns true if the activity is Unknown.
func (a Activity) IsUnknown() bool {
	return a == TrackerStateUnknown
}

// IsActiveHuman returns whether the activity is human-powered.
func (a Activity) IsActiveHuman() bool {
	return a >= TrackerStateWalking && a < TrackerStateAutomotive
}

// String implements the Stringer interface.
func (a Activity) String() string {
	switch a {
	case TrackerStateUnknown:
		return "Unknown"
	case TrackerStateStationary:
		return "Stationary"
	case TrackerStateWalking:
		return "Walking"
	case TrackerStateRunning:
		return "Running"
	case TrackerStateBike:
		return "Bike"
	case TrackerStateAutomotive:
		return "Automotive"
	case TrackerStateFlying:
		return "Fly"
	}
	return "Unknown"
}

// Emoji returns a single emoji representation of the activity. Tufte wouldn't mind.
func (a Activity) Emoji() string {
	switch a {
	case TrackerStateUnknown:
		return "❓"
	case TrackerStateStationary:
		return "📍" // 🛑  🧍  🛋  🧎️  📍 `🕴 `
	case TrackerStateWalking:
		return "🚶"
	case TrackerStateRunning:
		return "🏃"
	case TrackerStateBike:
		return "🚴"
	case TrackerStateAutomotive:
		return "🚗" // "🚜" // "🛻"
	case TrackerStateFlying:
		return "✈️ "
	}
	return "❓"
}

// InferFromSpeed infers activity from speed using high -> low max_speed breakpoints.
// maxMul is a multiplier to the max_speed of the activity.
func InferFromSpeed(speed, maxMul float64, mustActive bool) Activity {
	if speed > common.SpeedOfDrivingAutobahn*maxMul {
		return TrackerStateFlying
	}
	if speed > common.SpeedOfCyclingMax*maxMul {
		return TrackerStateAutomotive
	}
	if speed > ((common.SpeedOfRunningMean+common.SpeedOfRunningMax)/2)*maxMul {
		return TrackerStateBike
	}
	if speed > common.SpeedOfWalkingMax*maxMul {
		return TrackerStateRunning
	}
	if !mustActive && speed < common.SpeedOfWalkingMin {
		return TrackerStateStationary
	}
	return TrackerStateWalking
}

var activityMeanSpeeds = map[Activity]float64{
	TrackerStateStationary: -(common.SpeedOfWalkingMin * 0.9),
	TrackerStateWalking:    common.SpeedOfWalkingMean,
	TrackerStateRunning:    common.SpeedOfRunningMean,
	TrackerStateBike:       common.SpeedOfCyclingMean,
	TrackerStateAutomotive: common.SpeedOfDrivingCityUSMean,
	TrackerStateFlying:     common.SpeedOfDrivingAutobahn,
}

// InferSpeedFromClosest infers activity from speed using the closest mean speed.
func InferSpeedFromClosest(speed, maxMul float64, mustActive bool) Activity {
	delta := 100.0
	var closest Activity
	for act, stdSpeed := range activityMeanSpeeds {
		if mustActive && act == TrackerStateStationary {
			continue
		}
		d := math.Abs(speed - stdSpeed)
		if d < delta {
			delta = d
			closest = act
		}
	}
	return closest
}

func IsActivityReasonableForSpeed(a Activity, speed float64) bool {
	if a == TrackerStateUnknown {
		return true
	}
	if a == TrackerStateStationary {
		return speed < common.SpeedOfWalkingMin
	}
	if a == TrackerStateWalking {
		return speed >= common.SpeedOfWalkingMin && speed < common.SpeedOfWalkingMax
	}
	if a == TrackerStateRunning {
		return speed >= common.SpeedOfWalkingMean && speed < common.SpeedOfRunningMax
	}
	if a == TrackerStateBike {
		return speed >= common.SpeedOfWalkingMean && speed < common.SpeedOfDrivingHighway
	}
	if a == TrackerStateAutomotive {
		return speed >= common.SpeedOfWalkingMin && speed < common.SpeedOfDrivingPrettyDamnFast
	}
	if a == TrackerStateFlying {
		return speed >= common.SpeedOfDrivingPrettyDamnFast
	}
	return false
}

// BreakLap configures Lap splitting based on activity.
// It is a hot topic.
func BreakLap(a, b Activity) bool {
	if !a.IsKnown() || !b.IsKnown() {
		return false
	}
	if a.IsActive() != b.IsActive() {
		return true
	}
	// FIXME: This will screw up triathlons;
	// it blends common run+bike, walk+bike, and cycle+drive combos;
	// but it's a workaround for common schizo-activities.
	if a.IsActiveHuman() && b.IsActiveHuman() {
		return false
	}

	delta := math.Abs(float64(int(a) - int(b)))
	return delta > 1

	//if b.IsActiveHuman() && b.IsActiveHuman() {
	//	return false
	//}

	//if a.IsActive() && b.IsActive() {
	//	return true
	//}
	//return a == b

	//return a != b

	//if a == TrackerStateStationary && b >= TrackerStateWalking {
	//	return false
	//}
	//if a >= TrackerStateWalking && b == TrackerStateStationary {
	//	return false
	//}
	//return true
}

func FromAny(a any) Activity {
	if a == nil {
		return TrackerStateUnknown
	}
	reportStr, ok := a.(string)
	if !ok {
		return TrackerStateUnknown
	}
	return FromString(reportStr)
}

func FromString(str string) Activity {
	switch {
	case activityStationary.MatchString(str):
		return TrackerStateStationary
	case activityWalking.MatchString(str):
		return TrackerStateWalking
	case activityRunning.MatchString(str):
		return TrackerStateRunning
	case activityCycling.MatchString(str):
		return TrackerStateBike
	case activityDriving.MatchString(str):
		return TrackerStateAutomotive
	case activityFly.MatchString(str):
		return TrackerStateFlying
	}
	return TrackerStateUnknown
}

// Mode implements basic reasoning about Activity frequency or weighting.
type Mode struct {
	Activity Activity
	Scalar   float64
}

// SortModes describes the sorting order of modes.
// Greater scalar values are ordered first, less scalar values last.
// In case of scalar ties, the "lesser" activity is preferred first.
func SortModes(a, b Mode) int {
	if a.Scalar > b.Scalar {
		return -1
	} else if a.Scalar < b.Scalar {
		return 1
		// In the case of a tie, prefer the "lesser" activity.
		// Sloth is easier, and thus more likely, than action.
	} else if int(a.Activity) < int(b.Activity) {
		return -1
	} else if int(a.Activity) > int(b.Activity) {
		return 1
	} else {
		return 0
	}
}

// Modes is a slice of Mode.
type Modes []Mode

func (s Modes) Len() int           { return len(s) }
func (s Modes) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Modes) Less(i, j int) bool { return s[i].Scalar > s[j].Scalar }

// RelWeights mutates the Modes slice to have relative scalar weights (0 to 1).
func (s Modes) RelWeights() Modes {
	totalWeight := 0.0
	for _, m := range s {
		totalWeight += m.Scalar
	}
	for i := range s {
		s[i].Scalar /= totalWeight
	}
	return s
}

// ModeTracker tracks the activity modes over a sliding, time interval-based window.
type ModeTracker struct {
	IntervalLimit time.Duration
	Acts          []ActRecord

	Unknown    Mode
	Stationary Mode
	Walking    Mode
	Running    Mode
	Cycling    Mode
	Driving    Mode
	Flying     Mode
}

type ActRecord struct {
	A Activity
	T time.Time
	W float64
}

// NewModeTracker creates a new ModeTracker with the given interval.
// The constructor must be used; a zero-value init on the ModeTracker structure will not work.
func NewModeTracker(interval time.Duration) *ModeTracker {
	return &ModeTracker{
		IntervalLimit: interval,
		Acts:          []ActRecord{},
		Unknown:       Mode{TrackerStateUnknown, 0},
		Stationary:    Mode{TrackerStateStationary, 0},
		Walking:       Mode{TrackerStateWalking, 0},
		Running:       Mode{TrackerStateRunning, 0},
		Cycling:       Mode{TrackerStateBike, 0},
		Driving:       Mode{TrackerStateAutomotive, 0},
		Flying:        Mode{TrackerStateFlying, 0},
	}
}

// Push adds an activity record to the ModeTracker.
// It will drop any expired act records.
func (mt *ModeTracker) Push(a Activity, t time.Time, weight float64) {
	// Drop any expired act records.
	for len(mt.Acts) > 0 && t.Sub(mt.Acts[0].T) > mt.IntervalLimit {
		mt.drop(mt.Acts[0].A, mt.Acts[0].W)
		mt.Acts = mt.Acts[1:]
	}
	// Add the new act record.
	if mt.Acts == nil {
		mt.Acts = []ActRecord{ActRecord{a, t, weight}}
	} else {
		mt.Acts = append(mt.Acts, ActRecord{a, t, weight})
	}
	mt.add(a, weight)
}

// Sorted returns the modes sorted by scalar value, with greatest scalars first.
func (mt *ModeTracker) Sorted(onlyKnown bool) Modes {
	modes := Modes{
		mt.Stationary, mt.Walking, mt.Running, mt.Cycling, mt.Driving, mt.Flying,
	}
	if !onlyKnown {
		modes = append(modes, mt.Unknown)
	}
	slices.SortStableFunc(modes, SortModes)
	return modes
}

func (mt *ModeTracker) Reset() {
	mt.Acts = []ActRecord{}
	mt.Unknown.Scalar = 0
	mt.Stationary.Scalar = 0
	mt.Walking.Scalar = 0
	mt.Running.Scalar = 0
	mt.Cycling.Scalar = 0
	mt.Driving.Scalar = 0
	mt.Flying.Scalar = 0
}

func (mt *ModeTracker) Span() time.Duration {
	if len(mt.Acts) < 2 {
		return 0
	}
	return mt.Acts[len(mt.Acts)-1].T.Sub(mt.Acts[0].T)
}

// Has returns true if any of the activity records match the predicate,
// returning on the first truthy return.
func (mt *ModeTracker) Has(predicate func(a ActRecord) bool) bool {
	for _, act := range mt.Acts {
		if predicate(act) {
			return true
		}
	}
	return false
}

func (mt *ModeTracker) add(a Activity, weight float64) {
	switch a {
	case TrackerStateUnknown:
		mt.Unknown.Scalar += weight
	case TrackerStateStationary:
		mt.Stationary.Scalar += weight
	case TrackerStateWalking:
		mt.Walking.Scalar += weight
	case TrackerStateRunning:
		mt.Running.Scalar += weight
	case TrackerStateBike:
		mt.Cycling.Scalar += weight
	case TrackerStateAutomotive:
		mt.Driving.Scalar += weight
	case TrackerStateFlying:
		mt.Flying.Scalar += weight
	}
}

func (mt *ModeTracker) drop(a Activity, weight float64) {
	switch a {
	case TrackerStateUnknown:
		mt.Unknown.Scalar -= weight
	case TrackerStateStationary:
		mt.Stationary.Scalar -= weight
	case TrackerStateWalking:
		mt.Walking.Scalar -= weight
	case TrackerStateRunning:
		mt.Running.Scalar -= weight
	case TrackerStateBike:
		mt.Cycling.Scalar -= weight
	case TrackerStateAutomotive:
		mt.Driving.Scalar -= weight
	case TrackerStateFlying:
		mt.Flying.Scalar -= weight
	}
}
