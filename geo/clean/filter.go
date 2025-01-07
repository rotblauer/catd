package clean

import (
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"regexp"
)

// FilterPoorAccuracy filters out tracks with poor accuracies.
func FilterPoorAccuracy(ct cattrack.CatTrack) bool {
	accuracy := ct.Properties.MustFloat64("Accuracy")
	return accuracy > 0 && accuracy < params.DefaultCleanConfig.AccuracyThreshold
}

// FilterUltraHighSpeed filters out tracks with unreasonable speeds, for cats.
func FilterUltraHighSpeed(ct cattrack.CatTrack) bool {
	return ct.Properties.MustFloat64("Speed") < common.SpeedOfSound
}

// FilterWildElevation filters out tracks with unreasonable elevations.
func FilterWildElevation(ct cattrack.CatTrack) bool {
	elevation := ct.Properties.MustFloat64("Elevation")
	deepestDive := -100.0
	return elevation > common.ElevationOfDeadSea-deepestDive &&
		elevation < common.ElevationCommercialFlightCruising*1.2
}

var activityFlyRegex = regexp.MustCompile(`(?i)^fly`)

// FilterGrounded filters out tracks that are fast enough to probably be flying.
func FilterGrounded(ct cattrack.CatTrack) bool {
	act := ct.Properties.MustString("Activity", "")
	if matched := activityFlyRegex.MatchString(act); matched {
		return false
	}
	speed := ct.Properties.MustFloat64("Speed", -1)
	if speed > common.SpeedOfDrivingAutobahn {
		return false
	}
	return ct.Properties.MustFloat64("Elevation", 0) < common.ElevationOfEverest
}

func FilterLaps(ct cattrack.CatLap) bool {
	//duration := ct.Properties.MustFloat64("Duration", 0)
	//if duration < 120 {
	//	return false
	//}
	dist := ct.Properties.MustFloat64("Distance_Traversed", 0)
	if dist < 100 {
		return false
	}

	//if ct.BearingDeltaRate() > 180 {
	//	return false
	//}

	// Sanity check for speed.
	// This is a hacky workaround for a spurious pseudo-flight (ia 202411/12) that got
	// logged as a lap.
	speedReportedMean := ct.Properties.MustFloat64("Speed_Reported_Mean", 0)
	speedCalculatedMean := ct.Properties.MustFloat64("Speed_Calculated_Mean", 0)
	if speedCalculatedMean > 10*speedReportedMean {
		return false
	}
	return true
}

func FilterNaps(ct cattrack.CatNap) bool {
	duration := ct.Properties.MustFloat64("Duration", 0)
	return duration > 120
}

func FilterNoEmpty(ct cattrack.CatTrack) bool {
	return !ct.IsEmpty()
}
