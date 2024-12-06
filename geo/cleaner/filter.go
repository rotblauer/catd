package cleaner

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
		elevation < common.ElevationCommercialFlightMean*1.2
}

var activityFlyRegex = regexp.MustCompile(`(?i)^fly`)

// FilterGrounded filters out tracks that are fast enough to probably be flying.
func FilterGrounded(ct cattrack.CatTrack) bool {
	act := ct.Properties.MustString("Activity", "")
	if matched := activityFlyRegex.MatchString(act); matched {
		return false
	}
	speed := ct.Properties.MustFloat64("Speed", -1)
	return speed < common.SpeedOfDrivingAutobahn
}
