package cleaner

import (
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
)

func FilterAccuracy(ct *cattrack.CatTrack) bool {
	accuracy := ct.Properties.MustFloat64("Accuracy")
	return accuracy > 0 && accuracy < params.DefaultCleanConfig.AccuracyThreshold
}

func FilterSpeed(ct *cattrack.CatTrack) bool {
	return ct.Properties.MustFloat64("Speed") < common.SpeedOfSound
}

func FilterElevation(ct *cattrack.CatTrack) bool {
	elevation := ct.Properties.MustFloat64("Elevation")
	deepestDive := -100.0
	return elevation > common.ElevationOfDeadSea-deepestDive &&
		elevation < common.ElevationOfTroposphere
}
