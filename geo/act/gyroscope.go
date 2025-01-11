package act

import (
	"github.com/rotblauer/catd/types/cattrack"
	"math"
	"time"
)

var GyroscopeStableThresholdTime = 30 * time.Second

// isGyroscopicallyStable returns true if the feature is considered stable by the gyroscope.
// Valid is returned true only if all gyroscope attributes exist on the feature.
// Only gcps (the Android cat tracker) will have gyroscope readings.
func isGyroscopicallyStable(ct *cattrack.CatTrack) (stable, valid bool) {
	if !ct.IsGyroOK() {
		return false, false
	}
	sum := 0.0
	for _, prop := range cattrack.GyroscopeProps {
		sum += math.Abs(ct.Properties[prop].(float64))
	}
	return sum < cattrack.GyroscopeStableThresholdReading, true
}

func gyroSum(ct *cattrack.CatTrack) (sum float64, ok bool) {
	if !ct.IsGyroOK() {
		return 0, false
	}
	for _, prop := range cattrack.GyroscopeProps {
		sum += math.Abs(ct.Properties[prop].(float64))
	}
	return sum, true
}
