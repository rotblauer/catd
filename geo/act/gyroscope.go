package act

import (
	"github.com/rotblauer/catd/types/cattrack"
	"math"
	"time"
)

var gyroscopeProps = []string{"GyroscopeX", "GyroscopeY", "GyroscopeZ"}
var GyroscopeStableThresholdReading = 0.01
var GyroscopeStableThresholdTime = 30 * time.Second

// isGyroscopicallyStable returns true if the feature is considered stable by the gyroscope.
// Valid is returned true only if all gyroscope attributes exist on the feature.
// Only gcps (the Android cat tracker) will have gyroscope readings.
func isGyroscopicallyStable(ct *cattrack.CatTrack) (stable, valid bool) {
	sum := 0.0
	for _, prop := range gyroscopeProps {
		v, ok := ct.Properties[prop]
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
