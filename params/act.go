package params

import (
	"github.com/rotblauer/catd/common"
	"time"
)

// NapCentroidMaxPoints is when to stop calculating the centroid from all the slice tracks.
// FIXME: This is performance optimization.
var NapCentroidMaxPoints = int((10 * time.Minute).Seconds())

// ActDiscretionConfig is a generic type for configuring
// the discretion of cat acts.
type ActDiscretionConfig struct {
	// Interval separates acts by time.
	Interval time.Duration

	// ResetInterval is the time to reset the act inference machine.
	ResetInterval time.Duration

	// Distance separates acts by distance.
	Distance float64

	// SpeedThreshold separates acts by speed.
	// Generally, speeds lower than this are Stationary, and higher are Moving.
	SpeedThreshold float64

	// SplitActivities is a flag to split acts on activities.
	SplitActivities bool
}

var DefaultActImproverConfig = &ActDiscretionConfig{
	Interval:       20 * time.Second,
	ResetInterval:  time.Minute,
	SpeedThreshold: common.SpeedOfWalkingMin,
}

var DefaultLapConfig = &ActDiscretionConfig{
	Interval:       2 * time.Minute,
	Distance:       50.0,
	SpeedThreshold: common.SpeedOfWalkingSlow,
}

var DefaultNapConfig = &ActDiscretionConfig{
	Interval: 24 * time.Hour,
	Distance: 100.0,
}

type LineStringSimplificationConfig struct {
	DouglasPeuckerThreshold float64
}

var DefaultLineStringSimplificationConfig = &LineStringSimplificationConfig{
	DouglasPeuckerThreshold: 0.00008,
}

// DefaultActDiscretionConfigTripDetector is DEPRECATED.
var DefaultActDiscretionConfigTripDetector = &ActDiscretionConfig{
	Interval:       2 * time.Minute,
	Distance:       50,
	SpeedThreshold: 0.5,
}
