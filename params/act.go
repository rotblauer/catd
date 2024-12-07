package params

import "time"

type ActDiscretionConfig struct {
	// DwellInterval separates acts by time.
	DwellInterval time.Duration
	// DwellDistance separates acts by distance.
	DwellDistance float64
	// SpeedThreshold separates acts by speed.
	SpeedThreshold float64
}

var DefaultActDiscretionConfig = &ActDiscretionConfig{
	DwellInterval:  2 * time.Minute, // TODO Separate into lap threshold vs. td window.
	DwellDistance:  50,
	SpeedThreshold: 0.5,
}

type LapConfig struct {
	ActDiscretionConfig
	SplitActivities bool
}

var DefaultLapConfig = &LapConfig{
	ActDiscretionConfig: ActDiscretionConfig{
		DwellInterval:  2 * time.Minute,
		DwellDistance:  50.0,
		SpeedThreshold: 0.5,
	},
	SplitActivities: false,
}

type NapConfig ActDiscretionConfig

var DefaultNapConfig = &NapConfig{
	DwellInterval: 24 * time.Hour,
	DwellDistance: 100,
}

type LineStringSimplificationConfig struct {
	DouglasPeuckerThreshold float64
}

var DefaultLineStringSimplificationConfig = &LineStringSimplificationConfig{
	DouglasPeuckerThreshold: 0.00008,
}
