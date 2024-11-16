package params

import "time"

type Config struct {
	CleanConfig
	TripDetectorConfig
	SimplificationConfig
}

type CleanConfig struct {
	WangUrbanCanyonDistance float64
	TeleportFactor          float64
	TeleportInterval        time.Duration
}

var DefaultCleanConfig = CleanConfig{
	WangUrbanCanyonDistance: 200,
	TeleportInterval:        60 * time.Second,
	TeleportFactor:          10,
}

type TripDetectorConfig struct {
	DwellInterval  time.Duration
	DwellDistance  float64
	SpeedThreshold float64
}

var DefaultTripDetectorConfig = TripDetectorConfig{
	DwellInterval:  2 * time.Minute,
	DwellDistance:  50,
	SpeedThreshold: 0.5,
}

type SimplificationConfig struct {
	DouglasPeuckerThreshold float64
}

var DefaultSimplificationConfig = SimplificationConfig{
	DouglasPeuckerThreshold: 0.00008,
}
