package params

import "time"

type Config struct {
	TrackCleaningConfig
	TripDetectorConfig
	SimplificationConfig
}

type TrackCleaningConfig struct {
	// AccuracyThreshold is the threshold to determine if a point is accurate.
	// If the accuracy is greater than this value, it's considered inaccurate.
	AccuracyThreshold float64

	// WangUrbanCanyonDistance is the distance threshold to determine if a point is in an urban canyon.
	// It is derived as the distance between the target point and the centroid of the 5 points before and after it.
	WangUrbanCanyonDistance float64

	// WangUrbanCanyonWindow is the window of points to consider for the Wang Urban Canyon test.
	WangUrbanCanyonWindow time.Duration

	// TeleportSpeedFactor is the factor to determine teleportation.
	// If calculated speed is X times faster than reported speed, it's a teleportation.
	TeleportSpeedFactor float64

	// TeleportMinDistance is the minimum distance between two points to consider teleportation.
	// This helps remove spurious teleportations for small distances (e.g. speed=0.04, distance=10).
	TeleportMinDistance float64

	// Teleportations must happen within this window of time.
	// Otherwise, it'll be considered signal loss instead.
	TeleportWindow time.Duration
}

var DefaultCleanConfig = &TrackCleaningConfig{
	AccuracyThreshold:       100.0,
	WangUrbanCanyonDistance: 200.0,
	WangUrbanCanyonWindow:   60 * time.Second,
	TeleportSpeedFactor:     10.0,
	TeleportWindow:          60 * time.Second,
	TeleportMinDistance:     25.0,
}

type TripDetectorConfig struct {
	DwellInterval  time.Duration
	DwellDistance  float64
	SpeedThreshold float64
}

var DefaultTripDetectorConfig = &TripDetectorConfig{
	DwellInterval:  2 * time.Minute, // TODO Separate into lap threshold vs. td window.
	DwellDistance:  50,
	SpeedThreshold: 0.5,
}

type SimplificationConfig struct {
	DouglasPeuckerThreshold float64
}

var DefaultSimplifierConfig = &SimplificationConfig{
	DouglasPeuckerThreshold: 0.00008,
}

type NapConfig struct {
	DwellInterval time.Duration
	DwellDistance float64
}

var DefaultNapConfig = &NapConfig{
	// DwellInterval separates naps by time.
	DwellInterval: 24 * time.Hour,

	// DwellDistance separates naps by distance.
	DwellDistance: 100,
}
