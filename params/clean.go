package params

import "time"

type TrackCleaningConfig struct {
	// AccuracyThreshold is the threshold to determine if a point is accurate.
	// If the accuracy is greater than this value, it's considered inaccurate.
	AccuracyThreshold float64

	// WangUrbanCanyonMinDistance is the minimum distance threshold to determine if a point is in an urban canyon.
	// It is derived as the distance between the target point and the centroids of the 5 points before and after it.
	WangUrbanCanyonMinDistance float64

	// WangUrbanCanyonDistanceFromSpeedMul is the multiplier to determine the distance threshold for the Wang Urban Canyon test
	// using the speed of the target point.
	// Low speeds require a lower distance threshold.
	// Minimum distance is bounded by WangUrbanCanyonMinDistance.
	WangUrbanCanyonDistanceFromSpeedMul float64

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
	AccuracyThreshold:                   100.0,
	WangUrbanCanyonMinDistance:          200.0,
	WangUrbanCanyonDistanceFromSpeedMul: 10.0,
	WangUrbanCanyonWindow:               60 * time.Second,
	TeleportSpeedFactor:                 10.0,
	TeleportWindow:                      60 * time.Second,
	TeleportMinDistance:                 25.0,
}
