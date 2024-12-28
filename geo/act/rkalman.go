package act

import (
	"fmt"
	rkalman "github.com/regnull/kalman"
	"os"
)

func NewRKalmanFilter(latitude, speed, acceleration float64) *rkalman.GeoFilter {
	// Estimate process noise.
	processNoise := &rkalman.GeoProcessNoise{
		// We assume the measurements will take place at the approximately the
		// same location, so that we can disregard the earth's curvature.
		BaseLat: latitude,
		// How much do we expect the user to move, meters per second.
		DistancePerSecond: speed,
		// How much do we expect the user's speed to change, meters per second squared.
		SpeedPerSecond: acceleration,
	}
	// Initialize Kalman filter.
	filter, err := rkalman.NewGeoFilter(processNoise)
	if err != nil {
		fmt.Printf("failed to initialize Kalman filter: %s\n", err)
		os.Exit(1)
	}
	return filter
}
