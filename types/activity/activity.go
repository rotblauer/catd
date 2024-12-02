package activity

import (
	"regexp"
)

type Activity int

const (
	TrackerStateUnknown Activity = iota
	TrackerStateStationary
	TrackerStateWalking
	TrackerStateRunning
	TrackerStateCycling
	TrackerStateDriving
)

var (
	activityStationary = regexp.MustCompile(`(?i)stationary|still`)
	activityWalking    = regexp.MustCompile(`(?i)walk`)
	activityRunning    = regexp.MustCompile(`(?i)run`)
	activityCycling    = regexp.MustCompile(`(?i)cycle|bike|biking`)
	activityDriving    = regexp.MustCompile(`(?i)drive|driving|automotive`)
)

func (a Activity) IsMoving() bool {
	return a > TrackerStateStationary
}

func (a Activity) String() string {
	switch a {
	case TrackerStateUnknown:
		return "Unknown"
	case TrackerStateStationary:
		return "Stationary"
	case TrackerStateWalking:
		return "Walking"
	case TrackerStateRunning:
		return "Running"
	case TrackerStateCycling:
		return "Bike"
	case TrackerStateDriving:
		return "Automotive"
	}
	return "Unknown"
}

func IsContinuous(a, b Activity) bool {
	if a == TrackerStateUnknown || b == TrackerStateUnknown {
		return true
	}
	if a == TrackerStateStationary && b >= TrackerStateWalking {
		return false
	}
	if a >= TrackerStateWalking && b == TrackerStateStationary {
		return false
	}
	return true
}

func FromAny(a any) Activity {
	if a == nil {
		return TrackerStateUnknown
	}
	reportStr, ok := a.(string)
	if !ok {
		return TrackerStateUnknown
	}
	return FromString(reportStr)
}

func FromString(str string) Activity {
	switch {
	case activityStationary.MatchString(str):
		return TrackerStateStationary
	case activityWalking.MatchString(str):
		return TrackerStateWalking
	case activityRunning.MatchString(str):
		return TrackerStateRunning
	case activityCycling.MatchString(str):
		return TrackerStateCycling
	case activityDriving.MatchString(str):
		return TrackerStateDriving
	}
	return TrackerStateUnknown
}
