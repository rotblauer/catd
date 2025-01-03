package activity

import (
	"github.com/rotblauer/catd/common"
	"testing"
	"time"
)

func TestModeTracker_Sorted(t *testing.T) {
	acts := []Activity{
		TrackerStateUnknown,
		TrackerStateStationary,
		TrackerStateWalking,
		TrackerStateRunning,
		TrackerStateBike,
		TrackerStateAutomotive,
		TrackerStateFlying,
	}
	mt := NewModeTracker(60 * time.Second)
	start := time.Now().Add(-60 * time.Second)
	for i := 0; i < 120; i++ {
		mt.Push(acts[i%len(acts)], start.Add(time.Duration(i)*time.Second), 1)
	}
	sorted := mt.Sorted(false) // skip unknown
	if len(sorted) != len(acts) {
		t.Errorf("have %d want %d", len(sorted), len(acts))
	}

	t.Logf("sorted: %v", sorted)

	wantSum := 61.0 // 61 index slots in 60 seconds
	gotSum := 0.0
	for _, m := range sorted {
		gotSum += m.Scalar
	}
	if gotSum != wantSum {
		t.Errorf("have %f want %f", gotSum, wantSum)
	}
	if sorted[0].Activity != TrackerStateUnknown {
		t.Errorf("have %v want %v", sorted[0].Activity, TrackerStateStationary)
	}
	if sorted[len(sorted)-1].Activity != TrackerStateWalking {
		t.Errorf("have %v want %v", sorted[len(sorted)-1].Activity, TrackerStateFlying)
	}
}

func TestInferFromSpeed(t *testing.T) {
	cases := []struct {
		speed      float64
		maxMul     float64
		mustActive bool
		want       Activity
	}{
		{0, 1, false, TrackerStateStationary},
		{0, 1, true, TrackerStateWalking},
		{common.SpeedOfWalkingMin, 1, false, TrackerStateWalking},
		{common.SpeedOfWalkingSlow, 1, true, TrackerStateWalking},
		{common.SpeedOfWalkingMax, 1, false, TrackerStateWalking},
		{common.SpeedOfRunningMean, 1, true, TrackerStateRunning},
		{common.SpeedOfCyclingMean, 1, true, TrackerStateBike},
		{common.SpeedOfDrivingCityUSMean, 0.8, true, TrackerStateAutomotive},
		{common.SpeedOfDrivingHighway, 1, true, TrackerStateAutomotive},
		{common.SpeedOfDrivingAutobahn * 1.5, 1, true, TrackerStateFlying},
	}
	for i, c := range cases {
		got := InferFromSpeed(c.speed, c.maxMul, c.mustActive)
		if got != c.want {
			t.Errorf("i=%d (case: %v), have %v want %v", i, c, got, c.want)
		}
	}
	for i, c := range cases {
		got := InferSpeedFromClosest(c.speed, c.maxMul, c.mustActive)
		if got != c.want {
			t.Errorf("CLOSEST i=%d (case: %v), have %v want %v", i, c, got, c.want)
		}
	}
}
