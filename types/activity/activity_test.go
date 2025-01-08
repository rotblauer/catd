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
		t.Errorf("have %d wantMax %d", len(sorted), len(acts))
	}

	t.Logf("sorted: %v", sorted)

	wantSum := 61.0 // 61 index slots in 60 seconds
	gotSum := 0.0
	for _, m := range sorted {
		gotSum += m.Scalar
	}
	if gotSum != wantSum {
		t.Errorf("have %f wantMax %f", gotSum, wantSum)
	}
	if sorted[0].Activity != TrackerStateUnknown {
		t.Errorf("have %v wantMax %v", sorted[0].Activity, TrackerStateStationary)
	}
	if sorted[len(sorted)-1].Activity != TrackerStateWalking {
		t.Errorf("have %v wantMax %v", sorted[len(sorted)-1].Activity, TrackerStateFlying)
	}
}

func TestInferFromSpeed(t *testing.T) {
	cases := []struct {
		speed       float64
		maxMul      float64
		mustActive  bool
		wantMax     Activity
		wantClosest Activity
	}{
		{0, 1, false, TrackerStateStationary, TrackerStateStationary},
		{0, 1, true, TrackerStateWalking, TrackerStateWalking},
		{common.SpeedOfWalkingMin, 1, false, TrackerStateWalking, TrackerStateStationary},
		{common.SpeedOfWalkingSlow, 1, true, TrackerStateWalking, TrackerStateWalking},
		{common.SpeedOfWalkingMax, 1, false, TrackerStateWalking, TrackerStateWalking},
		{common.SpeedOfRunningMean, 1, true, TrackerStateRunning, TrackerStateRunning},
		{common.SpeedOfCyclingMean, 1, true, TrackerStateBike, TrackerStateBike},
		{common.SpeedOfDrivingCityUSMean, 0.8, true, TrackerStateAutomotive, TrackerStateAutomotive},
		{common.SpeedOfDrivingHighway, 1, true, TrackerStateAutomotive, TrackerStateAutomotive},
		{common.SpeedOfDrivingAutobahn * 1.5, 1, true, TrackerStateFlying, TrackerStateFlying},
	}
	for i, c := range cases {
		got := InferFromSpeedMax(c.speed, c.maxMul, c.mustActive)
		if got != c.wantMax {
			t.Errorf("i=%d (case: %v), have %v wantMax %v", i, c, got, c.wantMax)
		}
	}
	for i, c := range cases {
		got := InferSpeedFromClosest(c.speed, c.mustActive)
		if got != c.wantClosest {
			t.Errorf("CLOSEST i=%d (case: %v), have %v wantClosest %v", i, c, got, c.wantClosest)
		}
	}
}
