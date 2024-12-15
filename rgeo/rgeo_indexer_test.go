package rgeo

import "testing"

func TestDatasetNamesStable(t *testing.T) {
	names := DatasetNamesStable()
	if len(names) != 4 {
		t.Errorf("Expected 4 names, got %d", len(names))
	}
	if names[0] != "Countries10" {
		t.Errorf("Expected Countries10, got %s", names[0])
	}
	if names[1] != "Provinces10" {
		t.Errorf("Expected Provinces10, got %s", names[1])
	}
	if names[2] != "US_Counties10" {
		t.Errorf("Expected US_Counties10, got %s", names[2])
	}
	if names[3] != "Cities10" {
		t.Errorf("Expected Cities10, got %s", names[3])
	}
}
