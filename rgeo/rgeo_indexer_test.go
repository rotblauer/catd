package rgeo

import "testing"

func TestDatasetNamesStable(t *testing.T) {
	names := DatasetNamesStable
	if len(names) != 4 {
		t.Errorf("Expected 4 names, got %d", len(names))
	}
	if want := "github.com/sams96/rgeo.Cities10"; names[0] != want {
		t.Errorf("Expected %q, got %s", want, names[0])
	}
	if want := "github.com/sams96/rgeo.Countries10"; names[1] != want {
		t.Errorf("Expected %q, got %s", want, names[1])
	}
	if want := "github.com/sams96/rgeo.Provinces10"; names[2] != want {
		t.Errorf("Expected %q, got %s", want, names[2])
	}
	if want := "github.com/sams96/rgeo.US_Counties10"; names[3] != want {
		t.Errorf("Expected %q, got %s", want, names[3])
	}
}
