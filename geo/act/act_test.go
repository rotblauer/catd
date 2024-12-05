package act

import (
	"encoding/json"
	"errors"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"testing"
)

func TestAccelerationStuff(t *testing.T) {

	im := NewImprover()

	testdataPathGZ := "../../testing/testdata/private/2024-09-0_rye.json.gz"
	gzftw, err := flat.NewFlatGZReader(testdataPathGZ)
	if err != nil {
		t.Fatal(err)
	}
	defer gzftw.Close()

	dec := json.NewDecoder(gzftw.Reader())
	for {
		ct := cattrack.CatTrack{}
		err := dec.Decode(&ct)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatal(err)
		}
		if err := im.Improve(ct); err != nil {
			t.Fatal(err)
		}
		if im.Cat.ActivityState.IsActive() {
			t.Log(im.Cat.ActivityState,
				"acc", im.Cat.WindowAccelerationReportedSum/im.Cat.WindowSpan.Seconds(),
				"speed", im.Cat.WindowSpeedReportedSum/im.Cat.WindowSpan.Seconds(),
			)
		}
	}
}
