package act

import (
	"context"
	"github.com/rotblauer/catd/testing/testdata"
	"github.com/rotblauer/catd/types/cattrack"
	"testing"
)

func TestAccelerationStuff(t *testing.T) {
	im := NewImprover()
	ctx := context.Background()
	tracks, errs := testdata.ReadSourceJSONGZ[cattrack.CatTrack](ctx, testdata.Path(testdata.Source_EDGE1000))
	err := <-errs
	if err != nil {
		t.Fatal(err)
	}
	for ct := range tracks {
		if err := im.Improve(ct); err != nil {
			t.Fatal(err)
		}
		if im.Cat.ActivityState.IsActive() {
			//t.Log(im.Cat.ActivityState,
			//	"acc", im.Cat.WindowAccelerationReportedSum/im.Cat.WindowSpan.Seconds(),
			//	"speed", im.Cat.WindowSpeedReportedSum/im.Cat.WindowSpan.Seconds(),
			//)
		}
	}
}
