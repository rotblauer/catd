package act

import (
	"context"
	"github.com/paulmach/orb/geo"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
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

func TestProbableCat_Add(t *testing.T) {
	ctx := context.Background()
	tracks, errs := testdata.ReadSourceJSONGZ[cattrack.CatTrack](ctx, testdata.Path(testdata.Source_EDGE20241217))
	err := <-errs
	if err != nil {
		t.Fatal(err)
	}
	iaTracks := stream.Filter(ctx, func(ct cattrack.CatTrack) bool {
		return ct.CatID() == "ia"
	}, tracks)
	pc := NewProbableCat(params.DefaultActImproverConfig)
	for ct := range iaTracks {
		pc.Add(ct)

		kalmanVReportDist := geo.Distance(pc.Pos.KalmanPt, ct.Point())

		t.Logf(`act=%s lon=%3.06f k.lon=%3.06f lat=%3.06f k.lat=%3.06f Δ=%3.02f speed=%3.06f k.speed=%3.02f ewmaInterval.speed=%3.02f accuracyRate=%3.02f distToNap=%3.02f\n`,
			pc.Pos.Activity,
			ct.Point().Lon(),
			pc.Pos.KalmanPt.Lon(),
			ct.Point().Lat(),
			pc.Pos.KalmanPt.Lat(),
			kalmanVReportDist,
			wt(ct).Speed(),
			pc.Pos.KalmanSpeed,
			pc.Pos.SpeedRate,
			pc.Pos.accuracyRate,
			geo.Distance(pc.Pos.NapPt, ct.Point()),
		)
	}
}
