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
	//sourcePath := testdata.Path(testdata.Source_EDGE20241217)
	sourcePath := "/home/ia/tdata/edge.json.gz"
	//sourcePath := "/home/ia/tdata/20250103_500000.json.gz"

	ctx := context.Background()
	tracks, errs := testdata.ReadSourceJSONGZ[cattrack.CatTrack](ctx, sourcePath)
	err := <-errs
	if err != nil {
		t.Fatal(err)
	}
	myTracks := stream.Filter(ctx, func(ct cattrack.CatTrack) bool {
		return ct.CatID() == "rye"
	}, tracks)
	pc := NewProbableCat(params.DefaultActImproverConfig)
	//i := 0
	for ct := range myTracks {
		/*
		   1735588579
		   1735762585
		   1735765627
		*/
		/*
			rye
			1736007374 start
			1697 duration
		*/
		// rye goes shopping
		start := int64(1736007374 - 120)
		end := start + 1697 + 120
		if u := ct.MustTime().Unix(); u < start || u > end {
			continue
		}

		// ike drives spoko -> id
		//if u := ct.MustTime().Unix(); u < 1735762585-300 || u > 1735765627+300 {
		//	continue
		//}

		err := pc.Add(ct)
		if err != nil {
			t.Logf("track=%v err=%s\n", ct, err)
			t.Fatal(err)
		}

		//kalmanVReportDist := geo.Distance(pc.Pos.ProbablePt, ct.Point())
		//if i%1 == 0 {
		/*
			k.lat=%3.06f kΔ=%3.02f  k.speed=%3.02f
		*/
		t.Logf(`pos.act=%s ct.act=%s lon=%3.06f p.lon=%3.06f lat=%3.06f p.lat=%3.06f acc=%3.02f r.speed=%3.02f safe.speed=%3.02f speedRate=%3.02f calcSpeedRate=%3.02f gyroRate=%3.03f accuracyRate=%3.02f distToNap=%3.02f heading=%3.02f headingDeltaRate=%3.02f\n`,
			pc.Pos.Activity,
			ct.MustActivity(),
			ct.Point().Lon(),
			pc.Pos.ProbablePt.Lon(),
			ct.Point().Lat(),
			pc.Pos.ProbablePt.Lat(),
			pc.Pos.IReportedAccel,
			//kalmanVReportDist,
			wt(ct).UnsafeSpeed(),
			wt(ct).SafeSpeed(),
			//pc.Pos.KalmanSpeed,
			pc.Pos.speed.Snapshot().Rate()/100,
			pc.Pos.speedCalculated.Snapshot().Rate()/100,
			pc.Pos.gyroSum.Snapshot().Rate()/1000,
			pc.Pos.accuracy.Snapshot().Rate(),
			geo.Distance(pc.Pos.NapPt, ct.Point()),
			ct.Properties.MustFloat64("Heading", -1),
			pc.Pos.headingDelta.Snapshot().Rate(),
		)
		//}

	}
}
