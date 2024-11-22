package cattrack

import (
	"github.com/montanaflynn/stats"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/orb/planar"
	"github.com/rotblauer/catd/common"
	"time"
)

type CatNap geojson.Feature

func (cn *CatNap) MarshalJSON() ([]byte, error) {
	return (*geojson.Feature)(cn).MarshalJSON()
}

func (cn *CatNap) UnmarshalJSON(data []byte) error {
	return (*geojson.Feature)(cn).UnmarshalJSON(data)
}

func (cn *CatNap) IsValid() bool {
	_, ok := cn.Geometry.(orb.Point)
	return ok
}

func NewCatNap(tracks []*CatTrack) *CatNap {
	if len(tracks) < 2 {
		return nil
	}

	f := geojson.NewFeature(orb.Point{})

	f.Properties["Name"] = tracks[0].Properties.MustString("Name")
	f.Properties["UUID"] = tracks[0].Properties.MustString("UUID")
	f.Properties["RawPointCount"] = len(tracks)

	firstTime, lastTime := tracks[0].MustTime(), tracks[len(tracks)-1].MustTime()
	f.Properties["Time_Start_Unix"] = firstTime.Unix()
	f.Properties["Time_Start_RFC339"] = firstTime.Format(time.RFC3339)
	f.Properties["Time_End_Unix"] = lastTime.Unix()
	f.Properties["Time_End_RFC339"] = lastTime.Format(time.RFC3339)
	f.Properties["Duration"] = lastTime.Sub(firstTime).Round(time.Second).Seconds()

	accuracies := make([]float64, 0, len(tracks))
	elevations := make([]float64, 0, len(tracks))

	multipoint := orb.MultiPoint{}
	for _, t := range tracks {
		multipoint = append(multipoint, t.Point())
		accuracies = append(accuracies, t.Properties.MustFloat64("Accuracy"))
		elevations = append(elevations, t.Properties.MustFloat64("Elevation"))
	}

	statsMustFloat := func(fn func() (float64, error)) float64 {
		out, _ := fn()
		return out
	}

	installStats := func(key string, data []float64, precision int) {
		statsData := stats.Float64Data(data)
		f.Properties[key+"_Mean"] = common.DecimalToFixed(statsMustFloat(statsData.Mean), precision)
		f.Properties[key+"_Median"] = common.DecimalToFixed(statsMustFloat(statsData.Median), precision)
		f.Properties[key+"_Min"] = common.DecimalToFixed(statsMustFloat(statsData.Min), precision)
		f.Properties[key+"_Max"] = common.DecimalToFixed(statsMustFloat(statsData.Max), precision)
	}

	installStats("Accuracy", accuracies, 0)
	installStats("Elevation", elevations, 0)

	area := geo.Area(multipoint.Bound())
	f.Properties["Area"] = common.DecimalToFixed(area, 0)

	centroid, _ := planar.CentroidArea(multipoint)
	f.Geometry = centroid

	return (*CatNap)(f)
}
