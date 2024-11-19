package cattrack

import (
	"github.com/montanaflynn/stats"
	"github.com/paulmach/orb"
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
	if len(tracks) == 0 {
		return nil
	}

	f := geojson.NewFeature(orb.Point{})

	f.Properties["Name"] = tracks[0].Properties.MustString("Name")
	f.Properties["UUID"] = tracks[0].Properties.MustString("UUID")

	f.Properties["Time"] = map[string]any{
		"Start": map[string]any{
			"Unix":    tracks[0].MustTime().Unix(),
			"RFC3339": tracks[0].MustTime().Format(time.RFC3339),
		},
		"End": map[string]any{
			"Unix":    tracks[len(tracks)-1].MustTime().Unix(),
			"RFC3339": tracks[len(tracks)-1].MustTime().Format(time.RFC3339),
		},
		"Duration": tracks[len(tracks)-1].MustTime().Sub(tracks[0].MustTime()).Round(time.Second).Seconds(),
	}

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
		f.Properties[key] = map[string]float64{
			"Mean":   common.DecimalToFixed(statsMustFloat(statsData.Mean), precision),
			"Median": common.DecimalToFixed(statsMustFloat(statsData.Median), precision),
			"Min":    common.DecimalToFixed(statsMustFloat(statsData.Min), precision),
			"Max":    common.DecimalToFixed(statsMustFloat(statsData.Max), precision),
		}
	}

	installStats("Accuracy", accuracies, 0)
	installStats("Elevation", elevations, 0)

	centroid, area := planar.CentroidArea(multipoint)
	f.Properties["Area"] = common.DecimalToFixed(area, 0)
	f.Geometry = centroid

	return (*CatNap)(f)
}
