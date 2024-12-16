package rgeo

import (
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/reducer"
	"github.com/rotblauer/catd/types/cattrack"
	"github.com/sams96/rgeo"
	"regexp"
)

var TilingZoomLevels = map[int][2]int{}
var TilingZoomLevelsRe = map[string][2]int{
	"(?i)Countries": [2]int{1, 5},
	"(?i)Provinces": [2]int{1, 6},
	"(?i)Counties":  [2]int{3, 8},
	"(?i)Cities":    [2]int{3, 10},
}

func initTilingZoomLevels() {
	for i, datasetName := range DatasetNamesStable {
		for re, zoomLevels := range TilingZoomLevelsRe {
			if regexp.MustCompile(re).MatchString(datasetName) {
				TilingZoomLevels[i] = zoomLevels
			}
		}
	}
}

func getStableIndexForDataset(name string) int {
	for i, v := range DatasetNamesStable {
		if v == name {
			return i
		}
	}
	return -1
}

var DefaultIndexerT = &cattrack.StackerV1{
	VisitThreshold: params.RgeoDefaultVisitThreshold,
}

// TODO Meta cache me. Another shape index?
func CatKeyFn(ct cattrack.CatTrack, bucket reducer.Bucket) (string, error) {
	dataset := DatasetNamesStable[bucket]

	loc, err := R(dataset).GetLocation(ct.Point())
	if err != nil {
		// - Flying cat over bermuda triangle, over ocean = cat without a country, intl waters.
		// - Country mouse, not city cat = no city rgeocode.
		return "", reducer.ErrNoKeyFound
	}
	return getReducerKey(loc, dataset)
}

func TransformCatTrackFn(bucket int) func(ct cattrack.CatTrack) cattrack.CatTrack {
	return func(ct cattrack.CatTrack) cattrack.CatTrack {
		cp := ct
		cp.ID = cp.MustTime().Unix()
		dataset := DatasetNamesStable[bucket]
		cp.Geometry, _ = R(dataset).GetGeometry(cp.Point(), dataset)
		loc, _ := R(dataset).GetLocation(cp.Point())
		key, _ := getReducerKey(loc, dataset)
		props := map[string]any{
			"reducer_key":  key,
			"Country":      loc.CountryLong,
			"CountryCode3": loc.CountryCode3,
			"Province":     loc.Province,
			"County":       loc.County,
			"City":         loc.City,
		}
		cp.SetPropertiesSafe(props)
		return cp
	}
}

func CellDataForPointAtDataset(pt orb.Point, dataset string) (map[string]any, orb.Geometry) {
	loc, err := R(dataset).GetLocation(pt)
	if err != nil {
		return nil, nil
	}
	key, _ := getReducerKey(loc, dataset)
	props := map[string]any{
		"reducer_key":  key,
		"Country":      loc.CountryLong,
		"CountryCode3": loc.CountryCode3,
		"Province":     loc.Province,
		"County":       loc.County,
		"City":         loc.City,
	}
	g, err := R(dataset).GetGeometry(pt, dataset)
	if err != nil {
		return props, nil
	}
	return props, g
}

func getReducerKey(location rgeo.Location, dataset string) (string, error) {
	switch dataset {
	case dataSourcePre + "Countries10":
		v := location.CountryCode3
		if location.CountryCode3 == "" {
			return "", reducer.ErrNoKeyFound
		}
		return v, nil
	case dataSourcePre + "Provinces10":
		v := location.CountryCode3 + "-" + location.ProvinceCode
		if location.CountryCode3 == "" || location.ProvinceCode == "" {
			return "", reducer.ErrNoKeyFound
		}
		return v, nil
	case dataSourcePre + "US_Counties10":
		v := location.CountryCode3 + "-" + location.ProvinceCode + "-" + location.County
		if location.CountryCode3 == "" || location.ProvinceCode == "" || location.County == "" {
			return "", reducer.ErrNoKeyFound
		}
		if location.CountryCode3 != "USA" {
			// US Counties can only be in the US.
			/*
				2024/12/15 12:56:47 ERROR Failed to get geometry for track cat=jr track="{ID:1486412613 Type:Feature BBox:[] Geometry:[13.4454 52.48462] Properties:map[Accuracy:65 Activity:Stationary ActivityMode:Stationary ActivityMode.Automotive:0 ActivityMode.Bike:0 ActivityMode.Fly:0 ActivityMode.Running:0 ActivityMode.Stationary:288904 ActivityMode.Unknown:0 ActivityMode.Walking:6036 Alias:jr Count:2378 Elevation:35.46441 FirstTime:2017-01-20T09:35:50-07:00 Heading:-1 LastTime:2017-02-06T13:23:33-07:00 Name:Big Mamma Speed:-1 Time:2017-02-06T20:23:33.348Z TimeOffset:1 TotalTimeOffset:294940 UUID: UnixTime:1.486412613e+09 Version: VisitCount:7 catdReceivedAt:1.734292584e+09 reducer_key:DEU-DE-BE-]}" bucket=3 name=github.com/sams96/rgeo.US_Counties10
			*/
			return "", reducer.ErrNoKeyFound
		}
		return v, nil
	case dataSourcePre + "Cities10":
		v := location.CountryCode3 + "-" + location.ProvinceCode + "-" + location.City
		if location.CountryCode3 == "" || location.ProvinceCode == "" || location.City == "" {
			return "", reducer.ErrNoKeyFound
		}
		return v, nil
	default:
		panic("unhandled dataset")
	}
}

/*
BUG REPORT: Mismatched reducer keys.
See the second track below with reducer_key "USA-US-MN-Minneapolis", but obviously in Missoula.
This was resolved to be an issue with rgeo. The s2.ContainingShapesQuery backing it is not
threadsafe, and returns bad answers when called concurrently. This was fixed by
locking around the call in rotblauer's fork.

cattracks-ia/Cities10_cells/Cities10_cells
2 features

  {
  "Accuracy": 2.4000000953674316,
  "Activity": "Automotive",
  "ActivityMode.Automotive": 2274,
  "ActivityMode.Bike": 6784,
  "ActivityMode.Fly": 0,
  "ActivityMode.Running": 5467,
  "ActivityMode.Stationary": 738057,
  "ActivityMode.Unknown": 0,
  "ActivityMode.Walking": 10237,
  "Alias": "ia",
  "City": "Missoula",
  "Count": 85380,
  "Country": "United States of America",
  "County": "Missoula",
  "Elevation": 972.5,
  "FirstTime": "2024-12-02T13:40:06-07:00",
  "LastTime": "2024-12-11T10:18:37-07:00",
  "Name": "ranga-moto-act3",
  "Province": "Montana",
  "Speed": 24.020000457763672,
  "Time": "2024-12-11T17:18:37.927Z",
  "TotalTimeOffset": 762819,
  "UUID": "76170e959f967f40",
  "VisitCount": 1,
  "reducer_key": "USA-US-MT-Missoula",
  "id": 1733937517
}
{
  "Accuracy": 4.199999809265137,
  "Activity": "Running",
  "ActivityMode.Automotive": 2,
  "ActivityMode.Bike": 0,
  "ActivityMode.Fly": 0,
  "ActivityMode.Running": 1,
  "ActivityMode.Stationary": 0,
  "ActivityMode.Unknown": 0,
  "ActivityMode.Walking": 5,
  "Alias": "ia",
  "City": "Missoula",
  "Count": 6,
  "Country": "United States of America",
  "County": "Missoula",
  "Elevation": 961.7999877929688,
  "FirstTime": "2024-12-01T07:10:32-07:00",
  "LastTime": "2024-12-06T13:29:39-07:00",
  "Name": "ranga-moto-act3",
  "Province": "Montana",
  "Speed": 3.2200000286102295,
  "Time": "2024-12-06T20:29:39.096Z",
  "TotalTimeOffset": 8,
  "UUID": "76170e959f967f40",
  "VisitCount": 3,
  "reducer_key": "USA-US-MN-Minneapolis",
  "id": 1733516979
}
*/
