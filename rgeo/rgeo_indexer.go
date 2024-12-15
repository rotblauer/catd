package rgeo

import (
	"fmt"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/reducer"
	"github.com/rotblauer/catd/types/cattrack"
	"github.com/sams96/rgeo"
	"log/slog"
	"regexp"
	"slices"
)

var DBName = "rgeo.db"

var R *rgeo.Rgeo

const dataSourcePre = "github.com/sams96/rgeo."

var DatasetSources = map[string]func() []byte{
	//stripPkg(common.ReflectFunctionName(rgeo.Countries110)): rgeo.Countries110,
	dataSourcePre + "Cities10":      rgeo.Cities10,
	dataSourcePre + "Countries10":   rgeo.Countries10,
	dataSourcePre + "Provinces10":   rgeo.Provinces10,
	dataSourcePre + "US_Counties10": rgeo.US_Counties10,
}

var TilingZoomLevels = map[int][2]int{}
var TilingZoomLevelsRe = map[string][2]int{
	"(?i)Countries": [2]int{3, 5},
	"(?i)Provinces": [2]int{3, 6},
	"(?i)Counties":  [2]int{3, 8},
	"(?i)Cities":    [2]int{3, 10},
}

func init() {
	for i, v := range DatasetNamesStable() {
		for re, zooms := range TilingZoomLevelsRe {
			if regexp.MustCompile(re).MatchString(v) {
				TilingZoomLevels[i] = zooms
			}
		}
	}
}

var ErrAlreadyInitialized = fmt.Errorf("rgeo already initialized")

func DoInit() error {
	if R != nil {
		return ErrAlreadyInitialized
	}
	var err error
	sources := []func() []byte{}
	for _, v := range DatasetSources {
		sources = append(sources, v)
	}
	slog.Info("Initializing rgeo", "datasets", len(sources), "names", DatasetNamesStable())
	defer slog.Info("Initialized rgeo", "datasets", len(sources))
	R, err = rgeo.New(sources...)
	if err != nil {
		return err
	}
	return nil
}

var datasetNamesStableMemo = []string{}

func DatasetNamesStable() []string {
	if datasetNamesStableMemo == nil {
		return datasetNamesStableMemo
	}
	out := make([]string, 4)
	i := 0
	for k := range DatasetSources {
		out[i] = k
		i++
	}
	slices.Sort(out)
	datasetNamesStableMemo = out
	return datasetNamesStableMemo
}

func getIndexForStableName(name string) int {
	for i, v := range DatasetNamesStable() {
		if v == name {
			return i
		}
	}
	return -1
}

var DefaultIndexerT = &cattrack.StackerV1{
	VisitThreshold: params.S2DefaultVisitThreshold,
}

func CatKeyFn(ct cattrack.CatTrack, bucket reducer.Bucket) (string, error) {
	dataset := DatasetNamesStable()[bucket]
	loc, err := R.ReverseGeocode(ct.Point())
	if err != nil {
		// - Flying cat over bermuda triangle, over ocean = cat without a country, intl waters.
		// - Country mouse, not city cat = no city rgeocode.
		return "", reducer.ErrNoKeyFound
	}
	/*
		var TilingZoomLevelsRe = map[string][2]int{
			"(?i)Countries": [2]int{3, 5},
			"(?i)Provinces": [2]int{3, 6},
			"(?i)Counties":  [2]int{3, 8},
			"(?i)Cities":    [2]int{3, 10},
		}
	*/
	switch dataset {
	case dataSourcePre + "Countries10":
		return loc.CountryCode3, nil
	case dataSourcePre + "Provinces10":
		return loc.CountryCode3 + "-" + loc.ProvinceCode, nil
	case dataSourcePre + "US_Counties10":
		return loc.CountryCode3 + "-" + loc.ProvinceCode + "-" + loc.County, nil
	case dataSourcePre + "Cities10":
		return loc.CountryCode3 + "-" + loc.ProvinceCode + "-" + loc.City, nil
	default:
		panic("unhandled dataset")
	}
	return "", reducer.ErrNoKeyFound
}

func CellDataForPointAtDataset(pt orb.Point, dataset string) (map[string]any, orb.Geometry) {
	loc, err := R.ReverseGeocodeWithGeometry(pt, dataset)
	if err != nil {
		return nil, nil
	}
	props := map[string]any{
		"Country":      loc.CountryLong,
		"CountryCode3": loc.CountryCode3,
		"Province":     loc.Province,
		"County":       loc.County,
		"City":         loc.City,
	}
	return props, loc.Geometry
}

/*
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
