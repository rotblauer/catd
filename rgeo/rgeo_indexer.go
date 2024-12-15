package rgeo

import (
	"fmt"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/reducer"
	"github.com/rotblauer/catd/types/cattrack"
	"github.com/sams96/rgeo"
	"log/slog"
	"regexp"
	"slices"
	"strings"
)

var DBName = "rgeo.db"

var R *rgeo.Rgeo

func stripPkg(name string) string {
	return strings.TrimPrefix(name, "github.com/sams96/rgeo.")
}

func unstripPkg(name string) string {
	return "github.com/sams96/rgeo." + name
}

var DatasetSources = map[string]func() []byte{
	//stripPkg(common.ReflectFunctionName(rgeo.Countries110)): rgeo.Countries110,
	stripPkg(common.ReflectFunctionName(rgeo.Cities10)):      rgeo.Cities10,
	stripPkg(common.ReflectFunctionName(rgeo.Countries10)):   rgeo.Countries10,
	stripPkg(common.ReflectFunctionName(rgeo.Provinces10)):   rgeo.Provinces10,
	stripPkg(common.ReflectFunctionName(rgeo.US_Counties10)): rgeo.US_Counties10,
}

var TilingZoomLevels = map[int][2]int{}
var TilingZoomLevelsRe = map[string][2]int{
	"(?i)Countries": [2]int{3, 6},
	"(?i)Provinces": [2]int{3, 8},
	"(?i)Counties":  [2]int{3, 10},
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

var DatasetKeyFns = map[string]func(loc rgeo.Location) string{
	//stripPkg(common.ReflectFunctionName(rgeo.Countries110)): func(loc rgeo.Location) string {
	//	return loc.CountryCode3
	//},
	stripPkg(common.ReflectFunctionName(rgeo.Countries10)): func(loc rgeo.Location) string {
		return loc.CountryCode3
	},
	stripPkg(common.ReflectFunctionName(rgeo.US_Counties10)): func(loc rgeo.Location) string {
		return loc.CountryCode3 + "-" + loc.ProvinceCode + "-" + loc.County
	},
	stripPkg(common.ReflectFunctionName(rgeo.Provinces10)): func(loc rgeo.Location) string {
		return loc.CountryCode3 + "-" + loc.ProvinceCode
	},
	stripPkg(common.ReflectFunctionName(rgeo.Cities10)): func(loc rgeo.Location) string {
		return loc.CountryCode3 + "-" + loc.ProvinceCode + "-" + loc.City
	},
}

var datasetNamesStableMemo = []string{}

func DatasetNamesStable() []string {
	if datasetNamesStableMemo == nil {
		return datasetNamesStableMemo
	}
	out := make([]string, len(DatasetKeyFns))
	i := 0
	for k := range DatasetKeyFns {
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

func CatKeyFnFn(bucket reducer.Bucket) func(cattrack.CatTrack) string {
	dataset := DatasetNamesStable()[bucket]
	return func(ct cattrack.CatTrack) string {
		// FIXME No need for geometry, but need dataset param.
		loc, err := R.ReverseGeocodeWithGeometry(ct.Point(), unstripPkg(dataset))
		if err != nil {
			/*
				2024/12/14 20:57:27 rgeo.DatasetNames [github.com/sams96/rgeo.Cities10 github.com/sams96/rgeo.Countries10 github.com/sams96/rgeo.Provinces10 github.com/sams96/rgeo.US_Counties10]
				2024/12/14 20:57:27 INFO Rgeo Indexing complete cat=rye elapsed=6s
				panic: no geometry found for dataset "github.com/sams96/rgeo.Cities10"
			*/

			//log.Println("rgeo.DatasetNames", R.DatasetNames())
			//panic(err)

			// So maybe the cat is not in a city.
			// Yea, definitely.
			// - Flying cat over bermuda triangle, over ocean = cat without a country.
			// - Country mouse, not city cat = no city rgeocode.
			// TODO morgen
			// - handle these no-shows better
			// - get the Location data in the cattrack on the map
			return ""
		}
		return DatasetKeyFns[dataset](loc.Location)
	}
}

func CellDataForPointAtDataset(pt orb.Point, dataset string) (map[string]any, orb.Geometry) {
	loc, err := R.ReverseGeocodeWithGeometry(pt, unstripPkg(dataset))
	if err != nil {
		return nil, nil
	}
	props := map[string]any{
		"Country":  loc.CountryLong,
		"Province": loc.Province,
		"County":   loc.County,
		"City":     loc.City,
	}
	return props, loc.Geometry
}
