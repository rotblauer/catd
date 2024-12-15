package rgeo

import (
	"github.com/sams96/rgeo"
)

var R *rgeo.Rgeo

var Indices = map[string]func(loc rgeo.Location) string{
	"rgeo.Countries110": func(loc rgeo.Location) string {
		return loc.CountryCode3
	},
	"rgeo.Countries10": func(loc rgeo.Location) string {
		return loc.CountryCode3
	},
	"rgeo.US_Counties10": func(loc rgeo.Location) string {
		return loc.CountryCode3 + "-" + loc.ProvinceCode + "-" + loc.County
	},
	"rgeo.Provinces10": func(loc rgeo.Location) string {
		return loc.CountryCode3 + "-" + loc.ProvinceCode
	},
	"rgeo.Cities10": func(loc rgeo.Location) string {
		return loc.CountryCode3 + "-" + loc.ProvinceCode + "-" + loc.City
	},
}

var IndicesStable = []string{
	"rgeo.Countries110",
	"rgeo.Countries10",
	"rgeo.US_Counties10",
	"rgeo.Provinces10",
	"rgeo.Cities10",
}
