package rgeo

import (
	"github.com/paulmach/orb"
	rgeo2 "github.com/sams96/rgeo"
)

// Pt is [Lng,Lat].
type Pt [2]float64

func Pt2Point(pt Pt) orb.Point {
	return orb.Point{pt[0], pt[1]}
}

func Point2Pt(pt orb.Point) Pt {
	return Pt{pt.Lon(), pt.Lat()}
}

func (pt Pt) Point() orb.Point {
	return Pt2Point(pt)
}

// Plat is orb.Polygon(s) is one or more []orb.Rings, outlining countries, provinces, etc.
// Also: orb.Ring is orb.LineString.
type Plat struct {
	Polygon      [][][2]float64
	MultiPolygon [][][][2]float64
}

type Polygon [][][2]float64
type MultiPolygon [][][][2]float64

func Plat2Geom(plat *Plat) orb.Geometry {
	if plat.Polygon != nil {
		poly := make(orb.Polygon, len(plat.Polygon))
		for i, ring := range plat.Polygon {
			poly[i] = make(orb.Ring, len(ring))
			for j, pt := range ring {
				poly[i][j] = orb.Point{pt[0], pt[1]}
			}
		}
		return poly
	}
	if plat.MultiPolygon != nil {
		multiPoly := make(orb.MultiPolygon, len(plat.MultiPolygon))
		for i, poly := range plat.MultiPolygon {
			multiPoly[i] = make(orb.Polygon, len(poly))
			for j, ring := range poly {
				multiPoly[i][j] = make(orb.Ring, len(ring))
				for k, pt := range ring {
					multiPoly[i][j][k] = orb.Point{pt[0], pt[1]}
				}
			}
		}
		return multiPoly
	}
	return nil
}

func Geometry2Plat(poly orb.Geometry) *Plat {
	switch poly := poly.(type) {
	case orb.Polygon:
		out := &Plat{Polygon: make([][][2]float64, len(poly))}
		for i, ring := range poly {
			out.Polygon[i] = make([][2]float64, len(ring))
			for j, pt := range ring {
				out.Polygon[i][j] = [2]float64{pt[0], pt[1]}
			}
		}
		return out
	case orb.MultiPolygon:
		out := &Plat{MultiPolygon: make([][][][2]float64, len(poly))}
		for i, poly := range poly {
			out.MultiPolygon[i] = make([][][2]float64, len(poly))
			for j, ring := range poly {
				out.MultiPolygon[i][j] = make([][2]float64, len(ring))
				for k, pt := range ring {
					out.MultiPolygon[i][j][k] = [2]float64{pt[0], pt[1]}
				}
			}
		}
		return out
	}
	return nil
}

// GetLocationRequest should be [Lng,Lat].
type GetLocationRequest Pt

type GetLocationResponse struct {
	Location rgeo2.Location
	Error    string
}

type GetGeometryRequest struct {
	Pt
	Dataset string
}

type GetGeometryResponse struct {
	Plat  *Plat
	Error string
}
