package s2

import (
	"github.com/golang/geo/s2"
	"github.com/paulmach/orb"
)

func CellPolygonForPointAtLevel(pt orb.Point, level CellLevel) orb.Polygon {
	leaf := s2.CellIDFromLatLng(s2.LatLngFromDegrees(pt.Lat(), pt.Lon()))
	leveledCellID := CellIDWithLevel(leaf, level)

	cell := s2.CellFromCellID(leveledCellID)

	vertices := []orb.Point{}
	for i := 0; i < 4; i++ {
		vpt := cell.Vertex(i)
		//pt := cell.Edge(i) // tippe halt catch fire
		ll := s2.LatLngFromPoint(vpt)
		vertices = append(vertices, orb.Point{ll.Lng.Degrees(), ll.Lat.Degrees()})
	}

	return orb.Polygon{orb.Ring(vertices)}
}
