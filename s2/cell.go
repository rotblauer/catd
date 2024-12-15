package s2

import (
	"github.com/golang/geo/s2"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/reducer"
	"github.com/rotblauer/catd/types/cattrack"
)

/*
	cellID := s2.CellIDForTrackLevel(ct, level)
	return cellID.ToToken()
*/

func CatKeyFnFn(bucket reducer.Bucket) reducer.CatKeyFn {
	return func(track cattrack.CatTrack) string {
		level := CellLevel(bucket)
		return CellIDForTrackLevel(track, level).ToToken()
	}
}

// CellIDWithLevel returns the cellID truncated to the given level.
// https://docs.s2cell.aliddell.com/en/stable/s2_concepts.html#truncation
func CellIDWithLevel(cellID s2.CellID, level CellLevel) s2.CellID {
	var lsb uint64 = 1 << (2 * (30 - level))
	truncatedCellID := (uint64(cellID) & -lsb) | lsb
	return s2.CellID(truncatedCellID)
}

// CellIDForTrackLevel returns the cellID at some level for the given track.
func CellIDForTrackLevel(ct cattrack.CatTrack, level CellLevel) s2.CellID {
	coords := ct.Geometry.(orb.Point)
	return CellIDWithLevel(s2.CellIDFromLatLng(s2.LatLngFromDegrees(coords[1], coords[0])), level)
}

func dbBucket(level CellLevel) []byte { return []byte{byte(level)} }

func CellGeometryForPointAtLevel(pt orb.Point, level CellLevel) orb.Polygon {
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
