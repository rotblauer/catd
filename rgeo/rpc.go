package rgeo

import (
	"github.com/paulmach/orb"
	rgeo2 "github.com/sams96/rgeo"
)

// GetLocationRequest should be [X,Y]::[Lng,Lat].
type GetLocationRequest Pt

type GetLocationResponse struct {
	Location rgeo2.Location
	Error    error
}

type GetGeometryRequest struct {
	Pt
	Dataset string
}

type GetGeometryResponse struct {
	Geometry orb.Geometry
	Error    error
}
