package api

import "github.com/paulmach/orb/geojson"

type LastKnownQuery struct {
}

func LastKnown(q LastKnownQuery) []*geojson.Feature {
	return nil
}
