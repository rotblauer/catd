package api

import (
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/state"
	"github.com/rotblauer/catd/types/cattrack"
)

func LastKnown(catID conceptual.CatID) (*cattrack.CatTrack, error) {
	catApp := state.Cat{CatID: catID}
	reader, err := catApp.NewCatReader()
	if err != nil {
		return nil, err
	}
	return reader.ReadLastTrack()
}
