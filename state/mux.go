package state

import "github.com/rotblauer/catd/types/cattrack"

// Mux is app state.
type Mux struct{}

func (m *Mux) Populate(in <-chan *cattrack.CatTrack) {

}
