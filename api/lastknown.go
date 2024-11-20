package api

import (
	"github.com/rotblauer/catd/types/cattrack"
)

// LastKnown returns the last known state of a cat.
// TODO: Implement a way to get the last/current state for some cat.
func (c *Cat) LastKnown() (*cattrack.CatTrack, error) {
	if c.State == nil {
		_, err := c.WithState(true)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}
