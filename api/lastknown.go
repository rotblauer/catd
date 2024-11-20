package api

import (
	"github.com/rotblauer/catd/types/cattrack"
)

func (c *Cat) LastKnown() (*cattrack.CatTrack, error) {
	if c.State == nil {
		_, err := c.WithState(true)
		if err != nil {
			return nil, err
		}
	}
	return c.State.ReadLastTrack()
}
