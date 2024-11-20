package api

import (
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/state"
)

// Cat is the API representation of a cat.
// It does not reflect cat state. (Well, it can _reflect_ it, but not ~be~ it).
// It CAN reflect values about some (assumed, or inferred) cat,
// where data for the cat can come from some context, like
// a token (permissions), a CLI-flag, a URL parameter, or even
// an environment value. Anywhere cat data comes from, that is not the state of this app.
type Cat struct {
	CatID conceptual.CatID

	// Ok, actually we DO have to have/want a conn to state.
	// An API function might use another API function,
	// and they might want to share a state conn.
	State *state.CatState
}

// WithState returns a CatState for the Cat.
// If readOnly is false it will block.
func (c *Cat) WithState(readOnly bool) (*state.CatState, error) {
	s := &state.Cat{CatID: c.CatID}
	st, err := s.NewCatState(readOnly)
	if err != nil {
		return nil, err
	}
	c.State = st
	return c.State, nil
}
