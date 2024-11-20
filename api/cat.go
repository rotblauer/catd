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
}

// Writer returns a CatWriter for the Cat. Stateful. Blocking. Locking.
func (c Cat) Writer() (*state.CatWriter, error) {
	s := &state.Cat{CatID: c.CatID}
	return s.NewCatWriter()
}

// Reader returns a CatReader for the Cat.
func (c Cat) Reader() (*state.CatReader, error) {
	s := &state.Cat{CatID: c.CatID}
	return s.NewCatReader()
}
