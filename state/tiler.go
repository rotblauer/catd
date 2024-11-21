package state

import (
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/params"
	"io"
	"path/filepath"
)

// TilerState has field and methods for dealing with tiler-related state on disk.
// Does the tiler really need state? We can stream tracks to the tiler.
// BUT -- what if the Tiler doesn't want to, or can, or wants to wait (debounce)
// the tracks before processing them? Then it needs state.
// It needs to be able to manage its own, persistent, data.
// Sometimes this data will be for cats, sometimes for the global app.
type TilerState struct {
	Flat *flat.Flat
}

func NewTilerState() (*TilerState, error) {
	return &TilerState{
		Flat: flat.NewFlatWithRoot(params.DatadirRoot),
	}, nil
}

// OpenReaderGZ opens a gzip file for reading, in a cat-agnostic way.
func (t *TilerState) OpenReaderGZ(path string) (io.ReadCloser, error) {
	f, err := flat.NewFlatGZReader(filepath.Join(t.Flat.Path(), path))
	if err != nil {
		return nil, err
	}
	return f.Reader(), nil
}
