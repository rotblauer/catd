package app

import (
	"encoding/json"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
)

type Cat struct {
	CatID conceptual.CatID
}

type CatWriter struct {
	CatID         conceptual.CatID
	Flat          *flat.Flat
	TrackWriterGZ io.WriteCloser
}

func (c *Cat) NewCatWriter() (*CatWriter, error) {
	flatCat := flat.NewFlatWithRoot(DatadirRoot).ForCat(c.CatID)
	if err := flatCat.Ensure(); err != nil {
		return nil, err
	}
	gzFile, err := flatCat.TracksGZ()
	if err != nil {
		return nil, err
	}
	return &CatWriter{
		CatID:         c.CatID,
		Flat:          flatCat,
		TrackWriterGZ: gzFile.Writer(),
	}, nil
}

func (w *CatWriter) WriteTrack(ct *cattrack.CatTrack) error {
	if err := json.NewEncoder(w.TrackWriterGZ).Encode(ct); err != nil {
		return err
	}
	cache.SetLastKnownTTL(w.CatID, ct)
	return nil
}

func (w *CatWriter) WriteSnap(ct *cattrack.CatTrack) error {
	return nil
}

func (w *CatWriter) Close() error {
	return w.TrackWriterGZ.Close()
}
