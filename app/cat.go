package app

import (
	"encoding/json"
	"fmt"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"go.etcd.io/bbolt"
	"io"
	"path/filepath"
)

type Cat struct {
	CatID conceptual.CatID
}

type CatWriter struct {
	CatID conceptual.CatID
	Flat  *flat.Flat
}

// NewCatWriter defines filepath and encoding for a cat.
// It should be non-contentious.
func (c *Cat) NewCatWriter() (*CatWriter, error) {
	flatCat := flat.NewFlatWithRoot(params.DatadirRoot).ForCat(c.CatID)
	if err := flatCat.Ensure(); err != nil {
		return nil, err
	}
	return &CatWriter{
		CatID: c.CatID,
		Flat:  flatCat,
	}, nil
}

func (w *CatWriter) WriteTrack(wr io.Writer, ct *cattrack.CatTrack) error {
	if err := json.NewEncoder(wr).Encode(ct); err != nil {
		return err
	}
	cache.SetLastKnownTTL(w.CatID, ct)
	return nil
}

func (w *CatWriter) TrackWriter() (io.WriteCloser, error) {
	gzf, err := w.Flat.TracksGZ()
	if err != nil {
		return nil, err
	}
	return gzf.Writer(), nil

}

func (w *CatWriter) CustomWriter(target string) (io.WriteCloser, error) {
	f, err := w.Flat.NamedGZ(target)
	if err != nil {
		return nil, err
	}
	wr := f.Writer()
	return wr, nil
}

func (w *CatWriter) PersistLastTrack() error {
	catPath := w.Flat.Path()
	db, err := bbolt.Open(filepath.Join(catPath, "app.db"), 0600, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("app"))
		if err != nil {
			return err
		}
		track := cache.LastKnownTTLCache.Get(w.CatID.String())
		if track == nil {
			return fmt.Errorf("no last track (impossible if caller uses correctly)")
		}
		b, err := track.Value().MarshalJSON()
		if err != nil {
			return err
		}
		return bucket.Put([]byte("last"), b)
	})
}

func (w *CatWriter) WriteSnap(ct *cattrack.CatTrack) error {
	return nil
}

func (w *CatWriter) Close() error {
	return nil
}

type CatReader struct {
	CatID conceptual.CatID
	Flat  *flat.Flat
}

func (c *Cat) NewCatReader() (*CatReader, error) {
	f := flat.NewFlatWithRoot(params.DatadirRoot).ForCat(c.CatID)
	if !f.Exists() {
		return nil, fmt.Errorf("cat not found")
	}
	return &CatReader{
		CatID: c.CatID,
		Flat:  f,
	}, nil
}

func (w *CatReader) ReadLastTrack() (*cattrack.CatTrack, error) {
	catPath := w.Flat.Path()
	db, err := bbolt.Open(filepath.Join(catPath, "app.db"), 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()
	track := &cattrack.CatTrack{}
	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("app"))
		if bucket == nil {
			return fmt.Errorf("no app bucket")
		}
		b := bucket.Get([]byte("last"))
		if b == nil {
			return fmt.Errorf("no last track")
		}
		return json.Unmarshal(b, track)
	})
	return track, err
}
