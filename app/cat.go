package app

import (
	"bytes"
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

const catDBName = "app.db"

var catStateBucket = []byte("state")

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

func (w *CatWriter) storeKV(key []byte, data []byte) error {
	catPath := w.Flat.Path()
	db, err := bbolt.Open(filepath.Join(catPath, catDBName), 0600, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(catStateBucket)
		if err != nil {
			return err
		}
		return bucket.Put(key, data)
	})
}

func (w *CatWriter) StoreLastTrack() error {
	track := cache.LastKnownTTLCache.Get(w.CatID.String())
	if track == nil {
		return fmt.Errorf("no last track (impossible if caller uses correctly)")
	}
	b, err := track.Value().MarshalJSON()
	if err != nil {
		return err
	}
	return w.storeKV([]byte("last"), b)
}

// StoreTracksAt stores tracks in a KV store.
// It is expected that this function can be used to cache
// partial naps or partial laps (i.e. their last, unfinished, incomplete nap or lap).
// Keep in mind that the tracks are buffered in memory, not streamed.
// For this reason, it may be prudent to limit the number of tracks stored (and read!) this way,
// and/or to limit the use of it.
// However, tracks are encoded in newline-delimited JSON to allow for streaming, someday, maybe.
func (w *CatWriter) StoreTracksAt(key []byte, tracks []*cattrack.CatTrack) error {
	buf := bytes.NewBuffer([]byte{})
	enc := json.NewEncoder(buf)
	for _, track := range tracks {
		if err := enc.Encode(track); err != nil {
			return err
		}
	}
	return w.storeKV(key, buf.Bytes())
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

func (w *CatReader) readKV(key []byte) ([]byte, error) {
	catPath := w.Flat.Path()
	db, err := bbolt.Open(filepath.Join(catPath, catDBName), 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()
	var b []byte
	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(catStateBucket)
		if bucket == nil {
			return fmt.Errorf("no app bucket")
		}
		b = bucket.Get(key)
		return nil
	})
	return b, err
}

func (w *CatReader) ReadLastTrack() (*cattrack.CatTrack, error) {
	got, err := w.readKV([]byte("last"))
	if err != nil {
		return nil, err
	}
	track := &cattrack.CatTrack{}
	err = track.UnmarshalJSON(got)
	return track, err
}

func (w *CatReader) ReadTracksAt(key []byte) ([]*cattrack.CatTrack, error) {
	got, err := w.readKV(key)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(got))
	var tracks []*cattrack.CatTrack
	for {
		track := &cattrack.CatTrack{}
		if err := dec.Decode(track); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		tracks = append(tracks, track)
	}
	return tracks, nil
}
