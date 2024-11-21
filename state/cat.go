package state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"go.etcd.io/bbolt"
	"io"
	"path/filepath"
	"sync"
)

const catDBName = "state.db"

var catStateBucket = []byte("state")

type Cat struct {
	CatID conceptual.CatID
	State *CatState
}

type CatState struct {
	CatID conceptual.CatID
	DB    *bbolt.DB
	Flat  *flat.Flat

	Waiting sync.WaitGroup
	rOnly   bool
}

// NewCatState defines data sources, caches, and encoding for a cat.
// It should be non-contentious. It must be blocking; it should not permit
// competing writes or reads to cat state. It must be the one true canonical cat.
func (c *Cat) NewCatState(readOnly bool) (*CatState, error) {
	flatCat := flat.NewFlatWithRoot(params.DatadirRoot).ForCat(c.CatID)

	if !readOnly {
		if err := flatCat.Ensure(); err != nil {
			return nil, err
		}
	}

	// Opening a writable DB conn will block all other cat writers and readers
	// with essentially a file lock/flock.
	db, err := bbolt.Open(filepath.Join(flatCat.Path(), catDBName),
		0600, &bbolt.Options{
			ReadOnly: readOnly,
		})
	if err != nil {
		return nil, err
	}

	s := &CatState{
		CatID: c.CatID,
		DB:    db,
		Flat:  flatCat,
	}
	c.State = s
	return c.State, nil
}

func (s *CatState) Wait() {
	s.Waiting.Wait()
}

func (s *CatState) Close() error {
	if err := s.DB.Close(); err != nil {
		return err
	}
	return nil
}

func (s *CatState) WriteTrack(wr io.Writer, ct *cattrack.CatTrack) error {
	if err := json.NewEncoder(wr).Encode(ct); err != nil {
		return err
	}
	return nil
}

func (s *CatState) TrackGZWriter() (io.WriteCloser, error) {
	gzf, err := s.Flat.TracksGZWriter()
	if err != nil {
		return nil, err
	}
	return gzf.Writer(), nil

}

func (s *CatState) CustomGZWriter(target string) (io.WriteCloser, error) {
	f, err := s.Flat.NamedGZWriter(target)
	if err != nil {
		return nil, err
	}
	wr := f.Writer()
	return wr, nil
}

func (s *CatState) storeKV(key []byte, data []byte) error {
	if key == nil {
		return fmt.Errorf("storeKV: nil key")
	}
	if data == nil {
		return fmt.Errorf("storeKV: nil data")
	}
	return s.DB.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(catStateBucket)
		if err != nil {
			return err
		}
		return bucket.Put(key, data)
	})
}

// StoreTracksKV stores tracks in a KV store.
// It is expected that this function can be used to cache
// partial naps or partial laps (i.e. their last, unfinished, incomplete nap or lap).
// Keep in mind that the tracks are buffered in memory, not streamed.
// For this reason, it may be prudent to limit the number of tracks stored (and read!) this way,
// and/or to limit the use of it.
// However, tracks are encoded in newline-delimited JSON to allow for streaming, someday, maybe.
func (s *CatState) StoreTracksKV(key []byte, tracks []*cattrack.CatTrack) error {
	buf := bytes.NewBuffer([]byte{})
	enc := json.NewEncoder(buf)
	for _, track := range tracks {
		if err := enc.Encode(track); err != nil {
			return err
		}
	}
	return s.storeKV(key, buf.Bytes())
}

func (s *CatState) WriteKV(key []byte, value []byte) error {
	return s.storeKV(key, value)
}

// WriteSnap is not implemented.
// TODO: Implement WriteSnap.
func (s *CatState) WriteSnap(ct *cattrack.CatTrack) error {
	return fmt.Errorf("not implemented")
}

func (w *CatState) readKV(key []byte) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	err := w.DB.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(catStateBucket)
		if bucket == nil {
			return fmt.Errorf("no state bucket")
		}

		// Gotcha! The value returned by Get is only valid in the scope of the transaction.
		got := bucket.Get(key)
		if got == nil {
			return nil
		}
		_, err := buf.Write(got)
		return err
	})
	return buf.Bytes(), err
}

func (w *CatState) ReadKV(key []byte) ([]byte, error) {
	if key == nil {
		return nil, fmt.Errorf("readKV: nil key")
	}
	return w.readKV(key)
}

func (w *CatState) ReadTracksKV(key []byte) ([]*cattrack.CatTrack, error) {
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
