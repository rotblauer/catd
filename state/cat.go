package state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"go.etcd.io/bbolt"
	"path/filepath"
	"sync/atomic"
)

type CatState struct {
	CatID   conceptual.CatID
	Datadir string
	rOnly   atomic.Bool
	isOpen  atomic.Bool
	*State
}

func NewCatState(catID conceptual.CatID, datadir string, readOnly bool) *CatState {
	rOnly := atomic.Bool{}
	rOnly.Store(readOnly)
	return &CatState{
		CatID:   catID,
		Datadir: datadir,
		rOnly:   rOnly,
		isOpen:  atomic.Bool{},
	}
}

// Open defines data sources, caches, and encoding for a cat.
// It should be non-contentious. It must be blocking; it should not permit
// competing writes or reads to cat state. It must be the one true canonical cat.
// It should be as simple and idiot proof as possible.
func (cs *CatState) Open() error {
	if cs.isOpen.Load() {
		return fmt.Errorf("cat state already open")
	}
	if cs.State == nil {
		cs.State = &State{}
	}

	cs.Flat = catz.NewFlatWithRoot(cs.Datadir)
	if cs.rOnly.Load() == false {
		if err := cs.Flat.MkdirAll(); err != nil {
			return err
		}
	} else {
		// Test if dir exist in read-only mode.
		fi, err := cs.Flat.Stat()
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			return fmt.Errorf("cat data dir is not a directory")
		}
	}

	// Opening a writable DB conn will block all other cat writers and readers
	// with essentially a file lock/flock.
	var err error
	dbPath := filepath.Join(cs.Flat.Path(), params.CatStateDBName)
	cs.DB, err = bbolt.Open(dbPath, 0600, &bbolt.Options{
		ReadOnly: cs.rOnly.Load(),
	})
	if err != nil {
		return fmt.Errorf("bbolt failed open: %w (db.path=%s, flat.path=%s)",
			err, dbPath, cs.Flat.Path())
	}
	cs.isOpen.Store(true)

	return nil
}

func (s *CatState) IsOpen() bool {
	return s.isOpen.Load()
}

func (s *CatState) IsReadOnly() bool {
	return s.rOnly.Load()
}

func (s *CatState) SetReadWrite(rOnly bool) {
	s.rOnly.Store(rOnly)
}

func (s *CatState) Close() error {
	defer s.isOpen.Store(false)
	if err := s.DB.Close(); err != nil {
		return err
	}
	return nil
}

func (s *State) StoreKVMarshalJSON(bucket []byte, key []byte, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.storeKV(bucket, key, data)
}

func (s *State) ReadKVUnmarshalJSON(bucket []byte, key []byte, v interface{}) error {
	data, err := s.readKV(bucket, key)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func (s *State) StoreKV(bucket []byte, key []byte, value []byte) error {
	return s.storeKV(bucket, key, value)
}

func (s *State) ReadKV(bucket []byte, key []byte) ([]byte, error) {
	return s.readKV(bucket, key)
}

func (s *State) storeKV(bucket []byte, key []byte, data []byte) error {
	if key == nil {
		return fmt.Errorf("storeKV: nil key")
	}
	if data == nil {
		return fmt.Errorf("storeKV: nil data")
	}
	return s.DB.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		return b.Put(key, data)
	})
}

func (s *State) readKV(bucket []byte, key []byte) ([]byte, error) {
	if key == nil {
		return nil, fmt.Errorf("readKV: nil key")
	}

	buf := bytes.NewBuffer([]byte{})
	//var got []byte
	err := s.DB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return fmt.Errorf("no state bucket")
		}

		// Gotcha! The value returned by Get is only valid in the scope of the transaction.
		_, err := buf.Write(b.Get(key))
		return err
	})
	return buf.Bytes(), err
	//return got, err
}
