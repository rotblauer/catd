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
)

type CatState struct {
	CatID conceptual.CatID
	*State
}

// Open defines data sources, caches, and encoding for a cat.
// It should be non-contentious. It must be blocking; it should not permit
// competing writes or reads to cat state. It must be the one true canonical cat.
func (c *CatState) Open(catRoot string, readOnly bool) (*CatState, error) {

	if c.State != nil && c.open && readOnly == c.rOnly {
		return c, nil // Or throw error?
	}
	if c.State != nil && !c.open {
		// Opening a writable DB conn will block all other cat writers and readers
		// with essentially a file lock/flock.
		db, err := bbolt.Open(filepath.Join(c.Flat.Path(), params.CatStateDBName),
			0600, &bbolt.Options{
				ReadOnly: readOnly,
			})
		if err != nil {
			return nil, err
		}
		c.DB = db
		c.open = true
		return c, nil
	}

	flatCat := catz.NewFlatWithRoot(catRoot)
	if !readOnly {
		if err := flatCat.MkdirAll(); err != nil {
			return nil, err
		}
	}

	// Opening a writable DB conn will block all other cat writers and readers
	// with essentially a file lock/flock.
	db, err := bbolt.Open(filepath.Join(flatCat.Path(), params.CatStateDBName),
		0600, &bbolt.Options{
			ReadOnly: readOnly,
		})
	if err != nil {
		return nil, err
	}

	s := &CatState{
		CatID: c.CatID,
		State: &State{
			open: true,
			DB:   db,
			Flat: flatCat,
		},
	}
	return s, nil
}

func (s *State) IsOpen() bool {
	return s.open
}

func (s *State) Close() error {
	s.open = false
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
