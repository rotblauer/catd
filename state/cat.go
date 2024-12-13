package state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"go.etcd.io/bbolt"
	"path/filepath"
)

type Cat struct {
	CatID conceptual.CatID
	State *CatState
}

type CatState struct {
	CatID conceptual.CatID
	*State
}

// NewCatWithState defines data sources, caches, and encoding for a cat.
// It should be non-contentious. It must be blocking; it should not permit
// competing writes or reads to cat state. It must be the one true canonical cat.
func (c *Cat) NewCatWithState(readOnly bool) (*CatState, error) {
	flatCat := flat.NewFlatWithRoot(params.DatadirRoot).Joins(params.CatsDir, c.CatID.String())

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
			DB:   db,
			Flat: flatCat,
		},
	}
	c.State = s
	return c.State, nil
}

func (s *State) Close() error {
	if err := s.DB.Close(); err != nil {
		return err
	}
	return nil
}

func (s *State) NamedGZWriter(target string) (*flat.GZFileWriter, error) {
	f, err := s.Flat.NamedGZWriter(target, nil)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (s *State) StoreKVJSON(bucket []byte, key []byte, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.storeKV(bucket, key, data)
}

func (s *State) StoreKV(bucket []byte, key []byte, value []byte) error {
	return s.storeKV(bucket, key, value)
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

func (s *State) ReadKVUnmarshal(bucket []byte, key []byte, v interface{}) error {
	data, err := s.readKV(bucket, key)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func (s *State) ReadKV(bucket []byte, key []byte) ([]byte, error) {
	return s.readKV(bucket, key)
}

func (s *State) readKV(bucket []byte, key []byte) ([]byte, error) {
	if key == nil {
		return nil, fmt.Errorf("readKV: nil key")
	}

	buf := bytes.NewBuffer([]byte{})
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
}
