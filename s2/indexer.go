package s2

import (
	"encoding/json"
	"fmt"
)

type Indexer interface {
	Index(old, next Indexer) Indexer
	IsEmpty() bool
}

func UnmarshalIndexer(v []byte) (Indexer, error) {
	var targetIndexCountT IndexCountT
	if err := json.Unmarshal(v, &targetIndexCountT); err == nil {
		return targetIndexCountT, nil
	}
	var targetWrappedTrack WrappedTrack
	if err := json.Unmarshal(v, &targetWrappedTrack); err == nil {
		return targetWrappedTrack, nil
	}
	// TODO: add other possible types
	return nil, fmt.Errorf("unknown type")
}

// IndexCountT is an Indexer that counts the number of elements.
// It is an example of how to implement the Indexer interface.
type IndexCountT struct {
	Count int
}

func (it IndexCountT) Index(old, next Indexer) Indexer {
	if old == nil || old.IsEmpty() {
		old = IndexCountT{}
	}
	return IndexCountT{
		Count: old.(IndexCountT).Count + next.(IndexCountT).Count,
	}
}

func (it IndexCountT) IsEmpty() bool {
	return it.Count == 0
}
