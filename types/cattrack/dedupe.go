package cattrack

import (
	"fmt"
	"github.com/golang/groupcache/lru"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/rotblauer/catd/params"
)

func NewDedupeLRUFunc() func(CatTrack) bool {
	var dedupeCache = lru.New(params.DefaultBatchSize)
	return func(track CatTrack) bool {
		hash, err := hashstructure.Hash(track, hashstructure.FormatV2, nil)
		if err != nil {
			return false
		}
		key := fmt.Sprintf("%d", hash)
		_, ok := dedupeCache.Get(key)
		if ok {
			return false
		}
		dedupeCache.Add(key, true)
		return true
	}
}
