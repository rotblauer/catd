package cache

import (
	"fmt"
	"github.com/golang/groupcache/lru"
	"github.com/jellydator/ttlcache/v3"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
)

var LastPushTTLCache = ttlcache.New[string, []*cattrack.CatTrack](
	ttlcache.WithTTL[string, []*cattrack.CatTrack](params.CacheLastPushTTL))

var LastKnownTTLCache = ttlcache.New[string, *cattrack.CatTrack](
	ttlcache.WithTTL[string, *cattrack.CatTrack](params.CacheLastKnownTTL))

func SetLastKnownTTL(catID conceptual.CatID, ct *cattrack.CatTrack) {
	LastKnownTTLCache.Set(catID.String(), ct, ttlcache.DefaultTTL)
}

var PopulateDedupeCache = lru.New(10_000)

// DedupePassLRU returns true if the point is not a duplicate
// using a Least Recently Used (LRU) cache.
func DedupePassLRU(ct *cattrack.CatTrack) bool {
	// The hash of the feature is used to deduplicate points.
	hash, err := hashstructure.Hash(ct, hashstructure.FormatV2, nil)
	if err != nil {
		return false
	}

	key := fmt.Sprintf("%d", hash)
	_, ok := PopulateDedupeCache.Get(key)
	if ok {
		return false
	}
	PopulateDedupeCache.Add(key, true)
	return true
}

func NewDedupePassLRUFunc() func(cattrack.CatTrack) bool {
	var dedupeCache = lru.New(10_000)
	return func(track cattrack.CatTrack) bool {
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
