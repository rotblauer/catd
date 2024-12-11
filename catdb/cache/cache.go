package cache

import (
	"github.com/jellydator/ttlcache/v3"
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
