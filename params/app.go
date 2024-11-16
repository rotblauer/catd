package params

import "time"

var (
	CacheLastPushTTL  = 1 * 24 * time.Hour
	CacheLastKnownTTL = 7 * 24 * time.Hour
)
