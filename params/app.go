package params

import (
	"github.com/rotblauer/catd/s2"
	"time"
)

var (
	CacheLastPushTTL  = 1 * 24 * time.Hour
	CacheLastKnownTTL = 7 * 24 * time.Hour
)

var S2DefaultCellLevels = []s2.CellLevel{s2.CellLevel16, s2.CellLevel23}
