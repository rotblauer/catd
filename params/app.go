package params

import (
	"github.com/rotblauer/catd/s2"
	"os"
	"path/filepath"
	"time"
)

var (
	CacheLastPushTTL  = 1 * 24 * time.Hour
	CacheLastKnownTTL = 7 * 24 * time.Hour
)

var S2DefaultCellLevels = []s2.CellLevel{
	s2.CellLevel5,  // Modest nation-state
	s2.CellLevel8,  // A day's ride
	s2.CellLevel13, // About a kilometer
	s2.CellLevel16, // Throwing distance
	s2.CellLevel23, // Human body
}

var DefaultBatchSize = 100_000

var DatadirRoot = func() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".catd")
}()
