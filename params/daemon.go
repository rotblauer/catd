package params

import (
	"os"
	"path/filepath"
	"time"
)

type DaemonConfig struct {
	RootDir          string
	DebounceInterval time.Duration

	// EdgeEpsilon is the time threshold for edge tiling to complete.
	// If edge tiling takes longer than this, the canonical source is tiled.
	EdgeEpsilon time.Duration

	RPCPath    string
	RPCNetwork string
	RPCAddress string

	TmpDir string
}

func DefaultDaemonConfig() *DaemonConfig {
	return &DaemonConfig{
		// RootDir is the root directory for the tiler daemon.
		// The daemon will create two subdirectories:
		// - `tiles` for the final .mbtiles files
		// - `source` for the associated source files
		// Each of these subdirectories will be further divided by cat ID.
		// In this way, the `source` directory will imitate the
		// cat state directory (datadir/cats/).
		// This allows an easy `cp -a` to init or reset the tiler daemon's data.
		RootDir:          filepath.Join(DatadirRoot, "tiled"),
		DebounceInterval: 10 * time.Second,
		EdgeEpsilon:      1 * time.Minute,
		RPCPath:          "/tiler_rpc",
		RPCNetwork:       "tcp",
		RPCAddress:       "localhost:1234",
		TmpDir:           filepath.Join(os.TempDir(), "catd-tilerdaemon"),
	}
}
