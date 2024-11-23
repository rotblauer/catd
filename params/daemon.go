package params

import (
	"os"
	"path/filepath"
	"time"
)

type DaemonConfig struct {
	RootDir                        string
	DebounceTilingRequestsInterval time.Duration

	// EdgeTilingRunThreshold is the time threshold for edge tiling to complete.
	// If edge tiling takes longer than this, the canonical source is tiled
	// and edge will be rolled.
	EdgeTilingRunThreshold time.Duration

	RPCPath    string
	RPCNetwork string
	RPCAddress string

	TmpDir string

	// AwaitPendingOnShutdown will cause the daemon to wait for all pending
	// Edge tile requests to complete (which may take a long time).
	AwaitPendingOnShutdown bool
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
		RootDir: filepath.Join(DatadirRoot, "tiled"),
		TmpDir:  filepath.Join(os.TempDir(), "catd-tilerdaemon-tmp"),

		// FIXME: Debounce should be derived from HTTP timeout threshold?
		DebounceTilingRequestsInterval: 15 * time.Second,

		// FIXME: Should be based on expected or actual rate of tile generation requests,
		// e.g. 1 / 100 seconds.
		EdgeTilingRunThreshold: 1 * time.Minute,

		RPCPath:                "/tiler_rpc",
		RPCNetwork:             "tcp",
		RPCAddress:             "localhost:1234",
		AwaitPendingOnShutdown: false,
	}
}
