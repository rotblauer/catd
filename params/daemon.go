package params

import (
	"os"
	"path/filepath"
	"time"
)

type DaemonConfig struct {
	// RootDir is the parent directory for source and tile data.
	// Source data is stored in rootdir/source/, tile data in rootdir/tiles/.
	RootDir string

	// TilingPendingExpiry is how long to "debounce" requests
	// made to the RequestTiling method.
	TilingPendingExpiry time.Duration

	// EdgeTilingRunThreshold is the time threshold for edge tiling to complete.
	// If edge tiling takes longer than this, edge will be rolled and
	// a tiling request for the canonical version will be triggered.
	EdgeTilingRunThreshold time.Duration

	RPCPath    string
	RPCNetwork string
	RPCAddress string

	// TilingTmpDir is the parent directory for temporary source and tile files.
	// Cleanup not guaranteed. Probably best somewhere in /tmp.
	TilingTmpDir string

	// AwaitPendingOnShutdown will cause the daemon to wait for all pending
	// tiling requests to complete (which may take a long time).
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
		RootDir:      filepath.Join(DatadirRoot, "tiled"),
		TilingTmpDir: filepath.Join(os.TempDir(), "catd-tilerdaemon-tmp"),

		// FIXME: Debounce should be derived from HTTP timeout threshold?
		TilingPendingExpiry: 15 * time.Second,

		// FIXME: Should be based on expected or actual rate of tile generation requests,
		// e.g. 1 / 100 seconds.
		EdgeTilingRunThreshold: 1 * time.Minute,

		RPCPath:                "/tiler_rpc",
		RPCNetwork:             "tcp",
		RPCAddress:             "localhost:1234",
		AwaitPendingOnShutdown: false,
	}
}
