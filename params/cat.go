package params

import "path/filepath"

type CatRPCServices struct {
	// TileDaemonConfig is the configuration for a running TileD instance.
	// When cats push tracks, we can optionally make requests
	// on their behalf to the tiling service.
	// If nil, no tiling requests will happen.
	TileD *TileDaemonConfig

	// RgeoDaemonConfig is the configuration for connecting an RgeoD instance,
	// which provides reverse geocoding.
	// If no configuration is provided, or the connection fails,
	// reverse geocoding will not happen.
	RgeoD *RgeoDaemonConfig
}

func DefaultCatBackendConfig() *CatRPCServices {
	return &CatRPCServices{
		TileD: DefaultTileDaemonConfig(),
		RgeoD: InProcRgeoDaemonConfig,
	}
}

// DefaultCatDataDir returns the default datadir for a cat
// rooted in the params.DefaultDatadirRoot.
func DefaultCatDataDir(catID string) string {
	return DefaultCatDataDirRooted(DefaultDatadirRoot, catID)
}

// DefaultCatDataDirRooted returns the default datadir for a cat
// rooted in the given dataDirRoot.
func DefaultCatDataDirRooted(dataDirRoot string, catID string) string {
	return filepath.Join(dataDirRoot, CatsDir, catID)
}

var CatStateBucket = []byte("state")
var CatSnapBucket = []byte("snaps")

// CatStateKey_* are the names of the keys of various state objects in the CatState.

// v9000
var CatStateKey_ActImprover = []byte("act_improver")
var CatStateKey_Unbacktracker = []byte("unbacktracker")
var CatStateKey_Laps = []byte("laps")
var CatStateKey_Naps = []byte("naps")
var CatStateKey_OffsetIndexer = []byte("offset_indexer")

// v0
//var CatStateKey_ActImprover = []byte("act-improver")
//var CatStateKey_Unbacktracker = []byte("catUUIDWindowMap")
//var CatStateKey_Laps = []byte("lapstate")
//var CatStateKey_Naps = []byte("napstate")
//var CatStateKey_OffsetIndexer = []byte("offsetIndexer")
