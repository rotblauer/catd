package params

type CatBackendConfig struct {
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

func DefaultCatBackendConfig() *CatBackendConfig {
	return &CatBackendConfig{
		TileD: DefaultTileDaemonConfig(),
		RgeoD: DefaultRgeoDaemonConfig(),
	}
}
