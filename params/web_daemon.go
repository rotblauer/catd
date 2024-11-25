package params

type WebDaemonConfig struct {
	// TileDaemonConfig is the configuration for a running tiled instance.
	// When cats push tracks, we can optionally make requests
	// on their behalf to the tiling service.
	// If nil, no tiling requests will happen.
	TileDaemonConfig *TileDaemonConfig

	NetAddr string
	NetPort int
}

func DefaultWebDaemonConfig() *WebDaemonConfig {
	return &WebDaemonConfig{
		TileDaemonConfig: DefaultTileDaemonConfig(),
		NetAddr:          "localhost",
		NetPort:          3000,
	}
}
