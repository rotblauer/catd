package params

type WebDaemonConfig struct {
	ListenerConfig
	DataDir          string
	CatBackendConfig *CatRPCServices
}

func DefaultWebListenerConfig() ListenerConfig {
	return ListenerConfig{
		Network: "tcp",
		Address: "localhost:3000",
	}
}

func DefaultWebDaemonConfig() *WebDaemonConfig {
	return &WebDaemonConfig{
		DataDir:          DefaultDatadirRoot,
		ListenerConfig:   DefaultWebListenerConfig(),
		CatBackendConfig: DefaultCatBackendConfig(),
	}
}

func DefaultTestWebDaemonConfig() *WebDaemonConfig {
	d := &WebDaemonConfig{
		DataDir: "",
		ListenerConfig: ListenerConfig{
			Network: "tcp",
			Address: "localhost:3333",
		},
		CatBackendConfig: nil,
		//CatBackendConfig: &CatRPCServices{
		//	TileD: &TileDaemonConfig{
		//		ListenerConfig: ListenerConfig{
		//			Network: "tcp",
		//			Address: "localhost:3334",
		//		},
		//	},
		//	RgeoD: &RgeoDaemonConfig{
		//		ListenerConfig: ListenerConfig{
		//			Network: "unix",
		//			Address: "/tmp/catd-rgeo.sock",
		//		},
		//	},
		//},
	}
	return d
}
