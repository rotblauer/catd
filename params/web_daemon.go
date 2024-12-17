package params

type WebDaemonConfig struct {
	ListenerConfig
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
		ListenerConfig:   DefaultWebListenerConfig(),
		CatBackendConfig: DefaultCatBackendConfig(),
	}
}
