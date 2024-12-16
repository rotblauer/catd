package params

type RgeoDaemonConfig struct {
	ListenerConfig
	RPCPath string
}

func DefaultRgeoDaemonConfig() *RgeoDaemonConfig {
	return &RgeoDaemonConfig{
		ListenerConfig: ListenerConfig{
			Network: "unix",
			Address: "/tmp/catd-rgeo.sock",
		},
		RPCPath: "/rgeo_rpc",
	}
}
