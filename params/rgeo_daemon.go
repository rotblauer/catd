package params

type RgeoDaemonConfig struct {
	ListenerConfig
	Name    string
	RPCPath string
}

func DefaultRgeoDaemonConfig() *RgeoDaemonConfig {
	return &RgeoDaemonConfig{
		ListenerConfig: ListenerConfig{
			Network: "unix",
			Address: "/tmp/catd-rgeo.sock",
		},
		Name:    "ReverseGeocode",
		RPCPath: "/rgeo_rpc",
	}
}

// InProcRgeoDaemonConfig is a configuration structure instance
// which is shared between
// - cmd/rgeod.go
// - daemon/rgeod/daemon.go
// - rgeo/rgeo.go
// - cat backending
var InProcRgeoDaemonConfig = DefaultRgeoDaemonConfig()
