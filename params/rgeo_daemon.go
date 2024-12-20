package params

type RgeoDaemonConfig struct {
	ListenerConfig
	ServiceName string
	RPCPath     string
}

func DefaultRgeoDaemonConfig() *RgeoDaemonConfig {
	return &RgeoDaemonConfig{
		ListenerConfig: ListenerConfig{
			Network: "unix",
			Address: "/tmp/catd-rgeo.sock",
		},
		ServiceName: "ReverseGeocode",
		RPCPath:     "/rgeo_rpc",
	}
}

// InProcRgeoDaemonConfig are configuration defaults.
// It's a configuration structure instance shared between these at least:
// - cmd/rgeod.go
// - daemon/rgeod/daemon.go
// - rgeo/rgeo.go
// - cat backending
// This enables easy shared cli flag use for the various commands,
// like --tiled.listen.network, --tiled.listen.address, --rgeod.listen.network, etc.
// and is easy to pass around in the code.
var InProcRgeoDaemonConfig = DefaultRgeoDaemonConfig()
