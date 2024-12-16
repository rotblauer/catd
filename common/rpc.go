package common

import (
	"errors"
	"log/slog"
	"net/rpc"
)

type RPCArgNone *int

var argNone = 0
var ArgNone = &argNone

func DialRPC(network, address string) (*rpc.Client, error) {
	switch network {
	case "unix", "unixpacket":
		slog.Debug("Dialing RPC", "network", network, "address", address)
		return rpc.Dial(network, address)
	case "tcp", "tcp4", "tcp6":
		slog.Info("Dialing HTTP", "network", network, "address", address)
		return rpc.DialHTTP(network, address)
	}
	return nil, errors.New("unsupported network")
}
