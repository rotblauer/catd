package params

type ListenerConfig struct {
	// Network is the network to listen on.
	// The network must be "tcp", "tcp4", "tcp6", "unix" or "unixpacket".
	Network string
	// Address is the address to listen on.
	Address string
}
