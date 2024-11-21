package tiler

import (
	"github.com/rotblauer/catd/params"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
)

const RPCPath = "/tiler_rpc"
const RPCNetwork = "tcp"
const RPCAddress = "localhost:1234"

// RunDaemon starts the tiler daemon.
func RunDaemon(quit <-chan struct{}) {
	server := rpc.NewServer()

	daemon := NewDaemon()

	if err := server.Register(daemon); err != nil {
		slog.Error("Failed to register tiler daemon", "error", err)
		return
	}

	server.HandleHTTP(RPCPath, rpc.DefaultDebugPath)
	l, err := net.Listen(RPCNetwork, RPCAddress)
	if err != nil {
		log.Fatal("listen error:", err)
	}

	go func() {
		if err := http.Serve(l, server); err != nil {
			log.Fatal("serve error:", err)
		}
	}()

	slog.Info("TilerDaemon RPC HTTP server started")
	defer slog.Info("TilerDaemon stopped")
	for {
		select {
		case <-quit:
			slog.Warn("TilerDaemon quitting...")
			return
		}
	}
}

type Daemon struct {
	logger *slog.Logger
}

func NewDaemon() *Daemon {
	return &Daemon{
		logger: slog.With("daemon", "tiler"),
	}
}

type TilingRequestArgs struct {
	Source string
	Config params.TippeConfigT
}

type TilingResponse struct {
	Success bool
}

func (d *Daemon) RequestTiling(args *TilingRequestArgs, reply *TilingResponse) error {
	slog.Info("Requesting tiling", "source", args.Source, "config", args.Config)
	switch args.Config {
	case params.TippeConfigTracks:
		// TODO
	case params.TippeConfigSnaps:
		// TODO
	case params.TippeConfigLaps:
		tipFromReader()
	case params.TippeConfigNaps:
		// TODO
	default:
		slog.Warn("Unknown tiling config", "config", args.Config)
	}
	return nil
}

func (d *Daemon) Trigger(args *TilingRequestArgs, reply *TilingResponse) error {
	slog.Info("Triggering tiling", "source", args.Source, "config", args.Config)
	return nil
}

/*
type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Arith int

func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}
*/
