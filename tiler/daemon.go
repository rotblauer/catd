package tiler

import (
	"fmt"
	"github.com/bep/debounce"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const RPCPath = "/tiler_rpc"
const RPCNetwork = "tcp"
const RPCAddress = "localhost:1234"

type Daemon struct {
	Config       *DaemonConfig
	logger       *slog.Logger
	debouncers   map[string]func(func())
	queue        sync.Map
	debouncersMu sync.Mutex
	pending      sync.WaitGroup
	running      sync.WaitGroup
}

type DaemonConfig struct {
	MBTilesRoot      string
	DebounceInterval time.Duration
}

func DefaultDaemonConfig() *DaemonConfig {
	return &DaemonConfig{
		MBTilesRoot:      filepath.Join(params.DatadirRoot, "tiles"),
		DebounceInterval: 5 * time.Second,
	}
}

func NewDaemon(config *DaemonConfig) *Daemon {
	return &Daemon{
		Config:     config,
		logger:     slog.With("daemon", "tiler"),
		debouncers: make(map[string]func(func())),
		queue:      sync.Map{},
	}
}

// RunDaemon starts the tiler daemon.
func RunDaemon(config *DaemonConfig, quit <-chan struct{}) (done chan struct{}) {
	done = make(chan struct{})

	if config == nil {
		config = DefaultDaemonConfig()
	}

	server := rpc.NewServer()
	daemon := NewDaemon(config)

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

	go func() {
		defer func() {
			done <- struct{}{}
			close(done)
		}()
		slog.Info("TilerDaemon RPC HTTP server started")
		defer slog.Info("TilerDaemon stopped")
		for {
			select {
			case <-quit:
				daemon.logger.Info("TilerDaemon quitting, waiting on pending tasks...")
				daemon.pending.Wait()
				daemon.logger.Info("TilerDaemon quitting, waiting on running tasks...")
				daemon.running.Wait()
				daemon.logger.Info("TilerDaemon tasks done, exiting")
				return
			}
		}
	}()
	return done
}

type TilingRequestArgs struct {
	CatID    conceptual.CatID
	SourceGZ string
	Config   params.TippeConfigT
}

type TilingResponse struct {
	Success bool
}

func conventionalMBTilesBaseName(sourceGZ string) string {
	// Trim any and all extensions off the source file name,
	// then append .mbtiles extension.
	// This is the output file name.
	out := filepath.Base(sourceGZ)
	for filepath.Ext(out) != "" {
		out = strings.TrimSuffix(out, filepath.Ext(out))
	}
	out = out + ".mbtiles"
	return out
}

// tmpOutput returns a convention-driven path to a temporary .mbtiles file
// derived from the source (.gz) file name.
// Example:
// /datadir/catid/laps.geojson.gz => /tmp/tiler-daemon/catid/laps.mbtiles
func (r *TilingRequestArgs) tmpOutput() string {
	out := conventionalMBTilesBaseName(r.SourceGZ)
	return filepath.Join(os.TempDir(), "tiler-daemon", r.CatID.String(), out)
}

// finalOutput returns a convention-driven path to the final .mbtiles file,
// derived from the source (.gz) file name.
// Example:
// /datadir/cats/catid/laps.geojson.gz => /datadir/tiles/catid/laps.mbtiles
func (r *TilingRequestArgs) finalOutput() string {
	base := conventionalMBTilesBaseName(r.SourceGZ)
	return filepath.Join(params.DatadirRoot, "tiles", r.CatID.String(), base)
}

func (d *Daemon) enqueue(source string) {
	if _, ok := d.queue.Load(source); !ok {
		d.queue.Store(source, struct{}{})
		d.pending.Add(1)
	}
}

func (d *Daemon) unqueue(source string) {
	d.queue.Delete(source)
	d.pending.Done()
}

func (d *Daemon) RequestTiling(args *TilingRequestArgs, reply *TilingResponse) error {
	d.logger.Info("Requesting tiling", "source", args.SourceGZ, "type", args.Config)

	var debouncer func(func())
	d.debouncersMu.Lock()
	if f, ok := d.debouncers[args.SourceGZ]; ok {
		debouncer = f
	} else {
		debouncer = debounce.New(5 * time.Second)
		d.debouncers[args.SourceGZ] = debouncer
	}
	d.debouncersMu.Unlock()

	d.enqueue(args.SourceGZ)
	debouncer(func() {
		if err := d.doTiling(args, reply); err != nil {
			d.logger.Error("Tiling errored", "error", err)
		}
	})
	return nil
}

func (d *Daemon) doTiling(args *TilingRequestArgs, reply *TilingResponse) error {
	d.running.Add(1)
	defer d.running.Done()
	defer d.unqueue(args.SourceGZ)

	d.logger.Info("Tiling", "source", args.SourceGZ, "type", args.Config)

	cliConfig := params.CLIFlagsT{}
	switch args.Config {
	case params.TippeConfigTracks:
		cliConfig = params.DefaultTippeConfigs.Tracks()
	case params.TippeConfigSnaps:
		// TODO: Implement snaps
		cliConfig = params.DefaultTippeConfigs.Tracks()
	case params.TippeConfigLaps:
		cliConfig = params.DefaultTippeConfigs.Laps()
	case params.TippeConfigNaps:
		cliConfig = params.DefaultTippeConfigs.Naps()
	default:
		slog.Warn("Unknown tiling config", "config", args.Config)
		return fmt.Errorf("unknown tiling config: %s", args.Config)
	}

	// Use and then rename a temporary output file to avoid
	// overwriting/deleting/modifying the final output file
	// - in case of failure
	// - in case the final output is being used by another process
	tmpTarget := args.tmpOutput()

	cliConfig.MustSetPair("--layer", string(args.Config)).
		MustSetPair("--name", string(args.Config)).
		MustSetPair("--output", tmpTarget)

	if err := os.MkdirAll(filepath.Dir(tmpTarget), os.ModePerm); err != nil {
		slog.Error("Failed to create temp dir", "error", err)
		return err
	}

	d.logger.Info("Tiling", "source", args.SourceGZ, "type", args.Config,
		"tmp", tmpTarget)

	start := time.Now()
	if err := d.tip(args.SourceGZ, cliConfig); err != nil {
		d.logger.Error("Failed to tip", "error", err)
		return err
	}
	elapsed := time.Since(start)

	finalTarget := args.finalOutput()

	d.logger.Info("Tiling done", "source", args.SourceGZ, "type", args.Config,
		"final", finalTarget, "elapsed", elapsed.Round(time.Millisecond))

	if err := os.MkdirAll(filepath.Dir(finalTarget), os.ModePerm); err != nil {
		d.logger.Error("Failed to create final dir", "error", err)
		return err
	}

	if err := os.Rename(tmpTarget, finalTarget); err != nil {
		d.logger.Error("Failed to move tmp to final", "error", err)
	}

	return nil
}

/*
RPC example:

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
