package tiler

import (
	"fmt"
	"github.com/bep/debounce"
	"github.com/rotblauer/catd/catdb/flat"
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
	Config *DaemonConfig

	// flat is the flat file storage (root) for the tiler daemon.
	// Normally it will be nested in the default app datadir (see params.go).
	// It is necessary for the tiler daemon to maintain its own data store
	// of source data along with the resulting .mbtiles files.
	// This is because the tiler daemon is a separate process(es) from the main app
	// and should not compete on file locks with the main app, which could quickly
	// result in corrupted data.
	flat *flat.Flat

	logger        *slog.Logger
	debouncers    map[string]func(func())
	tilingQueue   sync.Map
	writeLock     sync.Map
	debouncersMu  sync.Mutex
	tilingPending sync.WaitGroup
	running       sync.WaitGroup

	Done      chan struct{}
	Interrupt chan struct{}
}

type DaemonConfig struct {
	RootDir          string
	DebounceInterval time.Duration
}

func DefaultDaemonConfig() *DaemonConfig {
	return &DaemonConfig{
		// RootDir is the root directory for the tiler daemon.
		// The daemon will create two subdirectories:
		// - `tiles` for the final .mbtiles files
		// - `source` for the associated source files
		// Each of these subdirectories will be further divided by cat ID.
		// In this way, the `source` directory will imitate the
		// cat state directory (datadir/cats/).
		// This allows an easy `cp -a` to init or reset the tiler daemon's data.
		RootDir:          filepath.Join(params.DatadirRoot, "tiled"),
		DebounceInterval: 5 * time.Second,
	}
}

func NewDaemon(config *DaemonConfig) *Daemon {
	if config == nil {
		config = DefaultDaemonConfig()
	}

	return &Daemon{
		Config:      config,
		flat:        flat.NewFlatWithRoot(config.RootDir),
		logger:      slog.With("daemon", "tiler"),
		debouncers:  make(map[string]func(func())),
		tilingQueue: sync.Map{},
		writeLock:   sync.Map{},

		Done:      make(chan struct{}, 1),
		Interrupt: make(chan struct{}, 1),
	}
}

// Run starts the tiler daemon.
func (d *Daemon) Run() error {
	server := rpc.NewServer()

	if err := server.Register(d); err != nil {
		slog.Error("Failed to register tiler daemon", "error", err)
		return err
	}

	server.HandleHTTP(RPCPath, rpc.DefaultDebugPath)
	l, err := net.Listen(RPCNetwork, RPCAddress)
	if err != nil {
		return err
	}

	go func() {
		if err := http.Serve(l, server); err != nil {
			log.Fatal("TileDaemon RPC HTTP serve error:", err)
		}
	}()

	go func() {
		defer func() {
			d.Done <- struct{}{}
			close(d.Done)
		}()
		d.logger.Info("TilerDaemon RPC HTTP server started",
			slog.Group("listen", "network", RPCNetwork, "address", RPCAddress))

		for {
			select {
			case <-d.Interrupt:
				d.logger.Info("TilerDaemon quitting, waiting on tilingPending tasks...")
				d.tilingPending.Wait()
				d.logger.Info("TilerDaemon quitting, waiting on running tasks...")
				d.running.Wait()
				d.logger.Info("TilerDaemon tasks done, exiting")
				return
			}
		}
	}()
	return nil
}

type PushFeaturesRequestArgs struct {
	CatID       conceptual.CatID
	SourceName  string
	LayerName   string
	TippeConfig params.TippeConfigName
	JSONBytes   []byte
}

func (a *PushFeaturesRequestArgs) validate() error {
	if a.CatID == "" {
		return fmt.Errorf("missing cat ID")
	}
	if a.SourceName == "" {
		return fmt.Errorf("missing source name")
	}
	if a.LayerName == "" {
		return fmt.Errorf("missing layer name")
	}
	if a.TippeConfig == "" {
		return fmt.Errorf("missing tippe config")
	}
	if len(a.JSONBytes) == 0 {
		return fmt.Errorf("missing features")
	}
	return nil
}

func (d *Daemon) writeGZ(f *flat.Flat, args *PushFeaturesRequestArgs, name string) error {

	wr, err := f.NamedGZWriter(name)
	if err != nil {
		return err
	}

	//if err := syscall.Flock(int(g.f.Fd()), syscall.LOCK_EX); err != nil {
	//	panic(err)
	//}

	if _, err := wr.Writer().Write(args.JSONBytes); err != nil {
		return err
	}
	if err := wr.Close(); err != nil {
		return err
	}
	return nil
}

func (d *Daemon) PushFeatures(args *PushFeaturesRequestArgs, reply *PushFeaturesResponse) error {
	d.logger.Info("PushFeatures", "cat", args.CatID, "source", args.SourceName, "layer", args.LayerName)
	if err := args.validate(); err != nil {
		slog.Warn("PushFeatures invalid args", "error", err)
		return err
	}

	// Store features in flat gzs.
	sourceFlat := flat.NewFlatWithRoot(d.flat.Path()).Joining("source", args.CatID.String())
	if err := sourceFlat.MkdirAll(); err != nil {
		return err
	}

	if err := d.writeGZ(sourceFlat, args, args.SourceName+".geojson.gz"); err != nil {
		return err
	}

	// Notify tiler daemon to tile the source file.

	return nil
}

// enqueue registers unique source files for tiling.
func (d *Daemon) enqueue(source string) {
	if _, ok := d.tilingQueue.Load(source); !ok {
		d.tilingQueue.Store(source, struct{}{})
		d.tilingPending.Add(1)
	}
}

// unqueue unregisters a tilingPending source file.
func (d *Daemon) unqueue(source string) {
	d.tilingQueue.Delete(source)
	d.tilingPending.Done()
}

type PushFeaturesResponse struct {
	Success bool
}

type TilingRequestArgs struct {
	CatID    conceptual.CatID
	SourceGZ string
	Config   params.TippeConfigName
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
	case params.TippeConfigNameTracks:
		cliConfig = params.DefaultTippeConfigs.Tracks()
	case params.TippeConfigNameSnaps:
		// TODO: Implement snaps
		cliConfig = params.DefaultTippeConfigs.Tracks()
	case params.TippeConfigNameLaps:
		cliConfig = params.DefaultTippeConfigs.Laps()
	case params.TippeConfigNameNaps:
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
