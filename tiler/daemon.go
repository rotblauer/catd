package tiler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/bep/debounce"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"io"
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

	logger         *slog.Logger
	debouncers     map[string]func(func())
	tilingPendingM sync.Map
	writeLock      sync.Map
	debouncersMu   sync.Mutex
	running        sync.WaitGroup

	Done      chan struct{}
	Interrupt chan struct{}
}

type DaemonConfig struct {
	RootDir          string
	DebounceInterval time.Duration

	// EdgeEpsilon is the time threshold for edge tiling to complete.
	// If edge tiling takes longer than this, the canonical source is tiled.
	EdgeEpsilon time.Duration
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
		EdgeEpsilon:      1 * time.Minute,
	}
}

func NewDaemon(config *DaemonConfig) *Daemon {
	if config == nil {
		config = DefaultDaemonConfig()
	}

	return &Daemon{
		Config:         config,
		flat:           flat.NewFlatWithRoot(config.RootDir),
		logger:         slog.With("daemon", "tiler"),
		debouncers:     make(map[string]func(func())),
		tilingPendingM: sync.Map{},
		writeLock:      sync.Map{},

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
			d.logger.Error("TileDaemon RPC HTTP serve error", "error", err)
			os.Exit(1)
		}
		d.logger.Info("TilerDaemon RPC HTTP server stopped")
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
				// Running pending tiling is important for `import` porting in.
				d.logger.Info("TilerDaemon interrupted", "awaiting", "pending")
				d.tilingPendingM.Range(func(key, value any) bool {
					args := value.(*TilingRequestArgs)
					slog.Warn("Running pending tiling",
						slog.Group("args", "cat", args.CatID, "source", args.SourceName, "config", args.LayerName))
					if err := d.doTiling(args, nil); err != nil {
						slog.Error("Failed to run pending tiling", "error", err)
					}
					return true
				})
				d.running.Wait()
				return
			}
		}
	}()
	return nil
}

type SourceSchema struct {
	CatID conceptual.CatID

	// SourceName is the name of the source file, the mbtiles file, and the tileset name.
	// Should be like 'laps', 'naps', or 'snaps'.
	// It will be used as a base name.
	SourceName string

	// LayerName is the name of the layer in the .mbtiles file.
	// Currently this is passed to tippecanoe --layer name.
	// Only one layer is supported per source file. FIXME?
	LayerName string

	// version conventionalizes how this daemon handles source versions, re: tiling.
	version TileSourceVersion
}

func (d *Daemon) SourcePathFor(schema SourceSchema, version TileSourceVersion) (string, error) {
	root := d.flat.Path()
	var out string
	switch version {
	case SourceVersionCanonical:
		out = filepath.Join(root, "source", schema.CatID.String(), schema.SourceName, schema.LayerName) + ".geojson.gz"
	case SourceVersionEdge:
		out = filepath.Join(root, "source", schema.CatID.String(), schema.SourceName, schema.LayerName) + "_edge.geojson.gz"
	default:
		panic(fmt.Sprintf("unknown source version %q", version))
	}
	clean := filepath.Clean(out)
	return filepath.Abs(clean)
}

func (d *Daemon) TargetPathFor(schema SourceSchema) (string, error) {
	root := d.flat.Path()
	out := filepath.Join(root, "tiles", schema.CatID.String(), schema.SourceName, schema.LayerName) + ".mbtiles"
	clean := filepath.Clean(out)
	return filepath.Abs(clean)
}

func (s SourceSchema) attributes() []string {
	return []string{"catID", s.CatID.String(), "source", s.SourceName, "layr", s.LayerName}
}

type PushFeaturesRequestArgs struct {
	SourceSchema

	// TippeConfig refers to a named default (or otherwise available?) tippecanoe configuration.
	// Should be like 'laps', 'naps',...
	// These might be generalized to linestrings, points, etc.,
	// but either way its arbitrary.
	TippeConfig params.TippeConfigName

	// JSONBytes is data to be written to the source file.
	// It will be written to a .geojson.gz file.
	// It must be JSON, obviously.
	JSONBytes []byte
}

type PushFeaturesResponse struct {
	Success bool
}

func (a *PushFeaturesRequestArgs) validate() error {
	if a.CatID == "" {
		return fmt.Errorf("missing cat ID")
	}
	if a.SourceName == "" {
		return fmt.Errorf("missing source name")
	}
	if strings.Contains(a.SourceName, string(filepath.Separator)) {
		return fmt.Errorf("source name contains path separator")
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

// pending registers unique source files for tiling.
// Returns true if was not pending before call (and is added to queue).
// Args from the last all are persisted.
func (d *Daemon) pending(source string, args *TilingRequestArgs) (last *TilingRequestArgs) {
	value, exists := d.tilingPendingM.Load(source)
	d.tilingPendingM.Store(source, args)
	if exists {
		return value.(*TilingRequestArgs)
	}
	return nil
}

// unpending unregisters a pending source file tile-request call.
func (d *Daemon) unpending(source string) {
	d.tilingPendingM.Delete(source)
}

func (d *Daemon) writeGZ(source string, data []byte) error {
	wr, err := flat.NewFlatGZWriter(source)
	if err != nil {
		return err
	}
	defer wr.Close()

	// Decode JSON-lines data as a data-integrity validation,
	// then encode JSON lines gzipped to file.
	dec := json.NewDecoder(bytes.NewReader(data))
	for {
		var v interface{}
		if err := dec.Decode(&v); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := json.NewEncoder(wr.Writer()).Encode(v); err != nil {
			return err
		}
	}
	return nil
}

func (d *Daemon) PushFeatures(args *PushFeaturesRequestArgs, reply *PushFeaturesResponse) error {
	d.logger.Info("PushFeatures", "cat", args.CatID, "source", args.SourceName, "layer", args.LayerName)
	if err := args.validate(); err != nil {
		slog.Warn("PushFeatures invalid args", "error", err)
		return err
	}

	for _, version := range []TileSourceVersion{SourceVersionCanonical, SourceVersionEdge} {
		source, err := d.SourcePathFor(args.SourceSchema, version)
		if err != nil {
			return fmt.Errorf("failed to get source path: %w", err)
		}
		if err := os.MkdirAll(filepath.Dir(source), os.ModePerm); err != nil {
			return err
		}
		if err := d.writeGZ(source, args.JSONBytes); err != nil {
			return err
		}
	}

	// Request tiling. Will get debounced.
	res := &TilingResponse{
		Elapsed: time.Duration(666),
	}
	err := d.RequestTiling(&TilingRequestArgs{
		SourceSchema: SourceSchema{
			CatID:      args.CatID,
			SourceName: args.SourceName,
			LayerName:  args.LayerName,
			version:    SourceVersionEdge,
		},
		TippeConfig: args.TippeConfig,
	}, res)

	d.logger.Info("PushFeatures done", "cat", args.CatID, "source", args.SourceName, "layer", args.LayerName, "error", err)

	return err
}

// TileSourceVersion is a name of a version of the source file to write, and to tile.
// Versions are (called) canonical and edge.
// The canonical source contains all the data.
// The edge source contains a subset of the data: the latest data.
// This scheme addresses the potential for (very) long-running canonical-data tiling processes
// because of potentially large source files.
// On a tiling request for some (canonical) data source, first, the edge source is tiled.
// If edge tiling takes longer than some threshold (or canonical tiles DNE), the canonical source is tiled.
// Everytime after the canonical source is tiled, the edge source is flushed (and re-tiled, tabula rasa).
// The edge epsilon threshold will be exceeded by the edge tiling process (probably)
// because it may continue to tile on new Requests and won't flush until the canonical tiling completes.
type TileSourceVersion string

const (
	SourceVersionCanonical TileSourceVersion = "canonical"
	SourceVersionEdge      TileSourceVersion = "edge"
)

type TilingRequestArgs struct {
	SourceSchema

	TippeConfig params.TippeConfigName

	// SkipCanon can be used to override the default version tiling behavior.
	SkipCanon bool
	// CanonOnly can be used to skip edge version tiling.
	CanonOnly bool
}

func (s SourceSchema) Validate() error {
	if s.CatID == "" {
		return fmt.Errorf("missing cat ID")
	}
	if s.SourceName == "" {
		return fmt.Errorf("missing source name")
	}
	if strings.Contains(s.SourceName, string(filepath.Separator)) {
		return fmt.Errorf("source name contains path separator")
	}
	if s.LayerName == "" {
		return fmt.Errorf("missing layer name")
	}
	if strings.Contains(s.LayerName, string(filepath.Separator)) {
		return fmt.Errorf("layer name contains path separator")
	}
	return nil
}

func (a *TilingRequestArgs) Validate() error {
	if err := a.SourceSchema.Validate(); err != nil {
		return err
	}
	if _, ok := params.LookupTippeConfig(a.TippeConfig); !ok {
		return fmt.Errorf("unknown tippe config %q", a.TippeConfig)
	}
	return nil
}

type TilingResponse struct {
	Success     bool
	Elapsed     time.Duration
	MBTilesPath string
}

// mbTilesBaseNameFromSourcePathGZ returns a convention-driven base name for .mbtiles files.
// It strips expected suffixes from the source file name and appends .mbtiles.
func mbTilesBaseNameFromSourcePathGZ(sourceGZ string) string {
	// Trim any and all extensions off the source file name,
	// then append .mbtiles extension.
	// This is the output file name.
	out := filepath.Base(sourceGZ)

	// Trim expected suffixes using a blacklist to avoid bad assumptions.
	blacklist := []string{".geojson", ".json", ".gz"}

trimming:
	for ext := filepath.Ext(out); ext != ""; ext = filepath.Ext(out) {
		for _, b := range blacklist {
			if ext == b {
				out = strings.TrimSuffix(out, b)
				continue trimming
			}
		}
	}

	out = out + ".mbtiles"
	return out
}

func validateSourcePathFile(source string) error {
	stat, err := os.Stat(source)
	if err != nil {
		return fmt.Errorf("source file not found: %w", err)
	}
	if stat.IsDir() {
		return fmt.Errorf("source file is a directory")
	}
	if stat.Size() == 0 {
		return fmt.Errorf("source file is empty")
	}
	return nil
}

// RequestTiling requests tiling of a source file.
// It uses a combination of debouncing and enqueuing to cause it to run
// at most end-to-end per source file. (Once it finishes, it can be run again.)
func (d *Daemon) RequestTiling(args *TilingRequestArgs, reply *TilingResponse) error {
	d.logger.Info("Requesting tiling",
		slog.Group("args",
			"cat", args.CatID, "source", args.SourceName, "layer", args.LayerName,
			"config", args.TippeConfig))

	source, err := d.SourcePathFor(args.SourceSchema, args.version)
	if err != nil {
		return fmt.Errorf("failed to get source path: %w", err)
	}

	if err := validateSourcePathFile(source); err != nil {
		return fmt.Errorf("invalid source file: %w", err)
	}

	// The debouncer prevents the first of multiple requests from being tiled.
	// It waits for some interval to see if more of the same requests are coming,
	// and calls the last of them. It waits for the interval to pass before executing any call.
	var debouncer func(func())
	d.debouncersMu.Lock()
	if f, ok := d.debouncers[source]; ok {
		debouncer = f
	} else {
		debouncer = debounce.New(5 * time.Second)
		d.debouncers[source] = debouncer
	}
	d.debouncersMu.Unlock()

	// If the source is already enqueued for tiling
	// it is either waiting to run or currently running.
	// Short circuit.

	// Queue the source and call.
	d.pending(source, args)

	// Short-circuiting means that the debouncer won't be called
	// if the source is already enqueued,
	// which means that the process will be allowed to run more-or-less in serial,
	// constantly while the requests keep coming in.
	// On the upside, any last run-overlapping requests will yield another run
	// if the process takes more than 5 seconds.
	// On the downside, it means that tippe will be running constantly
	// during an import, for example.
	// return

	debouncer(func() {
		reply := reply
		if err := d.doTiling(args, reply); err != nil {
			d.logger.Error("Tiling errored", "error", err)
		}
	})
	return nil
}

func (d *Daemon) doTiling(args *TilingRequestArgs, reply *TilingResponse) error {
	d.running.Add(1)

	source, _ := d.SourcePathFor(args.SourceSchema, args.version)

	defer func() {
		if value, ok := d.tilingPendingM.Load(source); ok {
			param := value.(*TilingRequestArgs)
			if err := d.RequestTiling(param, reply); err != nil {
				d.logger.Error("Failed to re-request pending tiling", "error", err)
			} else {
				d.logger.Debug("Re-requested pending tiling", "source", source)
			}
		}
	}()
	defer d.running.Done()

	d.unpending(source)

	d.logger.Info("doTiling", "source", source, slog.Group("schema",
		"cat", args.CatID, "source", args.SourceName, "layer", args.LayerName, "config", args.TippeConfig))

	target, err := d.TargetPathFor(args.SourceSchema)
	if err != nil {
		d.logger.Error("Failed to get target path", "error", err)
	}

	// Code below runs:
	// mkdir -p /tmp/tiler-daemon/catid
	// time tippecanoe --layer=... --name=... --output=/tmp/tiler-daemon/catid/laps.mbtiles /datadir/catid/laps.geojson.gz
	// mv /tmp/tiler-daemon/catid/laps.mbtiles /datadir/tiles/catid/laps.mbtiles
	// mkdir -p /datadir/tiles/catid
	// mv /tmp/tiler-daemon/catid/laps.mbtiles /datadir/tiles/catid/laps.mbtiles

	// args  tmpTarget string, cliConfig params.CLIFlagsT

	// Use and then rename a temporary output file to avoid
	// overwriting/deleting/modifying the final output file
	// - in case of failure
	// - in case the final output is being used by another process
	tmpTarget := filepath.Join(os.TempDir(), "catd-tilerd", "tiles_tmp",
		args.CatID.String(), args.SourceName, args.LayerName+"_"+string(args.version)+".mbtiles")
	if err := os.MkdirAll(filepath.Dir(tmpTarget), os.ModePerm); err != nil {
		slog.Error("Failed to create temp dir", "error", err)
		return err
	}

	cliConfig, ok := params.LookupTippeConfig(args.TippeConfig)
	if !ok {
		return fmt.Errorf("unknown tippe config %q", args.TippeConfig)
	}
	cliConfig.MustSetPair("--layer", args.LayerName).
		MustSetPair("--name", args.SourceName).
		MustSetPair("--output", tmpTarget)

	// Run tippecanoe!
	start := time.Now()
	if err := d.tip(source, cliConfig); err != nil {
		d.logger.Error("Failed to tip", "error", err)
		return err
	}
	elapsed := time.Since(start)

	if err := os.MkdirAll(filepath.Dir(target), os.ModePerm); err != nil {
		d.logger.Error("Failed to create final dir", "error", err)
		return err
	}

	if err := os.Rename(tmpTarget, target); err != nil {
		d.logger.Error("Failed to move tmp to final", "error", err)
	}

	d.logger.Info("Tiling done", "source", source, "target", target,
		"version", args.version, "elapsed", elapsed.Round(time.Millisecond))

	// Nobody is going to be waiting around long enough for this to be useful...
	if reply != nil {
		reply.Success = true
		reply.Elapsed = elapsed
		reply.MBTilesPath = target
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
