package tiler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dustin/go-humanize"
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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Daemon struct {
	Config *params.DaemonConfig

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
	tilingRunningM sync.Map
	writeLock      sync.Map
	debouncersMu   sync.Mutex
	running        sync.WaitGroup
	requestIDIndex int32

	Done      chan struct{}
	Interrupt chan struct{}
}

func NewDaemon(config *params.DaemonConfig) *Daemon {
	if config == nil {
		config = params.DefaultDaemonConfig()
	}

	return &Daemon{
		Config:         config,
		flat:           flat.NewFlatWithRoot(config.RootDir),
		logger:         slog.With("daemon", "tiler"),
		debouncers:     make(map[string]func(func())),
		tilingPendingM: sync.Map{},
		tilingRunningM: sync.Map{},
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

	server.HandleHTTP(d.Config.RPCPath, rpc.DefaultDebugPath)
	l, err := net.Listen(d.Config.RPCNetwork, d.Config.RPCAddress)
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

	cancelRunPending := make(chan struct{})
	go d.schedulePendingRequests(cancelRunPending)

	go func() {
		defer func() {
			d.Done <- struct{}{}
			close(d.Done)
		}()
		d.logger.Info("TilerDaemon RPC HTTP server started",
			slog.Group("listen", "network", d.Config.RPCNetwork, "address", d.Config.RPCAddress))

		for {
			select {
			case <-d.Interrupt:

				// Running pending tiling is important for `import` porting in.
				d.logger.Info("TilerDaemon interrupted", "awaiting", "pending")
				cancelRunPending <- struct{}{}
				d.running.Wait()
				d.logger.Info("TilerDaemon interrupted", "awaiting", "final sync run")
				d.runPendingRequests(false, true) // clean up, blocking
				d.running.Wait()
				return
			}
		}
	}()
	return nil
}

func (d *Daemon) runPendingRequests(scheduling, sync bool) {
	d.tilingPendingM.Range(func(key, value any) bool {
		args := value.(*TilingRequestArgs)
		if scheduling {
			if time.Since(args.requestedAt) < d.Config.DebounceInterval {
				return true // continue
			}
			if _, ok := d.tilingRunningM.Load(args.id()); ok {
				return true // continue, already running (save pending)
			}
		}

		d.logger.Info("Running pending tiling", "args", args.id())
		d.unpending(args)

		if !sync {
			go func() {
				if err := d.callTiling(args, nil); err != nil {
					d.logger.Error("Failed to run pending tiling", "error", err)
				}
			}()
		} else {
			if err := d.callTiling(args, nil); err != nil {
				d.logger.Error("Failed to run pending tiling", "error", err)
				return false
			}
		}
		return true
	})
}

func (d *Daemon) schedulePendingRequests(cancel <-chan struct{}) {
	ticker := time.NewTicker(d.Config.DebounceInterval)
	for range ticker.C {
		d.runPendingRequests(true, false)
		select {
		case <-cancel:
			ticker.Stop()
			return
		default:
		}
	}
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

	// requestID is a unique identifier for the request.
	// It is used to provide a consistent file name for temporary files.
	requestID int32
}

func (s SourceSchema) id() string {
	return fmt.Sprintf("%s/%s/%s", s.CatID, s.SourceName, s.LayerName)
}

func (d *Daemon) SourcePathFor(schema SourceSchema, version TileSourceVersion) (string, error) {
	root := d.flat.Path()
	var out string
	switch version {
	case SourceVersionCanonical:
		out = filepath.Join(root, "source", schema.CatID.String(), schema.SourceName, schema.LayerName) + ".geojson.gz"
	case SourceVersionEdge:
		out = filepath.Join(root, "source", schema.CatID.String(), schema.SourceName, schema.LayerName) + "_edge.geojson.gz"
	case sourceVersionBackup:
		out = filepath.Join(root, "source", schema.CatID.String(), schema.SourceName, schema.LayerName) + "_edge.bak.geojson.gz"
	default:
		panic(fmt.Sprintf("unknown source version %q", version))
	}
	clean := filepath.Clean(out)
	return filepath.Abs(clean)
}

// TargetPathFor returns the final output path for some source schema and version.
// It will have a .mbtiles extension
func (d *Daemon) TargetPathFor(schema SourceSchema, version TileSourceVersion) (string, error) {
	source, err := d.SourcePathFor(schema, version)
	if err != nil {
		return "", err
	}
	// base=root/source  rel=catid/source/layer/...geojson.gz
	base := filepath.Join(d.flat.Path(), "source")
	rel, err := filepath.Rel(base, source)
	if err != nil {
		return "", err
	}
	rel = strings.TrimSuffix(rel, ".geojson.gz")
	rel = strings.TrimSuffix(rel, ".json.gz")
	rel = strings.TrimSuffix(rel, ".gz")
	rel += ".mbtiles"
	return filepath.Join(d.flat.Path(), "tiles", rel), nil
}

// TmpTargetPathFor returns a deterministic temporary target path for a source schema and version.
// This value (filepath) is not safe for use by concurrent processes.
func (d *Daemon) TmpTargetPathFor(schema SourceSchema, version TileSourceVersion) (string, error) {
	target, err := d.TargetPathFor(schema, version)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(d.flat.Path(), target)
	if err != nil {
		return "", err
	}
	full := filepath.Join(d.Config.TmpDir, rel)
	//full += fmt.Sprintf(".%d", schema.requestID)
	return full, nil
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
	Success   bool
	WrittenTo string
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
func (d *Daemon) pending(args *TilingRequestArgs) (last *TilingRequestArgs) {
	value, exists := d.tilingPendingM.Load(args.id())
	d.tilingPendingM.Store(args.id(), args)
	if exists {
		return value.(*TilingRequestArgs)
	}
	return nil
}

// unpending unregisters a pending source file tile-request call.
func (d *Daemon) unpending(args *TilingRequestArgs) {
	d.tilingPendingM.Delete(args.id())
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

func (d *Daemon) rollEdgeToBackup(args *TilingRequestArgs) error {
	backupPath, _ := d.SourcePathFor(args.SourceSchema, sourceVersionBackup)

	edgePath, _ := d.SourcePathFor(args.SourceSchema, SourceVersionEdge)
	matches, err := filepath.Glob(edgePath + "*")
	if err != nil {
		return err
	}

	// Maintain lexical order.
	sort.Strings(matches)

	// Open the backup path for writing, truncating it.
	// Append-only. gzipped.
	if err := os.MkdirAll(filepath.Dir(backupPath), os.ModePerm); err != nil {
		return err
	}
	backup, err := os.OpenFile(backupPath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE|os.O_APPEND, 0660)
	if err != nil {
		return err
	}
	defer backup.Close()

	// Iterate the matches and copy them into the target backup file.
	for _, match := range matches {
		f, err := os.Open(match)
		if err != nil {
			return err
		}
		defer f.Close() // in case copy errors
		if _, err := io.Copy(backup, f); err != nil {
			return err
		}
		f.Close()
	}
	return nil
}

func (d *Daemon) PushFeatures(args *PushFeaturesRequestArgs, reply *PushFeaturesResponse) error {
	if args == nil {
		return fmt.Errorf("nil args")
	}

	d.logger.Info("PushFeatures", "cat", args.CatID, "source", args.SourceName, "layer", args.LayerName)

	if err := args.validate(); err != nil {
		slog.Warn("PushFeatures invalid args", "error", err)
		return err
	}

	args.requestID = atomic.AddInt32(&d.requestIDIndex, 1)

	for _, version := range []TileSourceVersion{SourceVersionCanonical, SourceVersionEdge} {
		source, err := d.SourcePathFor(args.SourceSchema, version)
		if err != nil {
			return fmt.Errorf("failed to get source path: %w", err)
		}

		// Append a glob-ready, lexical-ordering suffix to the edge file.
		// One edge file is written per request.
		// This keeps the data atomic.
		if version == SourceVersionEdge {
			source += fmt.Sprintf(".%014d", time.Now().UnixNano())
		}

		// Ensure dirs.
		if err := os.MkdirAll(filepath.Dir(source), os.ModePerm); err != nil {
			return err
		}

		b := make([]byte, len(args.JSONBytes))
		copy(b, args.JSONBytes)
		if err := d.writeGZ(source, b); err != nil {
			return err
		}
		d.logger.Info("Wrote source", "source", source, "size", humanize.Bytes(uint64(len(args.JSONBytes))))
	}

	// Request edge tiling. Will get debounced.
	res := &TilingResponse{
		Elapsed: time.Duration(666),
	}
	err := d.RequestTiling(&TilingRequestArgs{
		SourceSchema: SourceSchema{
			CatID:      args.CatID,
			SourceName: args.SourceName,
			LayerName:  args.LayerName,
		},
		Version:     SourceVersionEdge,
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
	sourceVersionBackup    TileSourceVersion = "backup"
)

type TilingRequestArgs struct {
	SourceSchema

	TippeConfig params.TippeConfigName

	Version TileSourceVersion

	// parsedSourcePath is the source file path, parsed and validated.
	parsedSourcePath string

	requestedAt time.Time
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

var ErrEmptyFile = errors.New("empty file (nothing to tile)")
var ErrNoFiles = errors.New("no files (nothing to tile)")

func validateSourcePathFile(source string, version TileSourceVersion) error {
	// Edge handling is different because we're checking groups of files with glob.
	if version == SourceVersionEdge {
		matches, err := filepath.Glob(source + "*")
		if err != nil {
			return err
		}
		if len(matches) == 0 {
			return ErrNoFiles
		}
		return nil
	}

	// Check that the file exists, is a file, and is not empty.
	stat, err := os.Stat(source)
	if err != nil {
		return fmt.Errorf("source file not found: %w", err)
	}
	if stat.IsDir() {
		return fmt.Errorf("source file is a directory")
	}
	if stat.Size() == 0 {
		return ErrEmptyFile
	}
	return nil
}

// RequestTiling requests tiling of a source file.
// It uses a combination of debouncing and enqueuing to cause it to run
// at most end-to-end per source file. (Once it finishes, it can be run again.)
func (d *Daemon) RequestTiling(args *TilingRequestArgs, reply *TilingResponse) error {
	if args == nil {
		return fmt.Errorf("nil args")
	}

	if err := args.Validate(); err != nil {
		return fmt.Errorf("invalid args: %w", err)
	}

	args.requestID = atomic.AddInt32(&d.requestIDIndex, 1)
	args.requestedAt = time.Now()

	d.logger.Info("Requesting tiling",
		slog.Group("args",
			"cat", args.CatID, "source", args.SourceName, "layer", args.LayerName,
			"config", args.TippeConfig))

	source, err := d.SourcePathFor(args.SourceSchema, args.Version)
	if err != nil {
		return fmt.Errorf("failed to get source path: %w", err)
	}

	// Check that the file exists, is a file, and is not empty.
	if err := validateSourcePathFile(source, args.Version); err != nil {

		// But backup files are allowed to be empty.
		if errors.Is(err, ErrEmptyFile) && args.Version == sourceVersionBackup {
			d.logger.Warn("RequestTiling empty backup file", "source", source)
			return nil
		}
		return err
	}

	// If the source is already enqueued for tiling
	// it is either waiting to run or currently running.
	// Short circuit.

	// Queue the source and call.
	d.pending(args)

	return nil
}

func (d *Daemon) callTiling(args *TilingRequestArgs, reply *TilingResponse) error {
	d.running.Add(1)
	defer d.running.Done()

	d.tilingRunningM.Store(args.id(), args)
	defer d.tilingRunningM.Delete(args.id())

	if args == nil {
		return fmt.Errorf("nil args")
	}

	source, err := d.SourcePathFor(args.SourceSchema, args.Version)
	if err != nil {
		return fmt.Errorf("failed to get source path: %w", err)
	}

	args.parsedSourcePath = source

	if reply == nil {
		reply = &TilingResponse{}
	}

	// If we're about to run tippe for the canonical data set,
	// move the edge data to a backup, since we're about to process
	// a canonical version which includes all this data.
	// TODO: Handle canon run failure - restore backup backup.
	if args.Version == SourceVersionCanonical {
		if err := d.rollEdgeToBackup(args); err != nil {
			d.logger.Error("Failed to roll edge files to backup", "error", err)
			return err
		}
	}

	// Actually do tippecanoe.
	if err := d.tiling(args, reply); err != nil {
		d.logger.Error("Failed to tile", "error", err)

		return err
	}

	// We can safely return now if this was canon;
	// there's no magic after the canon run.
	if args.Version == SourceVersionCanonical {
		return nil
	}

	// We want to trigger an edge->canonical run iff:
	// - the edge run took too long
	// - canonical tiles don't exist yet for the source
	var triggerCanon bool
	var triggerCanonReason string

	// If the canonical (output) file doesn't exist, we need to run it.
	canonMBTilesPath, _ := d.TargetPathFor(args.SourceSchema, SourceVersionCanonical)
	if _, err := os.Stat(canonMBTilesPath); os.IsNotExist(err) {
		triggerCanonReason = "does not exist yet"
		triggerCanon = true
	}

	if reply.Elapsed > d.Config.EdgeEpsilon {
		triggerCanonReason = fmt.Sprintf("edge tiling exceeded threshold epsilon=%v", d.Config.EdgeEpsilon.Round(time.Millisecond))
		triggerCanon = true
	}

	if !triggerCanon {
		// We're done here.
		return nil
	}

	d.logger.Info("Triggering canon tiling", "reason", triggerCanonReason, "source", source)

	return d.callTiling(&TilingRequestArgs{
		SourceSchema: SourceSchema{
			CatID:      args.CatID,
			SourceName: args.SourceName,
			LayerName:  args.LayerName,
		},
		Version:     SourceVersionCanonical,
		TippeConfig: args.TippeConfig,
	}, reply)
}

func (d *Daemon) tiling(args *TilingRequestArgs, reply *TilingResponse) error {
	d.logger.Info("callTiling", "source", args.parsedSourcePath, slog.Group("args",
		"cat", args.CatID, "source", args.SourceName, "layer", args.LayerName,
		"version", args.Version, "config", args.TippeConfig))

	// Sanity check.
	// The source file must exist and be a file.
	// It is possible, in the Edge case, that the source file does not exist.
	// This could happen if the canonical run just finished, and no new
	// features have been written to edge.
	if err := validateSourcePathFile(args.parsedSourcePath, args.Version); err != nil {
		return fmt.Errorf("tiling source file error: %w", err)
	}

	// Declare the final .mbtiles output target filepath.
	target, err := d.TargetPathFor(args.SourceSchema, args.Version)
	if err != nil {
		d.logger.Error("Failed to get target path", "error", err)
	}

	// Code below runs:
	// mkdir -p /tmp/tiler-daemon/catid
	// time tippecanoe --layer=... --name=... --output=/tmp/tiler-daemon/catid/laps.mbtiles /datadir/catid/laps.geojson.gz
	// mv /tmp/tiler-daemon/catid/laps.mbtiles /datadir/tiles/catid/laps.mbtiles
	// mkdir -p /datadir/tiles/catid
	// mv /tmp/tiler-daemon/catid/laps.mbtiles /datadir/tiles/catid/laps.mbtiles

	// Use and then rename a temporary output file to avoid
	// overwriting/deleting/modifying the final output file
	// - in case of failure
	// - in case the final output is being used by another process

	// Give tmp target a unique name so it doesn't get fucked with.
	mbtilesTmp, err := d.TmpTargetPathFor(args.SourceSchema, args.Version)
	if err != nil {
		d.logger.Error("Failed to get tmp target path", "error", err)
		return err
	}
	mbtilesTmp = mbtilesTmp + fmt.Sprintf(".%d", time.Now().UnixNano())

	if err := os.MkdirAll(filepath.Dir(mbtilesTmp), os.ModePerm); err != nil {
		d.logger.Error("Failed to create temp dir", "error", err)
		return err
	}

	cliConfig, ok := params.LookupTippeConfig(args.TippeConfig)
	if !ok {
		return fmt.Errorf("unknown tippe config %q", args.TippeConfig)
	}
	cliConfig.MustSetPair("--layer", args.LayerName).
		MustSetPair("--name", args.SourceName).
		MustSetPair("--output", mbtilesTmp)

	// If we're running Edge and the backup Edge source exists, use it too.
	// This way we're able to serve WIP-tiling canonical data. (async)
	sources := []string{args.parsedSourcePath}
	if args.Version == SourceVersionEdge {
		sources, err = filepath.Glob(args.parsedSourcePath + "*")
		if err != nil {
			d.logger.Error("Failed to glob edge source(s)", "error", err)
			return err
		}
		// Append the backup (rolled) path to the list if it exists and is not empty.
		edgeBackupPath, _ := d.SourcePathFor(args.SourceSchema, sourceVersionBackup)
		bak, err := os.Stat(edgeBackupPath)
		if err == nil && bak.Size() > 0 {
			sources = append(sources, edgeBackupPath)
		}
	}

	// Run tippecanoe!
	start := time.Now()
	if err := d.tip(cliConfig, sources...); err != nil {
		d.logger.Error("Failed to tip", "error", err)
		return err
	}
	elapsed := time.Since(start)

	if err := os.MkdirAll(filepath.Dir(target), os.ModePerm); err != nil {
		d.logger.Error("Failed to create final dir", "error", err)
		return err
	}

	if err := os.Rename(mbtilesTmp, target); err != nil {
		d.logger.Error("Failed to move tmp to final", "error", err)
		return err
	}

	d.logger.Info("Tiling done", "source", args.parsedSourcePath, "target", target,
		"version", args.Version, "elapsed", elapsed.Round(time.Millisecond))

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
