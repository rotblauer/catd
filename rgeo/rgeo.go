package rgeo

import (
	"errors"
	"fmt"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/params"
	srgeo "github.com/sams96/rgeo"
	"log/slog"
	"net/rpc"
	"slices"
	"sort"
)

// R gets a ReverseGeocoder instance.
// This can be an inproc instance or an RPC instance;
// an inproc instance will be preferred if it exists (has been Init-ed, see below),
// otherwise an attempt will be made to establish any configured RPC conn.
//
// *Rgeo contains indices representing collection of datasets.
// These datasets will be/can be reduced/cross-referenced to a single Location value.
// This function accepts a parameter which is not currently used,
// but which suggests that dataset services might be differentiated in the future.
func R(datasets ...string) ReverseGeocoder {
	// Attempt to re-use any existing global services.

	// rRPC is a poor idea in hindsight.
	// It's a global, it's not thread-safe, and the RPC client
	// would need refreshing to avoid Gob memory leaks.
	//
	//if rRPC != nil {
	//	// RPC clients are relatively disposable.
	//	// In fact, should be Closed regularly. (Avoid Gob mem leaks).
	//	if rRPC.errored.Load() {
	//		slog.Warn("RPC client errored, closing, attempting to re-init")
	//		rRPC.Close()
	//		rRPC = nil
	//	}
	//	return rRPC
	//}

	// If an inproc service exists, return it.
	// Takes ~20s to init, works great otherwise.
	if r != nil {
		return r
	}

	// If a (default) configuration exist for an RPC client,
	// attempt to connect to a/any/some remote rgeo daemon.
	// If this works, there's an catd rgeod running externally.
	// The new client returned will probably be a one-time-use instance.
	if rRPCConf != nil {
		rgc, err := NewRPCReverseGeocoderClient(rRPCConf)
		if err == nil {
			// We found a working, remote RPC client.
			return rgc
		}
		slog.Warn("Failed to connect to remote rgeo daemon", "error", err)
	}

	// Everything failed. Load datasets.
	// Once datasets are loaded, there's no reason to go back to RPC.
	slog.Info("Loading rgeo datasets")
	err := Init(defaultDatasets...)
	if err != nil {
		panic(err)
	}
	return r
}

// ReverseGeocoder defines an interface shared between an inproc service
// or a remote RPC service. These are the catd application interface needs.
type ReverseGeocoder interface {
	GetLocation(pt Pt) (srgeo.Location, error)
	GetGeometry(pt Pt, dataset string) (*Plat, error)
	// TODO Expose Datasets() []string? []func()[]byte?
}

// r is the instance of our inproc, wrapped, rgeo.Rgeo instance,
// which implements the defined interface.
var r *rR

// rR is the type of our wrapped rgeo.Rgeo instance, which implements the ReverseGeocoder interface.
type rR srgeo.Rgeo

func (rr *rR) GetLocation(pt Pt) (srgeo.Location, error) {
	opt := orb.Point{pt[0], pt[1]}
	return (*srgeo.Rgeo)(rr).ReverseGeocode(opt)
}

func (rr *rR) GetGeometry(pt Pt, dataset string) (*Plat, error) {
	opt := orb.Point{pt[0], pt[1]}
	geo, err := (*srgeo.Rgeo)(rr).GetGeometry(opt, dataset)
	if err != nil {
		return nil, err
	}
	poly, ok := geo.(orb.Polygon)
	if ok {
		out := make(orb.Polygon, len(poly))
		for i, ring := range poly {
			out[i] = make(orb.Ring, len(ring))
			for j, pt := range ring {
				out[i][j] = orb.Point{pt[0], pt[1]}
			}
		}
		p := &Plat{Polygon: out}
		return p, nil
	}
	multiPoly, ok := geo.(orb.MultiPolygon)
	if ok {
		out := make(orb.MultiPolygon, len(multiPoly))
		for i, poly := range multiPoly {
			out[i] = make([]orb.Ring, len(poly))
			for j, ring := range poly {
				out[i][j] = make([]orb.Point, len(ring))
				for k, pt := range ring {
					out[i][j][k] = orb.Point{pt[0], pt[1]}
				}
			}
		}
		p := &Plat{MultiPolygon: out}
		return p, nil
	}
	return nil, fmt.Errorf("expected Polygon or MultiPolygon, got %T", geo)
}

// rRPCConf is an instance of configuration information for a remote RgeoD instance.
// If this value exists, and rR is nil, then R(dataset) will attempt
// to connect to the remote RgeoD instance rather than use the inproc value.
var rRPCConf = params.InProcRgeoDaemonConfig

// rRPC is an instance of RPCReverseGeocoderClient which fulfills the interface
// via attempts, or attempts to (using the configuration defined by rRPCConf).
//var rRPC *RPCReverseGeocoderClient

type RPCReverseGeocoderClient struct {
	config *params.RgeoDaemonConfig
	client *rpc.Client

	// receiver is the string value of the receiver name,
	// for rpc request method standards, ie. `receiver.method_name` or `Receiver.MethodName`.
	receiver string
}

func NewRPCReverseGeocoderClient(config *params.RgeoDaemonConfig) (*RPCReverseGeocoderClient, error) {
	if config == nil {
		config = params.InProcRgeoDaemonConfig
	}
	client, err := common.DialRPC(config.Network, config.Address)
	if err != nil {
		return nil, err
	}
	return &RPCReverseGeocoderClient{
		config: config,
		client: client,
		// FIXME Real world, this must be same as service name in daemon.go.
		// Test world, this must be `MockRPCServer`.
		receiver: config.ServiceName,
	}, nil
}

func (r *RPCReverseGeocoderClient) GetLocation(pt Pt) (loc srgeo.Location, err error) {
	defer r.Close()
	res := &GetLocationResponse{}
	err = r.client.Call(r.receiver+".GetLocation", &pt, res)
	if err != nil {
		return loc, err
	}
	if res.Error != "" {
		return loc, errors.New(res.Error)
	}
	return res.Location, nil
}

func (r *RPCReverseGeocoderClient) GetGeometry(pt Pt, dataset string) (*Plat, error) {
	defer r.Close()
	res := &GetGeometryResponse{}
	err := r.client.Call(r.receiver+".GetGeometry", &GetGeometryRequest{pt, dataset}, res)
	if err != nil {
		return nil, err
	}
	if res.Error != "" {
		return nil, errors.New(res.Error)
	}
	return res.Plat, nil
}

func (r *RPCReverseGeocoderClient) Close() error {
	if r.client == nil {
		return nil
	}
	return r.client.Close()
}

var (
	Cities10      = srgeo.Cities10
	Countries10   = srgeo.Countries10
	Provinces10   = srgeo.Provinces10
	US_Counties10 = srgeo.US_Counties10
)

// defaultDatasets are the datasets that the reverse geocoder will use all the time.
var defaultDatasets = []func() []byte{
	Cities10,
	Countries10,
	Provinces10,
	US_Counties10,
}

var DatasetNamesStable = []string{}

func init() {
	doInit()
}

// doInit is a temporary, defaults-only pseudo function.
// FIXME
func doInit() {
	DatasetNamesStable = []string{}
	for _, d := range defaultDatasets {
		DatasetNamesStable = append(DatasetNamesStable, common.ReflectFunctionName(d))
	}
	sort.Slice(DatasetNamesStable, func(i, j int) bool {
		return DatasetNamesStable[i] < DatasetNamesStable[j]
	})
	initTilingZoomLevels()
}

func Init(datasets ...func() []byte) error {
	defer doInit()
	if r != nil {
		return ErrAlreadyInitialized
	}
	if len(datasets) == 0 {
		datasets = defaultDatasets
	}
	r1, err := srgeo.New(datasets...)
	if err != nil {
		return err
	}
	r = (*rR)(r1)

	// Assert that exported DatasetNamesStable matches actual loaded.
	doInit()
	names := r1.DatasetNames()
	if !slices.Equal(DatasetNamesStable, names) {
		return fmt.Errorf("DatasetNamesStable does not match actual, Expected/Got\n%v\n%v", DatasetNamesStable, names)
	}
	return nil
}

const dataSourcePre = "github.com/sams96/rgeo."

var ErrAlreadyInitialized = fmt.Errorf("rgeo already initialized")
