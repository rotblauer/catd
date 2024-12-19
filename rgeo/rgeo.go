package rgeo

import (
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

//var R_Cities *rgeo.Rgeo
//var R_Countries *rgeo.Rgeo
//var R_Provinces *rgeo.Rgeo
//var R_US_Counties *rgeo.Rgeo

type ReverseGeocoder interface {
	GetLocation(pt orb.Point) (srgeo.Location, error)
	GetGeometry(pt orb.Point, dataset string) (orb.Geometry, error)
}

// rR is the type of our wrapped rgeo.Rgeo instance, which implements the ReverseGeocoder interface.
type rR srgeo.Rgeo

// r is the instance of our wrapped rgeo.Rgeo instance.
var r *rR

func (rr *rR) GetLocation(pt orb.Point) (srgeo.Location, error) {
	return (*srgeo.Rgeo)(rr).ReverseGeocode(pt)
}

func (rr *rR) GetGeometry(pt orb.Point, dataset string) (orb.Geometry, error) {
	return (*srgeo.Rgeo)(rr).GetGeometry(pt, dataset)
}

// rRPC is an instance of configuration information for a remote RgeoD instance.
// If this value exists, and rR is nil, then R(dataset) will attempt
// to connect to the remote RgeoD instance rather than use the inproc value.
var rRPC = params.InProcRgeoDaemonConfig

// R gets a ReverseGeocoder instance.
// This can be an inproc instance or an RPC instance;
// an inproc instance will be preferred if it exists (has been Init-ed, see below),
// otherwise an attempt will be made to establish any configured RPC conn.
//
// *Rgeo contains indices representing collection of datasets.
// These datasets will be/can be reduced/cross-referenced to a single Location value.
// This function accepts a parameter which is not currently used,
// but which suggests that dataset services might be differentiated in the future.
func R(dataset string) ReverseGeocoder {
	if r != nil {
		return r
	}
	if rRPC != nil {
		rgc, err := NewRPCReverseGeocoder(rRPC)
		if err == nil {
			return rgc
		}
		slog.Error("Failed to connect to remote rgeo daemon", "error", err)
	}
	panic("R (rgeo) called, but not initialized or available")
}

type RPCReverseGeocoder struct {
	config *params.RgeoDaemonConfig
	client *rpc.Client
}

func NewRPCReverseGeocoder(config *params.RgeoDaemonConfig) (*RPCReverseGeocoder, error) {
	if config == nil {
		config = params.InProcRgeoDaemonConfig
	}
	client, err := common.DialRPC(config.Network, config.Address)
	if err != nil {
		return nil, err
	}
	return &RPCReverseGeocoder{config: config, client: client}, nil
}

func (r *RPCReverseGeocoder) GetLocation(pt orb.Point) (srgeo.Location, error) {
	var loc srgeo.Location
	res := &GetLocationResponse{}
	err := r.client.Call("ReverseGeocode", &GetLocationRequest{pt.Lon(), pt.Lat()}, res)
	if err != nil {
		return loc, err
	}
	return res.Location, res.Error
}

func (r *RPCReverseGeocoder) GetGeometry(pt orb.Point, dataset string) (orb.Geometry, error) {
	var geom = new(orb.Geometry)
	res := &GetGeometryResponse{}
	err := r.client.Call("GetGeometry", &GetGeometryRequest{Pt(pt), dataset}, res)
	if err != nil {
		return nil, err
	}
	return *geom, res.Error
}

func (r *RPCReverseGeocoder) Close() error {
	return r.client.Close()
}

var (
	Cities10      = srgeo.Cities10
	Countries10   = srgeo.Countries10
	Provinces10   = srgeo.Provinces10
	US_Counties10 = srgeo.US_Counties10
)

// datasets are the datasets that the reverse geocoder will use.
var datasets = []func() []byte{
	Cities10,
	Countries10,
	Provinces10,
	US_Counties10,
}

const dataSourcePre = "github.com/sams96/rgeo."

var DatasetNamesStable = []string{}

func init() {
	for _, d := range datasets {
		DatasetNamesStable = append(DatasetNamesStable, common.ReflectFunctionName(d))
	}
	sort.Slice(DatasetNamesStable, func(i, j int) bool {
		return DatasetNamesStable[i] < DatasetNamesStable[j]
	})
	initTilingZoomLevels()
}

var ErrAlreadyInitialized = fmt.Errorf("rgeo already initialized")

func Init() error {
	if r != nil {
		return ErrAlreadyInitialized
	}

	r1, err := srgeo.New(datasets...)
	if err != nil {
		return err
	}
	r = (*rR)(r1)

	// Test: Assert that exported DatasetNamesStable matches actual loaded.
	names := r1.DatasetNames()
	if !slices.Equal(DatasetNamesStable, names) {
		return fmt.Errorf("DatasetNamesStable does not match actual")
	}
	return nil
}

type Pt [2]float64
