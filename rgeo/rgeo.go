package rgeo

import (
	"fmt"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/common"
	"github.com/sams96/rgeo"
	"slices"
	"sort"
)

//var R_Cities *rgeo.Rgeo
//var R_Countries *rgeo.Rgeo
//var R_Provinces *rgeo.Rgeo
//var R_US_Counties *rgeo.Rgeo

type ReverseGeocoder interface {
	GetLocation(pt orb.Point) (rgeo.Location, error)
	GetGeometry(pt orb.Point, dataset string) (orb.Geometry, error)
}

// rR is the type of our wrapped rgeo.Rgeo instance, which implements the ReverseGeocoder interface.
type rR rgeo.Rgeo

// r is the instance of our wrapped rgeo.Rgeo instance.
var r *rR

func (rr *rR) GetLocation(pt orb.Point) (rgeo.Location, error) {
	return (*rgeo.Rgeo)(rr).ReverseGeocode(pt)
}

func (rr *rR) GetGeometry(pt orb.Point, dataset string) (orb.Geometry, error) {
	return (*rgeo.Rgeo)(rr).GetGeometry(pt, dataset)
}

func R(dataset string) ReverseGeocoder {
	// if r == nil {
	// WARN: This takes forever. Init first.
	//
	// *Rgeo contains indices representing collection of datasets.
	// These datasets will be/can be reduced/cross-referenced to a single Location value.
	// This function accepts a parameter which is not currently used,
	// but which suggests that dataset services might be differentiated in the future.

	// Another thing.

	return r
}

//type RPCReverseGeocoder struct {
//	client *rpc.Client
//}
//
//func NewRPCReverseGeocoder() (*RPCReverseGeocoder, error) {
//	conf := params.DefaultRgeoDaemonConfig()
//	client, err := common.DialRPC(conf.Network, conf.Address)
//	if err != nil {
//		return nil, err
//	}
//	return &RPCReverseGeocoder{client: client}, nil
//}
//
//func (r *RPCReverseGeocoder) GetLocation(pt orb.Point) (rgeo.Location, error) {
//	var loc rgeo.Location
//	err := r.client.Call("ReverseGeocode", pt, &loc)
//	return loc, err
//}
//
//func (r *RPCReverseGeocoder) GetGeometry(pt orb.Point, dataset string) (orb.Geometry, error) {
//	var geom orb.Geometry
//	err := r.client.Call("GetGeometry", &GetGeometryRequest{Pt(pt), dataset}, &geom)
//	return geom, err
//}
//
//func (r *RPCReverseGeocoder) Close() error {
//	return r.client.Close()
//}

var (
	Cities10      = rgeo.Cities10
	Countries10   = rgeo.Countries10
	Provinces10   = rgeo.Provinces10
	US_Counties10 = rgeo.US_Counties10
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

	r1, err := rgeo.New(datasets...)
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
