package rgeod

import (
	"errors"
	"fmt"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/rgeo"
	rrgeo "github.com/sams96/rgeo"
	"log"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"strings"
)

// TODO.
// RPC. Unix sockets. HTML? ...Ethereum? Subscriptions? New state - New Mexico? No, that is API. Application choices.

// RgeoDaemon is the daemon for the rgeod RPC service.
// Rgeo is useful as a daemon because
// - it takes a long time (1m?) to initialize an instance with all datasets (cities, countries, provinces, US counties)
// - it might make sense to cache calls to avoid hitting the actual backing rgeo API
// - there might be other uses in other places for reverse geocoding later, and transport agnostic keeps options open.
type RgeoDaemon struct {
	config    *params.RgeoDaemonConfig
	server    *rpc.Server
	logger    *slog.Logger
	interrupt chan struct{}
	ready     bool
}

func NewDaemon(config *params.RgeoDaemonConfig) (*RgeoDaemon, error) {
	logger := slog.With("daemon", "rgeo")
	if config == nil {
		logger.Warn("No config provided, using default")
		config = params.DefaultRgeoDaemonConfig()
	}
	return &RgeoDaemon{
		config:    config,
		logger:    logger,
		interrupt: make(chan struct{}, 1),
	}, nil
}

var ErrAlreadyRunning = errors.New("rgeo daemon already running")

// Start starts the daemon and does not wait for it to complete.
func (d *RgeoDaemon) Start() error {
	d.logger.Info("Rgeo daemon starting...",
		"network", d.config.ListenerConfig.Network, "address", d.config.ListenerConfig.Address)

	if strings.HasPrefix(d.config.Network, "unix") {
		defer os.Remove(d.config.Address)
	}
	c, err := common.DialRPC(d.config.ListenerConfig.Network, d.config.ListenerConfig.Address)
	if err == nil {
		c.Close()
		return fmt.Errorf("%w: %s", ErrAlreadyRunning, d.config.ListenerConfig.Address)
	}

	d.server = rpc.NewServer()
	err = d.server.Register(&ReverseGeocode{d})
	if err != nil {
		return err
	}
	listener, err := net.Listen(d.config.ListenerConfig.Network, d.config.ListenerConfig.Address)
	if err != nil {
		return err
	}
	go d.server.Accept(listener)

	d.logger.Info("Initializing rgeo datasets (this may take a while)... ")
	if err := rgeo.Init(); err != nil {
		if !errors.Is(err, rgeo.ErrAlreadyInitialized) {
			log.Fatalln("Failed to initialize rgeo datasets", "error", err)
		}
	}
	d.ready = true
	d.logger.Info("Rgeo daemon and datasets ready")
	sig := <-d.interrupt
	d.logger.Info("Rgeo daemon interrupted", "signal", sig)
	return nil
}

// Stop stops the daemon and waits for it to complete.
func (d *RgeoDaemon) Stop() error {
	d.logger.Info("Rgeo daemon stopping")
	d.interrupt <- struct{}{}
	return nil
}

type ReverseGeocode struct {
	*RgeoDaemon
}

var ErrNotReady = errors.New("rgeo daemon not ready")

func (r *ReverseGeocode) Ping(common.RPCArgNone, common.RPCArgNone) error {
	if !r.ready {
		return ErrNotReady
	}
	return nil
}

type Pt [2]float64

// GetLocationRequest should be [X,Y]::[Lng,Lat].
type GetLocationRequest Pt

type GetLocationResponse struct {
	Location rrgeo.Location
	Error    error
}

func (r *ReverseGeocode) GetLocation(req *GetLocationRequest, res *GetLocationResponse) error {
	if req == nil {
		return errors.New("request is nil")
	}
	if res == nil {
		return errors.New("response is nil")
	}
	pt := orb.Point{req[0], req[1]}
	loc, err := rgeo.R("").GetLocation(pt)
	if err != nil {
		res.Error = err
		return nil
	}
	res.Location = loc
	return nil
}

type GetGeometryRequest struct {
	Pt
	Dataset string
}
type GetGeometryResponse struct {
	Geometry orb.Geometry
	Error    error
}

func (r *ReverseGeocode) GetGeometry(req *GetGeometryRequest, res *GetGeometryResponse) error {
	if req == nil {
		return errors.New("request is nil")
	}
	if req.Dataset == "" {
		return errors.New("no dataset provided")
	}
	if res == nil {
		return errors.New("response is nil")
	}
	pt := orb.Point{req.Pt[0], req.Pt[1]}
	geom, err := rgeo.R(req.Dataset).GetGeometry(pt, req.Dataset)
	if err != nil {
		res.Error = err
		return nil
	}
	res.Geometry = geom
	return nil
}
