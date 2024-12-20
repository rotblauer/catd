package rgeod

import (
	"errors"
	"fmt"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/rgeo"
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
		config = params.InProcRgeoDaemonConfig
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
		if _, err := os.Stat(d.config.Address); err == nil {
			d.logger.Info("Found existing socket file, checking response", "address", d.config.Address)
			c, err := common.DialRPC(d.config.ListenerConfig.Network, d.config.ListenerConfig.Address)
			if err == nil {
				c.Close()
				d.logger.Warn("Socket file already in use, refusing to compete", "address", d.config.ListenerConfig.Address)
				return fmt.Errorf("%w: %s", ErrAlreadyRunning, d.config.ListenerConfig.Address)
			}
			d.logger.Warn("Removing existing socket file (non-responsive)", "address", d.config.Address)
			os.Remove(d.config.Address)
		}
		defer os.Remove(d.config.Address)
	}

	d.server = rpc.NewServer()
	service := &ReverseGeocodeService{d}
	err := d.server.RegisterName(params.InProcRgeoDaemonConfig.ServiceName, service)
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

var ErrNotReady = errors.New("rgeo daemon not ready")

type ReverseGeocodeService struct {
	*RgeoDaemon
}

//func (r *ReverseGeocodeService) methodName(method any) string {
//	return r.config.ServiceName + "." + common.ReflectFunctionName(method)
//}

func (r *ReverseGeocodeService) Ping(common.RPCArgNone, common.RPCArgNone) error {
	if !r.ready {
		r.logger.Error("Ping")
		return ErrNotReady
	}
	r.logger.Debug("Ping")
	return nil
}

func (r *ReverseGeocodeService) GetLocation(req *rgeo.GetLocationRequest, res *rgeo.GetLocationResponse) error {
	defer func() {
		if res.Error != "" {
			r.logger.Warn("ReverseGeocode.GetLocation", "request", req, "error", res.Error)
		} else {
			r.logger.Debug("ReverseGeocode.GetLocation", "request", req, "response", res.Location)
		}
	}()
	if req == nil {
		return errors.New("request is nil")
	}
	if res == nil {
		return errors.New("response is nil")
	}

	pt := rgeo.Pt{req[0], req[1]}
	loc, err := rgeo.R("").GetLocation(pt)
	if err != nil {
		res.Error = err.Error()
		return nil
	}
	res.Location = loc
	return nil
}

func (r *ReverseGeocodeService) GetGeometry(req *rgeo.GetGeometryRequest, res *rgeo.GetGeometryResponse) error {
	defer func() {
		if res.Error != "" {
			r.logger.Warn("ReverseGeocode.GetGeometry", "request", req, "error", res.Error)
		} else {
			r.logger.Debug("ReverseGeocode.GetGeometry", "request", req, "response", res.Plat)
		}
	}()
	if req == nil {
		return errors.New("request is nil")
	}
	if req.Dataset == "" {
		return errors.New("no dataset provided")
	}
	if res == nil {
		return errors.New("response is nil")
	}
	pt := rgeo.Pt{req.Pt[0], req.Pt[1]}
	geom, err := rgeo.R(req.Dataset).GetGeometry(pt, req.Dataset)
	if err != nil {
		res.Error = err.Error()
		return nil
	}
	res.Plat = geom
	return nil
}
