package api

import (
	"errors"
	"github.com/ethereum/go-ethereum/event"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/state"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"net/rpc"
)

// Cat is the API representation of a cat.
// It does not reflect cat state. (Well, it can _reflect_ it, but not ~be~ it).
// It CAN reflect values about some (assumed, or inferred) cat,
// where data for the cat can come from some context, like
// a token (permissions), a CLI-flag, a URL parameter, or even
// an environment value. Anywhere cat data comes from, that is not the state of this app.
type Cat struct {
	CatID   conceptual.CatID
	DataDir string

	// Ok, actually we DO have to have/want a conn to state.
	// An API function might use another API function,
	// and they might want to share a state conn.
	State *state.CatState

	// backend hooks the cat up with tiled and rgeod services.
	backend *params.CatRPCServices

	// logger logs lines with the cat name attached.
	logger        *slog.Logger
	completedLaps event.FeedOf[cattrack.CatLap]
	completedNaps event.FeedOf[cattrack.CatNap]
}

// NewCat inits a new Cat, but it does not access state.
// The given datadir should be the CAT datadir (not the daemon datadir).
func NewCat(catID conceptual.CatID, datadir string, backend *params.CatRPCServices) (*Cat, error) {
	if catID == "" {
		return nil, errors.New("catID is required")
	}
	logger := slog.With("cat", catID)
	if datadir == "" {
		logger.Warn("No data dir provided, using default", "cat", catID)
		datadir = params.DefaultCatDataDir(catID.String())
	}
	c := &Cat{
		CatID:         catID,
		DataDir:       datadir,
		backend:       backend, // can be nil
		logger:        logger,
		completedLaps: event.FeedOf[cattrack.CatLap]{},
		completedNaps: event.FeedOf[cattrack.CatNap]{},
	}

	if c.IsTilingEnabled() {
		c.logger.Info("Tiled RPC client configured",
			"network", c.backend.TileD.Network, "address", c.backend.TileD.Address)
	} else {
		c.logger.Debug("No Tiled RPC client configured")
	}

	if c.IsRgeoEnabled() {
		c.logger.Info("Rgeo RPC client configured",
			"network", c.backend.RgeoD.Network, "address", c.backend.RgeoD.Address)
	} else {
		c.logger.Debug("No Rgeo RPC client configured")
	}

	return c, nil
}

// LockOrLoadState makes sure a Cat has r|w CatState.
// If readOnly is false it will block unless already open.
func (c *Cat) LockOrLoadState(readOnly bool) error {
	if c.CatID == "" {
		return errors.New("catID is required")
	}
	if c.DataDir == "" {
		c.DataDir = params.DefaultCatDataDir(c.CatID.String())
	}
	if c.State == nil {
		c.State = state.NewCatState(c.CatID, c.DataDir, readOnly)
		return c.State.Open()
	}
	if !c.State.IsOpen() {
		return c.State.Open()
	}
	return nil
}

// getOrInitState gets the state if it exists, or initializes it if it doesn't.
// It is a way for API methods to idempotently declare if and how they need persistent cat resources.
func (c *Cat) getOrInitState(readOnly bool) {
	if c.State == nil {
		c.LockOrLoadState(readOnly)
	}
}

func (c *Cat) Close() error {
	if !c.State.IsOpen() {
		return errors.New("cat state not open")
	}
	return c.State.Close()
	return nil
}

func (c *Cat) IsRPCEnabled() bool {
	return c.backend != nil
}

func (c *Cat) IsTilingEnabled() bool {
	return c.IsRPCEnabled() && c.backend.TileD != nil
}

func (c *Cat) IsRgeoEnabled() bool {
	return c.IsRPCEnabled() && c.backend.RgeoD != nil
}

func getRPCClient(config params.ListenerConfig) (*rpc.Client, error) {
	switch config.Network {
	case "unix", "unixpacket":
		return rpc.Dial(config.Network, config.Address)
	case "tcp", "tcp4", "tcp6":
		return rpc.DialHTTP(config.Network, config.Address)
	default:
		panic("unsupported network type")
	}
	return nil, nil
}

func (c *Cat) dialTiledRPC() (*rpc.Client, error) {
	return getRPCClient(c.backend.TileD.ListenerConfig)
}

func (c *Cat) dialRgeo() (*rpc.Client, error) {
	return getRPCClient(c.backend.RgeoD.ListenerConfig)
}
