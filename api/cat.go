package api

import (
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
	CatID conceptual.CatID

	// Ok, actually we DO have to have/want a conn to state.
	// An API function might use another API function,
	// and they might want to share a state conn.
	State *state.CatState

	// logger logs lines with the cat name attached.
	logger *slog.Logger

	tiledConf *params.TileDaemonConfig
	rpcClient *rpc.Client

	completedLaps event.FeedOf[cattrack.CatLap]
	completedNaps event.FeedOf[cattrack.CatNap]
}

func NewCat(catID conceptual.CatID, daemonConf *params.TileDaemonConfig) (*Cat, error) {
	c := &Cat{
		CatID:         catID,
		logger:        slog.With("cat", catID),
		tiledConf:     daemonConf,
		completedLaps: event.FeedOf[cattrack.CatLap]{},
		completedNaps: event.FeedOf[cattrack.CatNap]{},
	}

	if c.tiledConf != nil {
		if err := c.dialRPCServer(); err != nil {
			c.logger.Error("Failed to dial RPC client", "error", err)
			return nil, err
		}
		c.logger.Info("Dialed RPC client", "network", c.tiledConf.RPCNetwork, "address", c.tiledConf.RPCAddress)
	} else {
		c.logger.Debug("No tiled config, not dialing RPC client")
	}

	return c, nil
}

// WithState returns a CatState for the Cat.
// If readOnly is false it will block.
func (c *Cat) WithState(readOnly bool) (*state.CatState, error) {
	if c.State != nil {
		return c.State, nil
	}
	s := &state.Cat{CatID: c.CatID}
	st, err := s.NewCatWithState(readOnly)
	if err != nil {
		return nil, err
	}
	c.State = st
	return c.State, nil
}

func (c *Cat) getOrInitState(readOnly bool) {
	if c.State == nil {
		_, err := c.WithState(readOnly)
		if err != nil {
			c.logger.Error("Failed to create cat state", "error", err)
			return
		}
	}
}

func (c *Cat) Close() {
	if err := c.State.Close(); err != nil {
		c.logger.Error("Failed to close cat state", "error", err)
	}
	if c.IsRPCEnabled() {
		if err := c.rpcClient.Close(); err != nil {
			c.logger.Error("Failed to close RPC client", "error", err)
		}
	}
}

func (c *Cat) dialRPCServer() error {
	client, err := rpc.DialHTTP(c.tiledConf.RPCNetwork, c.tiledConf.RPCAddress)
	if err != nil {
		return err
	}
	c.rpcClient = client
	return nil
}
