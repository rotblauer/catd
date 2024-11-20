package api

import (
	"context"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
)

// S2IndexTracks indexes incoming CatTracks for one cat.
func (c *Cat) S2IndexTracks(ctx context.Context, in <-chan *cattrack.CatTrack) {

	if c.State == nil {
		_, err := c.WithState(false)
		if err != nil {
			slog.Error("Failed to create cat state", "error", err)
			return
		}
	}

	c.State.Waiting.Add(1)
	defer c.State.Waiting.Done()

	cellIndexer, err := s2.NewCellIndexer(c.CatID, params.DatadirRoot, params.S2DefaultCellLevels, params.DefaultBatchSize)
	if err != nil {
		slog.Error("Failed to initialize indexer", "error", err)
		return
	}
	defer func() {
		if err := cellIndexer.Close(); err != nil {
			slog.Error("Failed to close indexer", "error", err)
		}
	}()
	// Blocking.
	if err := cellIndexer.Index(ctx, in); err != nil {
		slog.Error("CellIndexer errored", "error", err)
	}
}
