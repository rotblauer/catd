package api

import (
	"context"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"sync"
)

// S2IndexTracks indexes incoming CatTracks for one cat.
func S2IndexTracks(ctx context.Context, wg *sync.WaitGroup, catID conceptual.CatID, in <-chan *cattrack.CatTrack) {
	if wg != nil {
		wg.Add(1)
		defer wg.Done()
	}

	cellIndexer, err := s2.NewCellIndexer(catID, params.DatadirRoot, params.S2DefaultCellLevels, params.DefaultBatchSize)
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
