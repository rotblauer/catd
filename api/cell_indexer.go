package api

import (
	"context"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/tiler"
	"github.com/rotblauer/catd/types/cattrack"
)

// S2IndexTracks indexes incoming CatTracks for one cat.
func (c *Cat) S2IndexTracks(ctx context.Context, in <-chan *cattrack.CatTrack) {
	c.getOrInitState()

	c.State.Waiting.Add(1)
	defer c.State.Waiting.Done()

	cellIndexer, err := s2.NewCellIndexer(c.CatID, params.DatadirRoot, params.S2DefaultCellLevels, params.DefaultBatchSize)
	if err != nil {
		c.logger.Error("Failed to initialize indexer", "error", err)
		return
	}
	defer func() {
		if err := cellIndexer.Close(); err != nil {
			c.logger.Error("Failed to close indexer", "error", err)
		}
	}()

	feed, err := cellIndexer.GetUniqueIndexFeed(s2.CellLevel23)
	if err != nil {
		c.logger.Error("Failed to open feed", "error", err)
		return
	}

	unique23s := make(chan []*cattrack.CatTrack)
	defer close(unique23s)
	sub23 := feed.Subscribe(unique23s)
	defer sub23.Unsubscribe()

	c.State.Waiting.Add(1)
	go sendToCatRPCClient[[]*cattrack.CatTrack](ctx, c, &tiler.PushFeaturesRequestArgs{
		SourceSchema: tiler.SourceSchema{
			CatID:      c.CatID,
			SourceName: "level-23",
			LayerName:  "tracks",
		},
		TippeConfig: params.TippeConfigNameTracks,
	}, unique23s)

	// Blocking.
	if err := cellIndexer.Index(ctx, in); err != nil {
		c.logger.Error("CellIndexer errored", "error", err)
	}
}
