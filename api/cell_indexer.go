package api

import (
	"context"
	"fmt"
	"github.com/golang/geo/s2"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/params"
	catS2 "github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"math/rand"
	"time"
)

// S2IndexTracks indexes incoming CatTracks for one cat.
func (c *Cat) S2IndexTracks(ctx context.Context, in <-chan cattrack.CatTrack) {
	c.getOrInitState()

	c.State.Waiting.Add(1)
	defer c.State.Waiting.Done()

	c.logger.Info("S2 Indexing cat tracks")
	start := time.Now()
	defer func() {
		c.logger.Info("S2 Indexing complete", "elapsed", time.Since(start).Round(time.Second))
	}()

	cellIndexer, err := catS2.NewCellIndexer(c.CatID, params.DatadirRoot, params.S2DefaultCellLevels, params.DefaultBatchSize)
	if err != nil {
		c.logger.Error("Failed to initialize indexer", "error", err)
		return
	}
	defer func() {
		if err := cellIndexer.Close(); err != nil {
			c.logger.Error("Failed to close indexer", "error", err)
		}
	}()

	for _, level := range cellIndexer.Levels {

		// FIXME Shove me off to own function.
		// Beware routines. Must wait in. Defers. Closers.
		uniqLevelFeed, err := cellIndexer.FeedOfUniqueTracksForLevel(level)
		if err != nil {
			c.logger.Error("Failed to get S2 feed", "level", level, "error", err)
			return
		}

		uniqTracksLeveled := make(chan []cattrack.CatTrack)
		defer close(uniqTracksLeveled)
		sub := uniqLevelFeed.Subscribe(uniqTracksLeveled)
		defer sub.Unsubscribe()

		txed := stream.Transform(ctx, func(track cattrack.CatTrack) cattrack.CatTrack {
			cp := track

			pt := cp.Point()
			leaf := s2.CellIDFromLatLng(s2.LatLngFromDegrees(pt.Lat(), pt.Lon()))
			leveledCellID := catS2.CellIDWithLevel(leaf, level)

			cell := s2.CellFromCellID(leveledCellID)

			vertices := []orb.Point{}
			for i := 0; i < 4; i++ {
				vpt := cell.Vertex(i)
				//pt := cell.Edge(i) // tippe halt catch fire
				ll := s2.LatLngFromPoint(vpt)
				vertices = append(vertices, orb.Point{ll.Lng.Degrees(), ll.Lat.Degrees()})
			}

			cp.Geometry = orb.Polygon{orb.Ring(vertices)}
			cp.ID = rand.Int63()
			return cp
		}, stream.Unslice[[]cattrack.CatTrack, cattrack.CatTrack](ctx, uniqTracksLeveled))

		levelZoomMin := catS2.SlippyCellZoomLevels[level][0]
		levelZoomMax := catS2.SlippyCellZoomLevels[level][1]

		levelTippeConfig, _ := params.LookupTippeConfig(params.TippeConfigNameCells, nil)
		levelTippeConfig = levelTippeConfig.Copy()
		levelTippeConfig.MustSetPair("--maximum-zoom", fmt.Sprintf("%d", levelZoomMax))
		levelTippeConfig.MustSetPair("--minimum-zoom", fmt.Sprintf("%d", levelZoomMin))

		sendBatchToCatRPCClient[cattrack.CatTrack](ctx, c, &tiled.PushFeaturesRequestArgs{
			SourceSchema: tiled.SourceSchema{
				CatID:      c.CatID,
				SourceName: "s2_cells",
				LayerName:  fmt.Sprintf("level-%02d-polygons", level),
			},
			TippeConfig:    "",
			TippeConfigRaw: levelTippeConfig,
		}, stream.Filter(ctx, func(track cattrack.CatTrack) bool {
			return !track.IsEmpty()
		}, txed))
	}

	// Blocking.
	if err := cellIndexer.Index(ctx, in); err != nil {
		c.logger.Error("CellIndexer errored", "error", err)
	}
}

//func (c *Cat) sendToTiled(ctx context.Context, args *tiled.PushFeaturesRequestArgs, in <-chan cattrack.CatTrack) {
//	if c.rpcClient == nil {
//		c.logger.Debug("Cat RPC client not configured (noop)", "method", "PushFeatures")
//		return
//	}
//	c.State.Waiting.Add(1)
//	go func() {
//		defer c.State.Waiting.Done()
//		sendBatchToCatRPCClient[cattrack.CatTrack](ctx, c, args, in)
//	}()
//}
