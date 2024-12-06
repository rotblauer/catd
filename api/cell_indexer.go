package api

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/event"
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
		cellIndexer.Waiting.Wait()
		if err := cellIndexer.Close(); err != nil {
			c.logger.Error("Failed to close indexer", "error", err)
		}
	}()

	subs := []event.Subscription{}
	chans := []chan []cattrack.CatTrack{}
	for _, level := range cellIndexer.Levels {

		// FIXME Shove me off to own function.
		// Beware routines. Must wait in. Defers. Closers.
		uniqLevelFeed, err := cellIndexer.FeedOfUniqueTracksForLevel(level)
		if err != nil {
			c.logger.Error("Failed to get S2 feed", "level", level, "error", err)
			return
		}

		// First paradigm: send unique tracks to tiled, with source mode appending.
		// This builds maps with unique tracks, but where each track is the FIRST "track" seen
		// (this FIRST track can be a "small" Indexed track, though, if multiples were cached).
		// This pattern was used by CatTracksV1 to build point-based maps of unique cells for level 23.
		// The problem with this is that the first track is not as useful as the last track,
		// or as a latest-state indexed track value.
		u1 := make(chan []cattrack.CatTrack)
		chans = append(chans, u1)
		u1Sub := uniqLevelFeed.Subscribe(u1)
		subs = append(subs, u1Sub)
		go c.sendUniqueTracksLevelAppending(ctx, level, u1, u1Sub.Err())

		u2 := make(chan []cattrack.CatTrack)
		chans = append(chans, u2)
		u2Sub := uniqLevelFeed.Subscribe(u2)
		subs = append(subs, u2Sub)

		go c.dumpLevelIfUnique(ctx, cellIndexer, level, u2, u2Sub.Err())
	}

	// Blocking.
	if err := cellIndexer.Index(ctx, in); err != nil {
		c.logger.Error("CellIndexer errored", "error", err)
	}
	for _, sub := range subs {
		sub.Unsubscribe()
	}
	for _, ch := range chans {
		close(ch)
	}
}

func (c *Cat) dumpLevelIfUnique(ctx context.Context, cellIndexer *catS2.CellIndexer, level catS2.CellLevel, in <-chan []cattrack.CatTrack, awaitErr <-chan error) {
	c.State.Waiting.Add(1)
	defer c.State.Waiting.Done()
	cellIndexer.Waiting.Add(1)
	defer cellIndexer.Waiting.Done()

	unsliced := stream.Unslice[[]cattrack.CatTrack, cattrack.CatTrack](ctx, in)

	// Block, waiting to see if any unique tracks are sent.
	uniqs := 0
	stream.Sink(ctx, func(track cattrack.CatTrack) { uniqs++ }, unsliced)
	if uniqs == 0 {
		c.logger.Debug("No unique tracks for level", "level", level)
		return
	}

	// There were some unique tracks at this level.
	c.logger.Info("S2 Unique tracks", "level", level, "count", uniqs)

	// So now we need to send ALL unique tracks at this level to tiled,
	// and tell tiled to use mode truncate.
	// This will replace the existing map/level with the newest version of unique tracks.
	dump, errs := cellIndexer.DumpLevel(level)

	levelZoomMin := catS2.SlippyCellZoomLevels[level][0]
	levelZoomMax := catS2.SlippyCellZoomLevels[level][1]

	levelTippeConfig, _ := params.LookupTippeConfig(params.TippeConfigNameCells, nil)
	levelTippeConfig = levelTippeConfig.Copy()
	levelTippeConfig.MustSetPair("--maximum-zoom", fmt.Sprintf("%d", levelZoomMax))
	levelTippeConfig.MustSetPair("--minimum-zoom", fmt.Sprintf("%d", levelZoomMin))

	sendBatchToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
		SourceSchema: tiled.SourceSchema{
			CatID:      c.CatID,
			SourceName: "s2_cells",
			LayerName:  fmt.Sprintf("level-%02d-polygons", level),
		},
		TippeConfigName: "",
		TippeConfigRaw:  levelTippeConfig,
		Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical},
		SourceModes:     []tiled.SourceMode{tiled.SourceModeTrunc},
	}, stream.Transform[cattrack.CatTrack, cattrack.CatTrack](ctx, func(track cattrack.CatTrack) cattrack.CatTrack {
		cp := track

		// FIXME Use a real ID
		cp.ID = rand.Int63()
		cp.Geometry = catS2.CellPolygonForPointAtLevel(cp.Point(), level)

		return cp
	}, dump))

	// Block on dump errors.
	for err := range errs {
		if err != nil {
			c.logger.Error("Failed to dump level if unique", "error", err)
		}
	}
}

func (c *Cat) sendUniqueTracksLevelAppending(ctx context.Context, level catS2.CellLevel, in <-chan []cattrack.CatTrack, awaitErr <-chan error) {
	txed := stream.Transform(ctx, func(track cattrack.CatTrack) cattrack.CatTrack {
		cp := track

		// FIXME Use a real ID
		cp.ID = rand.Int63()
		cp.Geometry = catS2.CellPolygonForPointAtLevel(cp.Point(), level)

		return cp
	}, stream.Unslice[[]cattrack.CatTrack, cattrack.CatTrack](ctx, in))

	levelZoomMin := catS2.SlippyCellZoomLevels[level][0]
	levelZoomMax := catS2.SlippyCellZoomLevels[level][1]

	levelTippeConfig, _ := params.LookupTippeConfig(params.TippeConfigNameCells, nil)
	levelTippeConfig = levelTippeConfig.Copy()
	levelTippeConfig.MustSetPair("--maximum-zoom", fmt.Sprintf("%d", levelZoomMax))
	levelTippeConfig.MustSetPair("--minimum-zoom", fmt.Sprintf("%d", levelZoomMin))

	sendBatchToCatRPCClient[cattrack.CatTrack](ctx, c, &tiled.PushFeaturesRequestArgs{
		SourceSchema: tiled.SourceSchema{
			CatID:      c.CatID,
			SourceName: "s2_cells_first",
			LayerName:  fmt.Sprintf("level-%02d-polygons", level),
		},
		TippeConfigName: "",
		TippeConfigRaw:  levelTippeConfig,
		Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
		SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
	}, stream.Filter(ctx, func(track cattrack.CatTrack) bool {
		return !track.IsEmpty()
	}, txed))

	for err := range awaitErr {
		if err != nil {
			c.logger.Error("Failed to send unique tracks level", "error", err)
		}
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
