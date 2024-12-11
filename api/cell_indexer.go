package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/event"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/params"
	catS2 "github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

func (c *Cat) GetDefaultCellIndexer() (*catS2.CellIndexer, error) {
	return catS2.NewCellIndexer(&catS2.CellIndexerConfig{
		CatID:           c.CatID,
		Flat:            c.State.Flat,
		Levels:          catS2.DefaultCellLevels,
		BatchSize:       (params.DefaultBatchSize / 10) + 1, // 10% of default batch size? Why? Reduce batch-y-ness.
		DefaultIndexerT: catS2.DefaultIndexerT,
		LevelIndexerT:   nil,
	})
}

// S2IndexTracks indexes incoming CatTracks for one cat.
func (c *Cat) S2IndexTracks(ctx context.Context, in <-chan cattrack.CatTrack) error {
	c.getOrInitState(false)

	c.logger.Info("S2 Indexing cat tracks")
	start := time.Now()
	defer func() {
		c.logger.Info("S2 Indexing complete", "elapsed", time.Since(start).Round(time.Second))
	}()

	cellIndexer, err := c.GetDefaultCellIndexer()
	if err != nil {
		c.logger.Error("Failed to initialize indexer", "error", err)
		return err
	}
	defer func() {
		if err := cellIndexer.Close(); err != nil {
			c.logger.Error("Failed to close indexer", "error", err)
		}
	}()

	subs := []event.Subscription{}
	chans := []chan []cattrack.CatTrack{}
	sendErrs := make(chan error, 30)
	for _, level := range cellIndexer.Config.Levels {
		if level < catS2.CellLevelTilingMinimum ||
			level > catS2.CellLevelTilingMaximum {
			continue
		}
		if !c.IsRPCEnabled() {
			c.logger.Warn("No tiled configuration, skipping S2 indexing", "level", level)
			continue
		}
		uniqLevelFeed, err := cellIndexer.FeedOfUniqueTracksForLevel(level)
		if err != nil {
			c.logger.Error("Failed to get S2 feed", "level", level, "error", err)
			return err
		}

		// First paradigm: send unique tracks to tiled, with source mode appending.
		// This builds maps with unique tracks, but where each track is the FIRST "track" seen
		// (this FIRST track can be a "small" Indexed track, though, if multiples were cached).
		// This pattern was used by CatTracksV1 to build point-based maps of unique cells for level 23.
		// The problem with this is that the first track is not as useful as the last track,
		// nor as useful a latest-state index value.
		//u1 := make(chan []cattrack.CatTrack)
		//chans = append(chans, u1)
		//u1Sub := uniqLevelFeed.Subscribe(u1)
		//subs = append(subs, u1Sub)
		//go c.sendUniqueTracksLevelAppending(ctx, level, u1, u1Sub.Err())

		// Second paradigm: in the event of a (any) unique cell for the level,
		// send ALL indexed tracks/index-values from that level to tiled, mode truncate.
		//
		// This might be crazy because when a cat goes wandering in fresh powder,
		// every time they push (a batch of unique tracks) there'll be a lot of redundant data
		// being constantly (or, with each batch) sent to tiled.
		// Big asymmetry between some fresh powder and the avalanche.
		// This is less a concern for low cell levels (e.g. 6) than high ones (e.g. 18).
		//
		// One way to improve this might be to...
		// - give tiled an indexing database option (opposed to only flat gz files),
		//   and then establish a convention for pushing indexed data (i.e. index on this level's cell id).
		//   Tiled would also need to be able to dump all indexed data for some tiled-request config.
		//   This would fix the amount of data going "over the wire" to tiled,
		//   but that's not really the major concern since, for now at least,
		//   since I expect the services to be co-located.
		// ...
		//
		// The major constraint here is that in order to produce an updated tileset (.mbtiles),
		// tippecanoe needs to read ALL the data for that tileset; it doesn't do "updates".
		u2 := make(chan []cattrack.CatTrack)
		chans = append(chans, u2)
		u2Sub := uniqLevelFeed.Subscribe(u2)
		subs = append(subs, u2Sub)
		go func() {
			sendErrs <- c.tiledDumpLevelIfUnique(ctx, cellIndexer, level, u2)
		}()
	}

	// Blocking.
	c.logger.Info("S2 Indexing blocking")
	if err := cellIndexer.Index(ctx, in); err != nil {
		c.logger.Error("CellIndexer errored", "error", err)
	}
	for i, sub := range subs {
		sub.Unsubscribe()
		close(chans[i])
		rpcErr := <-sendErrs
		if rpcErr != nil {
			c.logger.Error("Failed to send unique tracks level", "error", rpcErr)
			return err
		}
	}
	close(sendErrs)
	return nil
}

// tiledDumpLevelIfUnique sends all unique tracks at a given level to tiled, with mode truncate.
// FIXME: Dumps for high levels can be large, and currently all these dumps are blocking and sent in one request.
func (c *Cat) tiledDumpLevelIfUnique(ctx context.Context, cellIndexer *catS2.CellIndexer, level catS2.CellLevel, in <-chan []cattrack.CatTrack) error {
	unsliced := stream.Unbatch[[]cattrack.CatTrack, cattrack.CatTrack](ctx, in)

	// Block, waiting to see if any unique tracks are sent.
	uniqs := 0
	stream.Sink(ctx, func(track cattrack.CatTrack) { uniqs++ }, unsliced)
	if uniqs == 0 {
		c.logger.Debug("No unique tracks for level", "level", level)
		return nil
	}

	// There were some unique tracks at this level.
	c.logger.Info("S2 Unique tracks", "level", level, "count", uniqs)

	// So now we need to send ALL unique tracks at this level to tiled,
	// and tell tiled to use mode truncate.
	// This will replace the existing map/level with the newest version of unique tracks.
	sendErrCh := make(chan error, 1)
	dump, errs := cellIndexer.DumpLevel(level)
	batched := stream.Batch(ctx, nil, func(s []cattrack.CatTrack) bool {
		return len(s) > 10_000
	}, dump)

	levelZoomMin := catS2.TilingDefaultCellZoomLevels[level][0]
	levelZoomMax := catS2.TilingDefaultCellZoomLevels[level][1]

	levelTippeConfig, _ := params.LookupTippeConfig(params.TippeConfigNameCells, nil)
	levelTippeConfig = levelTippeConfig.Copy()
	levelTippeConfig.MustSetPair("--maximum-zoom", fmt.Sprintf("%d", levelZoomMax))
	levelTippeConfig.MustSetPair("--minimum-zoom", fmt.Sprintf("%d", levelZoomMin))

	wait := sync.WaitGroup{}
	didErr := atomic.Bool{}
	go func() {
		sourceMode := tiled.SourceModeTrunc
		first := true
		stream.Sink(ctx, func(s []cattrack.CatTrack) {
			if didErr.Load() {
				return
			}
			wait.Add(1)
			defer wait.Done()
			if !first {
				sourceMode = tiled.SourceModeAppend
			}
			first = false
			err := sendToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
				SourceSchema: tiled.SourceSchema{
					CatID:      c.CatID,
					SourceName: "s2_cells",
					LayerName:  fmt.Sprintf("level-%02d-polygons", level),
				},
				TippeConfigName: "",
				TippeConfigRaw:  levelTippeConfig,
				Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical},
				SourceModes:     []tiled.SourceMode{sourceMode},
			}, stream.Transform[cattrack.CatTrack, cattrack.CatTrack](ctx, func(track cattrack.CatTrack) cattrack.CatTrack {
				cp := track

				cp.ID = track.MustTime().Unix()
				cp.Geometry = catS2.CellPolygonForPointAtLevel(cp.Point(), level)

				return cp
			}, stream.Slice(ctx, s)))
			if err != nil {
				didErr.Store(true)
				sendErrCh <- err
			}
		}, batched)
		sendErrCh <- nil
	}()

	// Block on dump errors.
	for err := range errs {
		if err != nil {
			c.logger.Error("Failed to dump level if unique", "error", err)
		}
	}
	wait.Wait()
	return <-sendErrCh
}

func (c *Cat) sendUniqueTracksLevelAppending(ctx context.Context, level catS2.CellLevel, in <-chan []cattrack.CatTrack, awaitErr <-chan error) {
	txed := stream.Transform(ctx, func(track cattrack.CatTrack) cattrack.CatTrack {
		cp := track

		cp.ID = track.MustTime().Unix()
		cp.Geometry = catS2.CellPolygonForPointAtLevel(cp.Point(), level)

		return cp
	}, stream.Unbatch[[]cattrack.CatTrack, cattrack.CatTrack](ctx, in))

	levelZoomMin := catS2.TilingDefaultCellZoomLevels[level][0]
	levelZoomMax := catS2.TilingDefaultCellZoomLevels[level][1]

	levelTippeConfig, _ := params.LookupTippeConfig(params.TippeConfigNameCells, nil)
	levelTippeConfig = levelTippeConfig.Copy()
	levelTippeConfig.MustSetPair("--maximum-zoom", fmt.Sprintf("%d", levelZoomMax))
	levelTippeConfig.MustSetPair("--minimum-zoom", fmt.Sprintf("%d", levelZoomMin))

	sendToCatRPCClient[cattrack.CatTrack](ctx, c, &tiled.PushFeaturesRequestArgs{
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

// S2CollectLevel returns all indexed tracks for a given S2 cell level.
func (c *Cat) S2CollectLevel(ctx context.Context, level catS2.CellLevel) ([]cattrack.CatTrack, error) {
	c.getOrInitState(true)

	cellIndexer, err := c.GetDefaultCellIndexer()
	if err != nil {
		return nil, err
	}
	defer cellIndexer.Close()

	out := []cattrack.CatTrack{}
	dump, errs := cellIndexer.DumpLevel(level)
	out = stream.Collect(ctx, dump)
	return out, <-errs
}

// S2CollectLevel returns all indexed tracks for a given S2 cell level.
func (c *Cat) S2DumpLevel(wr io.Writer, level catS2.CellLevel) error {
	c.getOrInitState(true)

	cellIndexer, err := c.GetDefaultCellIndexer()
	if err != nil {
		return err
	}
	defer cellIndexer.Close()

	enc := json.NewEncoder(wr)
	dump, errs := cellIndexer.DumpLevel(level)
	go func() {
		for track := range dump {
			if err := enc.Encode(track); err != nil {
				select {
				case errs <- err:
					return
				default:
				}
			}
		}
	}()
	return <-errs
}
