package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/event"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/reducer"
	catS2 "github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"time"
)

func (c *Cat) GetDefaultS2CellIndexer() (*reducer.CellIndexer, error) {
	bucketLevels := []reducer.Bucket{}
	for _, level := range catS2.DefaultCellLevels {
		bucketLevels = append(bucketLevels, reducer.Bucket(level))
	}
	return reducer.NewCellIndexer(&reducer.CellIndexerConfig{
		CatID:           c.CatID,
		DBPath:          filepath.Join(c.State.Flat.Path(), params.S2DBName),
		BatchSize:       params.DefaultBatchSize, // 10% of default batch size? Why? Reduce batch-y-ness.
		Buckets:         bucketLevels,
		DefaultIndexerT: catS2.DefaultIndexerT,
		LevelIndexerT:   nil,
		BucketKeyFn:     catS2.CatKeyFn,
		Logger:          slog.With("reducer", "s2"),
	})
}

// S2IndexTracks indexes incoming CatTracks for one cat.
func (c *Cat) S2IndexTracks(ctx context.Context, in <-chan cattrack.CatTrack) error {
	c.getOrInitState(false)

	c.logger.Info("Indexing S2 cat tracks")
	start := time.Now()
	defer func() {
		c.logger.Info("Indexing S2 complete", "elapsed", time.Since(start).Round(time.Second))
	}()

	cellIndexer, err := c.GetDefaultS2CellIndexer()
	if err != nil {
		c.logger.Error("Failed to initialize S2 indexer", "error", err)
		return err
	}
	defer cellIndexer.Close()

	subs := []event.Subscription{}
	chans := []chan []cattrack.CatTrack{}
	sendErrs := make(chan error, len(catS2.DefaultCellLevels))
	defer close(sendErrs)
	logOnce := sync.Once{}
	for _, level := range catS2.DefaultCellLevels {
		if !c.IsTilingEnabled() {
			logOnce.Do(func() {
				c.logger.Warn("No RPC configuration, skipping S2 indexing", "level", level)
			})
			break
		}
		if level < catS2.CellLevelTilingMinimum ||
			level > catS2.CellLevelTilingMaximum {
			continue
		}
		uniqLevelFeed, err := cellIndexer.FeedOfUniqueTracksForBucket(reducer.Bucket(level))
		if err != nil {
			c.logger.Error("Failed to get S2 feed", "level", level, "error", err)
			return err
		}

		u2 := make(chan []cattrack.CatTrack)
		chans = append(chans, u2)
		u2Sub := uniqLevelFeed.Subscribe(u2)
		subs = append(subs, u2Sub)
		go func() {
			sendErrs <- c.tiledDumpS2LevelIfUnique(ctx, cellIndexer, reducer.Bucket(level), u2)
		}()
	}

	// Blocking.
	c.logger.Info("Indexing S2 blocking")
	if err := cellIndexer.Index(ctx, in); err != nil {
		c.logger.Error("CellIndexer S2 errored", "error", err)
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
	return nil
}

// tiledDumpS2LevelIfUnique sends all unique tracks at a given level to tiled, with mode truncate.
func (c *Cat) tiledDumpS2LevelIfUnique(ctx context.Context, cellIndexer *reducer.CellIndexer, level reducer.Bucket, in <-chan []cattrack.CatTrack) error {
	// Block, waiting to see if any unique tracks are sent.
	uniqs := 0
	for i := range in {
		uniqs += len(i)
	}
	if uniqs == 0 {
		c.logger.Debug("No unique tracks for level", "level", level)
		return nil
	}

	levelZoomMin := catS2.TilingDefaultCellZoomLevels[catS2.CellLevel(level)][0]
	levelZoomMax := catS2.TilingDefaultCellZoomLevels[catS2.CellLevel(level)][1]
	levelTippeConfig, _ := params.LookupTippeConfig(params.TippeConfigNameCells, nil)
	levelTippeConfig = levelTippeConfig.Copy()
	levelTippeConfig.MustSetPair("--maximum-zoom", fmt.Sprintf("%d", levelZoomMax))
	levelTippeConfig.MustSetPair("--minimum-zoom", fmt.Sprintf("%d", levelZoomMin))

	// Dump all indexed tracks for the level.
	dump, errs := cellIndexer.DumpLevel(reducer.Bucket(level))
	edit := stream.Transform[cattrack.CatTrack, cattrack.CatTrack](ctx, func(track cattrack.CatTrack) cattrack.CatTrack {
		track.ID = track.MustTime().Unix()
		track.Geometry = catS2.GetCellGeometry(track.Point(), catS2.CellLevel(level))
		return track
	}, dump)
	err := sendToCatTileD(ctx, c, &tiled.PushFeaturesRequestArgs{
		SourceSchema: tiled.SourceSchema{
			CatID:      c.CatID,
			SourceName: "s2_cells",
			LayerName:  fmt.Sprintf("level-%02d", level),
		},
		TippeConfigName: "",
		TippeConfigRaw:  levelTippeConfig,
		Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical},
		SourceModes:     []tiled.SourceMode{tiled.SourceModeTruncate},
	}, edit)
	for err := range errs {
		if err != nil {
			c.logger.Error("Failed to dump unique tracks level", "error", err)
			return err
		}
	}
	return err
}

// sendUniqueTracksLevelAppending is not currently in use, but here
// for reference in case you want to send unique tracks to tiled with source mode appending.
func (c *Cat) sendUniqueTracksLevelAppending(ctx context.Context, level catS2.CellLevel, in <-chan []cattrack.CatTrack, awaitErr <-chan error) {
	transformed := stream.Transform(ctx, func(track cattrack.CatTrack) cattrack.CatTrack {
		cp := track
		cp.ID = track.MustTime().Unix()
		cp.Geometry = catS2.GetCellGeometry(cp.Point(), level)
		return cp
	}, stream.Unbatch[[]cattrack.CatTrack, cattrack.CatTrack](ctx, in))

	levelZoomMin := catS2.TilingDefaultCellZoomLevels[level][0]
	levelZoomMax := catS2.TilingDefaultCellZoomLevels[level][1]

	levelTippeConfig, _ := params.LookupTippeConfig(params.TippeConfigNameCells, nil)
	levelTippeConfig = levelTippeConfig.Copy()
	levelTippeConfig.MustSetPair("--maximum-zoom", fmt.Sprintf("%d", levelZoomMax))
	levelTippeConfig.MustSetPair("--minimum-zoom", fmt.Sprintf("%d", levelZoomMin))

	sendToCatTileD[cattrack.CatTrack](ctx, c, &tiled.PushFeaturesRequestArgs{
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
	}, transformed))

	for err := range awaitErr {
		if err != nil {
			c.logger.Error("Failed to send unique tracks level", "error", err)
		}
	}
}

/*
	RE: Above

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
	// - give tiled an indexing database option (opposed to only lat gz files),
	//   and then establish a convention for pushing indexed data (i.e. index on this level's cell id).
	//   Tiled would also need to be able to dump all indexed data for some tiled-request config.
	//   This would fix the amount of data going "over the wire" to tiled,
	//   but that's not really the major concern since, for now at least,
	//   since I expect the services to be co-located.
	// ...
	//
	// The major constraint here is that in order to produce an updated tileset (.mbtiles),
	// tippecanoe needs to read ALL the data for that tileset; it doesn't do "updates".
*/

// S2CollectLevel returns all indexed tracks for a given S2 cell level.
func (c *Cat) S2CollectLevel(ctx context.Context, level catS2.CellLevel) ([]cattrack.CatTrack, error) {
	c.getOrInitState(true)

	cellIndexer, err := c.GetDefaultS2CellIndexer()
	if err != nil {
		return nil, err
	}
	defer cellIndexer.Close()

	out := []cattrack.CatTrack{}
	dump, errs := cellIndexer.DumpLevel(reducer.Bucket(level))
	out = stream.Collect(ctx, dump)
	return out, <-errs
}

// S2CollectLevel writes all indexed tracks for a given S2 cell level.
func (c *Cat) S2DumpLevel(wr io.Writer, level catS2.CellLevel) error {
	c.getOrInitState(true)

	cellIndexer, err := c.GetDefaultS2CellIndexer()
	if err != nil {
		return err
	}
	defer cellIndexer.Close()

	enc := json.NewEncoder(wr)
	dump, errs := cellIndexer.DumpLevel(reducer.Bucket(level))
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
