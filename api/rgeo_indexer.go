package api

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/event"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/reducer"
	"github.com/rotblauer/catd/rgeo"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

/*
2024/12/14 19:55:28 INFO Rgeo unique tracks dumped cat=iPhone_16_Pro bucket=3 count=1 push.batch_size=111111 pushed.batches=1                                                                          [48/9416]
2024/12/14 19:55:28 INFO Rgeo unique tracks dumping cat=iPhone_16_Pro bucket=4 count=1 push.batch_size=111111
2024/12/14 19:55:28 INFO PushFeatures d=tile cat=iPhone_16_Pro source=github.com/sams96/rgeo.Cities10_cells layer=github.com/sams96/rgeo.Cities10_cells size="20 kB"
2024/12/14 19:55:28 ERROR PushFeatures invalid args error="source name contains path separator"
2024/12/14 19:55:28 ERROR Failed to call RPC client cat=iPhone_16_Pro method=PushFeatures source=github.com/sams96/rgeo.Cities10_cells all.len=1 error="source name contains path separator"
2024/12/14 19:55:28 ERROR Failed to send unique tracks level cat=iPhone_16_Pro error="source name contains path separator"
2024/12/14 19:55:28 INFO Rgeo unique tracks dumped cat=iPhone_16_Pro bucket=4 count=1 push.batch_size=111111 pushed.batches=1
2024/12/14 19:55:28 ERROR Failed to send unique tracks level cat=iPhone_16_Pro error="source name contains path separator"
2024/12/14 19:55:28 INFO Rgeo Indexing complete cat=iPhone_16_Pro elapsed=25s
2024/12/14 19:55:28 INFO S2 unique tracks dumping cat=iPhone_16_Pro level=6 count=1 push.batch_size=111111
*/

func (c *Cat) GetDefaultRgeoIndexer() (*reducer.CellIndexer, error) {
	bucketLevels := []reducer.Bucket{}
	for i := range rgeo.DatasetNamesStable {
		bucketLevels = append(bucketLevels, reducer.Bucket(i))
	}
	return reducer.NewCellIndexer(&reducer.CellIndexerConfig{
		CatID:           c.CatID,
		DBPath:          filepath.Join(c.State.Flat.Path(), params.RgeoDBName),
		BatchSize:       params.DefaultBatchSize,
		Buckets:         bucketLevels,
		DefaultIndexerT: rgeo.DefaultIndexerT,
		LevelIndexerT:   nil,
		BucketKeyFn:     rgeo.CatKeyFn,
		Logger:          slog.With("reducer", "rgeo"),
	})
}

// RGeoIndexTracks indexes incoming CatTracks for one cat.
func (c *Cat) RGeoIndexTracks(ctx context.Context, in <-chan cattrack.CatTrack) error {
	c.getOrInitState(false)

	c.logger.Info("Rgeo Indexing cat tracks")
	start := time.Now()
	defer func() {
		c.logger.Info("Rgeo Indexing complete", "elapsed", time.Since(start).Round(time.Second))
	}()

	cellIndexer, err := c.GetDefaultRgeoIndexer()
	if err != nil {
		c.logger.Error("Failed to initialize rgeo indexer", "error", err)
		return err
	}
	defer func() {
		if err := cellIndexer.Close(); err != nil {
			c.logger.Error("Failed to close indexer", "error", err)
		}
	}()

	subs := []event.Subscription{}
	chans := []chan []cattrack.CatTrack{}
	sendErrs := make(chan error, len(rgeo.DatasetNamesStable))
	for dataI, dataset := range rgeo.DatasetNamesStable {
		if !c.IsTilingEnabled() {
			c.logger.Warn("No RPC configuration, skipping Rgeo indexing", "bucket", dataset)
			continue
		}

		uniqLevelFeed, err := cellIndexer.FeedOfUniqueTracksForBucket(reducer.Bucket(dataI))
		if err != nil {
			c.logger.Error("Failed to get Rgeo feed", "bucket", dataset, "error", err)
			return err
		}

		u2 := make(chan []cattrack.CatTrack)
		chans = append(chans, u2)
		u2Sub := uniqLevelFeed.Subscribe(u2)
		subs = append(subs, u2Sub)
		go func() {
			sendErrs <- c.tiledDumpRgeoLevelIfUnique(ctx, cellIndexer, reducer.Bucket(dataI), u2)
		}()
	}

	// Blocking.
	c.logger.Info("Indexing Rgeo blocking")
	if err := cellIndexer.Index(ctx, in); err != nil {
		c.logger.Error("CellIndexer Rgeo errored", "error", err)
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

func (c *Cat) tiledDumpRgeoLevelIfUnique(ctx context.Context, cellIndexer *reducer.CellIndexer, bucket reducer.Bucket, in <-chan []cattrack.CatTrack) error {
	unsliced := stream.Unbatch[[]cattrack.CatTrack, cattrack.CatTrack](ctx, in)

	// Block, waiting to see if any unique tracks are sent.
	uniqs := 0
	stream.Sink(ctx, func(track cattrack.CatTrack) { uniqs++ }, unsliced)
	if uniqs == 0 {
		c.logger.Debug("No unique tracks for bucket", "bucket", bucket)
		return nil
	}

	// There were some unique tracks at this level.
	batchSize := params.RPCTrackBatchSize
	pushBatchN := int32(0)
	c.logger.Info("Rgeo unique tracks dumping", "bucket", bucket, "count", uniqs, "push.batch_size", batchSize)
	defer func() {
		c.logger.Info("Rgeo unique tracks dumped", "bucket", bucket, "count", uniqs,
			"push.batch_size", batchSize, "pushed.batches", pushBatchN)
	}()

	// So now we need to send ALL unique tracks at this level to tiled,
	// and tell tiled to use mode truncate.
	// This will replace the existing map/level with the newest version of unique tracks.
	dump, errs := cellIndexer.DumpLevel(bucket)

	levelZoomMin := rgeo.TilingZoomLevels[int(bucket)][0]
	levelZoomMax := rgeo.TilingZoomLevels[int(bucket)][1]

	levelTippeConfig, _ := params.LookupTippeConfig(params.TippeConfigNamePlats, nil)
	levelTippeConfig = levelTippeConfig.Copy()
	levelTippeConfig.MustSetPair("--maximum-zoom", fmt.Sprintf("%d", levelZoomMax))
	levelTippeConfig.MustSetPair("--minimum-zoom", fmt.Sprintf("%d", levelZoomMin))

	sendErrCh := make(chan error, 1)
	go func() {
		defer close(sendErrCh)

		// Batch dumped tracks to avoid sending too many at once.
		batched := stream.Batch(ctx, nil, func(s []cattrack.CatTrack) bool {
			return len(s) == batchSize
		}, dump)

		// TileD is sensitive to file paths. Sanitize.
		sourceName := strings.ReplaceAll(rgeo.DatasetNamesStable[bucket]+"_cells",
			string(filepath.Separator), "_")
		layerName := strings.ReplaceAll(rgeo.DatasetNamesStable[bucket]+"_cells",
			string(filepath.Separator), "_")

		sourceMode := tiled.SourceModeTrunc
		for s := range batched {
			if atomic.LoadInt32(&pushBatchN) > 0 {
				sourceMode = tiled.SourceModeAppend
			}
			atomic.AddInt32(&pushBatchN, 1)
			err := sendToCatTileD(ctx, c, &tiled.PushFeaturesRequestArgs{
				SourceSchema: tiled.SourceSchema{
					CatID:      c.CatID,
					SourceName: sourceName,
					LayerName:  layerName,
				},
				TippeConfigName: "",
				TippeConfigRaw:  levelTippeConfig,
				Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical},
				SourceModes:     []tiled.SourceMode{sourceMode},
			}, stream.Filter(ctx, func(t cattrack.CatTrack) bool {
				return !t.IsEmpty()
			},
				stream.Transform[cattrack.CatTrack, cattrack.CatTrack](ctx,
					rgeo.TransformCatTrackFn(int(bucket)),
					stream.Slice(ctx, s))))
			if err != nil {
				sendErrCh <- err
				return
			}
		}
	}()

	// Block on dump errors.
	for err := range errs {
		if err != nil {
			c.logger.Error("Failed to dump level if unique", "error", err)
			return err
		}
	}
	for err := range sendErrCh {
		if err != nil {
			c.logger.Error("Failed to send unique tracks level", "error", err)
			return err
		}
	}
	return nil
}

// S2CollectLevel returns all indexed tracks for a given S2 cell level.
func (c *Cat) RgeoCollectLevel(ctx context.Context, level int) ([]cattrack.CatTrack, error) {
	c.getOrInitState(true)

	cellIndexer, err := c.GetDefaultRgeoIndexer()
	if err != nil {
		return nil, err
	}
	defer cellIndexer.Close()

	out := []cattrack.CatTrack{}
	dump, errs := cellIndexer.DumpLevel(reducer.Bucket(level))
	out = stream.Collect(ctx, dump)
	return out, <-errs
}
