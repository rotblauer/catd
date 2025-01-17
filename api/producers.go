package api

import (
	"context"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/geo/clean"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"sync"
)

//func (c *Cat) TeeToFileGZ(ctx context.Context, in <-chan cattrack.CatTrack, path string) {
//	gz, err := catz.NewGZFileWriter(path, catz.DefaultGZFileWriterConfig())
//	if err != nil {
//		c.logger.Error("Failed to create GZ file writer", "error", err)
//		return
//	}
//	defer gz.Close()
//
//	for track := range in {
//		select {
//		case <-ctx.Done():
//			return
//		default:
//		}
//		if _, err := gz.Write(track); err != nil {
//			c.logger.Error("Failed to write track to GZ file", "error", err)
//			return
//		}
//	}
//}

func (c *Cat) ProducerPipelines(ctx context.Context, in <-chan cattrack.CatTrack) error {

	c.logger.Info("Producer pipelines")
	defer c.logger.Info("Producer pipelines complete")

	// Clean and improve tracks for pipeline handlers.
	wOffsets := cattrack.WithTimeOffset(ctx, in)
	offsetsPass, offsetsDebug := stream.Tee(ctx, wOffsets)

	//stream.Blackhole(offsetsDebug)
	////// P.S. Don't send all tracks to tiled unless development?
	go func() {
		if err := sendToCatTileD(ctx, c, &tiled.PushFeaturesRequestArgs{
			SourceSchema: tiled.SourceSchema{
				CatID:      c.CatID,
				SourceName: "tracks",
				LayerName:  "tracks",
			},
			TippeConfigName: params.TippeConfigNameTracks,
			Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
			SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
		}, offsetsDebug); err != nil {
			c.logger.Error("Failed to send tracks", "error", err)
		}
	}()

	cleaned := c.CleanTracks(ctx, offsetsPass)
	//cleanDebug, cleanPass := stream.Tee[cattrack.CatTrack](ctx, cleaned)

	//improved := c.ImprovedActTracks(ctx, cleaned)
	improved := c.ImprovedActTracks(ctx, cleaned)
	improvedA := make(chan cattrack.CatTrack)
	improvedB := make(chan cattrack.CatTrack)
	improvedC := make(chan cattrack.CatTrack)
	stream.TeeMany(ctx, improved, improvedA, improvedB, improvedC)

	////// P.S. Don't send all tracks to tiled unless development?
	go func() {
		if err := sendToCatTileD(ctx, c, &tiled.PushFeaturesRequestArgs{
			SourceSchema: tiled.SourceSchema{
				CatID:      c.CatID,
				SourceName: "tracks-improved",
				LayerName:  "tracks-improved",
			},
			TippeConfigName: params.TippeConfigNameTracks,
			Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
			SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
		}, improvedA); err != nil {
			c.logger.Error("Failed to send tracks", "error", err)
		}
	}()

	c.State.Waiting.Add(1)
	go func() {
		defer c.State.Waiting.Done()
		once := sync.Once{}
		batches := stream.Batch(ctx, nil, func(tracks []cattrack.CatTrack) bool {
			return len(tracks) >= 1000
		}, improvedB)
		for batch := range batches {
			if params.INFLUXDB_URL == "" {
				once.Do(func() {
					c.logger.Warn("InfluxDB not configured", "method", "ExportCatTracks")
				})
				continue
			}
			err := c.ExportInfluxDB(batch)
			if err != nil {
				// CHORE: Return error via chan.
				c.logger.Error("Failed to post batch to InfluxDB", "error", err)
			} else {
				c.logger.Debug("Batch InfluxDB export", "count", len(batch))
			}
		}
	}()

	pipeliners := improvedC
	//pipeliners := improved
	areaPipeCh := make(chan cattrack.CatTrack, params.DefaultChannelCap)
	vectorPipeCh := make(chan cattrack.CatTrack, params.DefaultChannelCap)
	simpleIndexerCh := make(chan cattrack.CatTrack, params.DefaultChannelCap)
	stream.TeeMany(ctx, pipeliners, areaPipeCh, vectorPipeCh, simpleIndexerCh)

	groundedArea := stream.Filter[cattrack.CatTrack](ctx, clean.FilterGrounded, areaPipeCh)
	g1, g2 := stream.Tee(ctx, groundedArea)

	nPipes := 4
	errs := make(chan error, nPipes)
	go func() { errs <- c.S2IndexTracks(ctx, g1) }()
	go func() { errs <- c.RGeoIndexTracks(ctx, g2) }()
	go func() { errs <- c.CatActPipeline(ctx, vectorPipeCh) }()
	go func() { errs <- c.OffsetIndexer(ctx, simpleIndexerCh) }()

	c.logger.Debug("Producer pipelines waiting for completion")

	for i := 0; i < nPipes; i++ {
		select {
		case err := <-errs:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	close(errs)
	return nil
}

// OffsetIndexer is a simple indexer that reduces tracks by count and time offsets.
func (c *Cat) OffsetIndexer(ctx context.Context, in <-chan cattrack.CatTrack) error {
	c.logger.Info("Simple offset indexer")
	defer c.logger.Info("Simple offset indexer complete")

	indexerT := &cattrack.OffsetIndexT{}
	oldT := &cattrack.OffsetIndexT{}

	oldTrack := cattrack.CatTrack{}
	if err := c.State.ReadKVUnmarshalJSON(params.CatStateBucket, params.CatStateKey_OffsetIndexer, &oldTrack); err != nil {
		c.logger.Warn("Did not read offsetIndexer state (new cat?)", "error", err)
	} else {
		oldT = indexerT.FromCatTrack(oldTrack).(*cattrack.OffsetIndexT)
	}

	lastTrack := cattrack.CatTrack{}
	for track := range in {
		select {
		case <-ctx.Done():
			break
		default:
		}
		lastTrack = track
		indexing := indexerT.FromCatTrack(track)
		next := indexerT.Index(oldT, indexing)
		*oldT = *next.(*cattrack.OffsetIndexT)
	}
	c.logger.Info("Simple indexer complete")

	storeTrack := indexerT.ApplyToCatTrack(oldT, lastTrack)
	err := c.State.StoreKVMarshalJSON([]byte("state"), params.CatStateKey_OffsetIndexer, storeTrack)
	if err != nil {
		c.logger.Error("Failed to store offsetIndexer state", "error", err)
		return err
	}
	return nil
}

//// HatspotIndexer is an indexer on NetworkInfo, Stationary-ness, and level 16 cell.
//// It intends to provide another, good, heuristic for napping iOS cats.
//// When these cats use an often-used wifi network which is not moving (wifi/level16),
//// we can assume they are napping because they are in their normal nap hotspot zone.
//func (c *Cat) HatspotIndexer(ctx context.Context, in <-chan cattrack.CatTrack) error {
//	c.logger.Info("Simple offset indexer")
//	defer c.logger.Info("Simple offset indexer complete")
//
//	indexerT := &cattrack.OffsetIndexT{}
//	oldT := &cattrack.OffsetIndexT{}
//
//	oldTrack := cattrack.CatTrack{}
//	if err := c.State.ReadKVUnmarshalJSON(params.CatStateBucket, params.CatStateKey_OffsetIndexer, &oldTrack); err != nil {
//		c.logger.Warn("Did not read offsetIndexer state (new cat?)", "error", err)
//	} else {
//		oldT = indexerT.FromCatTrack(oldTrack).(*cattrack.OffsetIndexT)
//	}
//
//	lastTrack := cattrack.CatTrack{}
//	for track := range in {
//		select {
//		case <-ctx.Done():
//			break
//		default:
//		}
//		lastTrack = track
//		indexing := indexerT.FromCatTrack(track)
//		next := indexerT.Index(oldT, indexing)
//		*oldT = *next.(*cattrack.OffsetIndexT)
//	}
//	c.logger.Info("Simple indexer complete")
//
//	storeTrack := indexerT.ApplyToCatTrack(oldT, lastTrack)
//	err := c.State.StoreKVMarshalJSON([]byte("state"), params.CatStateKey_OffsetIndexer, storeTrack)
//	if err != nil {
//		c.logger.Error("Failed to store offsetIndexer state", "error", err)
//		return err
//	}
//	return nil
//}
