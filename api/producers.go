package api

import (
	"context"
	"github.com/rotblauer/catd/geo/clean"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
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

	cleaned := c.CleanTracks(ctx, wOffsets)
	//cleanDebug, cleanPass := stream.Tee[cattrack.CatTrack](ctx, cleaned)

	//////// P.S. Don't send all tracks to tiled unless development?
	//go func() {
	//	if err := sendToCatTileD(ctx, c, &tiled.PushFeaturesRequestArgs{
	//		SourceSchema: tiled.SourceSchema{
	//			CatID:      c.CatID,
	//			SourceName: "tracks",
	//			LayerName:  "tracks",
	//		},
	//		TippeConfigName: params.TippeConfigNameTracks,
	//		Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
	//		SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
	//	}, cleanDebug); err != nil {
	//		c.logger.Error("Failed to send tracks", "error", err)
	//	}
	//}()

	improved := c.ImprovedActTracks(ctx, cleaned)
	//improved := c.ImprovedActTracks(ctx, cleanPass)
	//improvedPass, improvedDebug := stream.Tee[cattrack.CatTrack](ctx, improved)

	//////// P.S. Don't send all tracks to tiled unless development?
	//go func() {
	//	if err := sendToCatTileD(ctx, c, &tiled.PushFeaturesRequestArgs{
	//		SourceSchema: tiled.SourceSchema{
	//			CatID:      c.CatID,
	//			SourceName: "tracks-improved",
	//			LayerName:  "tracks-improved",
	//		},
	//		TippeConfigName: params.TippeConfigNameTracks,
	//		Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
	//		SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
	//	}, improvedDebug); err != nil {
	//		c.logger.Error("Failed to send tracks", "error", err)
	//	}
	//}()

	//pipeliners := improvedPass
	pipeliners := improved
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
