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
	cleaned := c.CleanTracks(ctx, in)
	improved := c.ImprovedActTracks(ctx, cleaned)
	woffsets := cattrack.WithTimeOffset(ctx, improved)

	areaPipeCh := make(chan cattrack.CatTrack, params.DefaultBatchSize)
	vectorPipeCh := make(chan cattrack.CatTrack, params.DefaultBatchSize)
	simpleIndexerCh := make(chan cattrack.CatTrack, params.DefaultBatchSize)
	stream.TeeMany(ctx, woffsets, areaPipeCh, vectorPipeCh, simpleIndexerCh)

	groundedArea := stream.Filter[cattrack.CatTrack](ctx, clean.FilterGrounded, areaPipeCh)
	g1, g2 := stream.Tee(ctx, groundedArea)

	nPipes := 4
	errs := make(chan error, nPipes)
	go func() { errs <- c.S2IndexTracks(ctx, g1) }()
	go func() { errs <- c.RGeoIndexTracks(ctx, g2) }()
	go func() { errs <- c.CatActPipeline(ctx, vectorPipeCh) }()
	go func() { errs <- c.SimpleIndexer(ctx, simpleIndexerCh) }()

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

func (c *Cat) SimpleIndexer(ctx context.Context, in <-chan cattrack.CatTrack) error {

	c.logger.Info("Simple indexer")
	defer c.logger.Info("Simple indexer complete")

	indexer := &cattrack.OffsetIndexT{}
	old := &cattrack.OffsetIndexT{}
	if err := c.State.ReadKVUnmarshalJSON([]byte("state"), []byte("stacker"), old); err != nil {
		c.logger.Warn("Did not read stacker state (new cat?)", "error", err)
	}

	for track := range in {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		indexing := indexer.FromCatTrack(track)
		next := indexer.Index(old, indexing)
		*old = *next.(*cattrack.OffsetIndexT)
	}

	c.logger.Info("Simple indexer complete")

	return c.State.StoreKVMarshalJSON([]byte("state"), []byte("stacker"), old)
}
