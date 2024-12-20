package api

import (
	"context"
	"github.com/rotblauer/catd/geo/clean"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
)

func (c *Cat) ProducerPipelines(ctx context.Context, in <-chan cattrack.CatTrack) error {

	c.logger.Info("Producer pipelines")
	defer c.logger.Info("Producer pipelines complete")

	// Clean and improve tracks for pipeline handlers.
	cleaned := c.CleanTracks(ctx, in)
	improved := c.ImprovedActTracks(ctx, cleaned)
	woffsets := TracksWithOffset(ctx, improved)

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

	indexer := &cattrack.StackerV1{}
	old := &cattrack.StackerV1{}
	if err := c.State.ReadKVUnmarshalJSON([]byte("state"), []byte("stacker"), old); err != nil {
		c.logger.Warn("Failed to read stacker state (new cat?)", "error", err)
	}

	for track := range in {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		indexing := indexer.FromCatTrack(track)
		next := indexer.Index(old, indexing)
		*old = *next.(*cattrack.StackerV1)
	}

	c.logger.Info("Simple indexer complete")

	return c.State.StoreKVMarshalJSON([]byte("state"), []byte("stacker"), old)
}
