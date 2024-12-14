package api

import (
	"context"
	"github.com/rotblauer/catd/geo/clean"
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

	areaPipeCh, vectorPipeCh := stream.Tee(ctx, woffsets)
	groundedArea := stream.Filter[cattrack.CatTrack](ctx, clean.FilterGrounded, areaPipeCh)

	nPipes := 2
	errs := make(chan error, nPipes)
	go func() { errs <- c.S2IndexTracks(ctx, groundedArea) }()
	go func() { errs <- c.CatActPipeline(ctx, vectorPipeCh) }()

	c.logger.Info("Producer pipelines waiting for completion")

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
