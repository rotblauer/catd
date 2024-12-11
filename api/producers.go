package api

import (
	"context"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
)

func (c *Cat) ProducerPipelines(ctx context.Context, in <-chan cattrack.CatTrack) error {
	// Clean and improve tracks for pipeline handlers.
	betterTracks := TracksWithOffset(ctx, c.ImprovedActTracks(ctx, c.CleanTracks(ctx, in)))
	_, v := stream.Tee(ctx, betterTracks)
	//groundedArea := stream.Filter[cattrack.CatTrack](ctx, clean.FilterGrounded, a)
	return c.CatActPipeline(ctx, v)
	//errs := make(chan error, 2)
	//go func() {
	//	errs <- c.S2IndexTracks(ctx, groundedArea)
	//}()
	//go func() {
	//	errs <- c.CatActPipeline(ctx, vectorPipeCh)
	//}()
	//for i := 0; i < 2; i++ {
	//	select {
	//	case err := <-errs:
	//		if err != nil {
	//			return err
	//		}
	//	case <-ctx.Done():
	//		return ctx.Err()
	//	}
	//}
	//return nil
}
