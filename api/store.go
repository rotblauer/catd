package api

import (
	"context"
	"github.com/rotblauer/catd/events"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
)

// Store stores incoming CatTracks for one cat to disk.
func (c *Cat) Store(ctx context.Context, in <-chan *cattrack.CatTrack) (stored <-chan *cattrack.CatTrack, errs <-chan error) {
	storedCh, errCh := make(chan *cattrack.CatTrack), make(chan error)

	if c.State == nil {
		_, err := c.WithState(false)
		if err != nil {
			slog.Error("Failed to create cat state", "error", err)
			return
		}
	}
	slog.Info("Storing cat tracks gz", "cat", c.CatID)
	c.State.Waiting.Add(1)

	go func() {
		defer close(storedCh)
		defer close(errCh)
		defer c.State.Waiting.Done()

		storedN := int64(0)
		defer func() {
			slog.Info("Stored cat tracks gz", "cat", c.CatID, "count", storedN)
		}()

		wr, err := c.State.TrackGZWriter()
		if err != nil {
			slog.Error("Failed to create track writer", "error", err)
			errCh <- err
			return
		}
		defer func() {
			if err := wr.Close(); err != nil {
				slog.Error("Failed to close track writer", "error", err)
				errCh <- err
			}
		}()

		storeResults := stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
			if err := c.State.WriteTrack(wr, ct); err != nil {
				return err
			}

			slog.Log(ctx, slog.LevelDebug-1, "Stored cat track", "track", ct.StringPretty())
			events.NewStoredTrackFeed.Send(ct)
			storedN++

			return ct
		}, in)

		// Block on sending stored results to respective channels,
		// but permitting context interruption.
		for result := range storeResults {
			select {
			case <-ctx.Done():
				return
			default:
			}

			switch t := result.(type) {
			case error:
				errCh <- t
			case *cattrack.CatTrack:
				storedCh <- t
			default:
				panic("impossible")
			}
		}
	}()

	return storedCh, errCh
}
