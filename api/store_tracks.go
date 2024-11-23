package api

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
)

// StoreTracks stores incoming CatTracks for one cat to disk.
func (c *Cat) StoreTracks(ctx context.Context, in <-chan *cattrack.CatTrack) (stored <-chan *cattrack.CatTrack, errs <-chan error) {
	c.getOrInitState()

	storedCh, errCh := make(chan *cattrack.CatTrack), make(chan error)

	c.logger.Info("Storing cat tracks gz", "cat", c.CatID)

	c.State.Waiting.Add(1)
	go func() {
		defer close(storedCh)
		defer close(errCh)
		defer c.State.Waiting.Done()

		storedN := int64(0)
		defer func() {
			c.logger.Info("Stored cat tracks gz", "count", storedN)
		}()

		wr, err := c.State.NamedGZWriter(flat.TracksFileName)
		if err != nil {
			c.logger.Error("Failed to create track writer", "error", err)
			errCh <- err
			return
		}
		defer func() {
			if err := wr.Close(); err != nil {
				c.logger.Error("Failed to close track writer", "error", err)
				errCh <- err
			}
		}()

		enc := json.NewEncoder(wr.Writer())

		storeResults := stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
			if err := enc.Encode(ct); err != nil {
				slog.Error("Failed to encode cat track gz", "error", err)
				return err
			}
			c.logger.Log(ctx, slog.LevelDebug-1, "Stored cat track", "track", ct.StringPretty())

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
