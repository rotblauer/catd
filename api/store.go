package api

import (
	"context"
	"github.com/rotblauer/catd/app"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/events"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
)

// Store stores incoming CatTracks for one cat to disk.
func Store(ctx context.Context, catID conceptual.CatID, in <-chan *cattrack.CatTrack) (stored <-chan *cattrack.CatTrack, errs <-chan error) {
	storedCh, errCh := make(chan *cattrack.CatTrack), make(chan error)
	/*
		defer close(storedCh)
		defer close(errCh)
	*/
	go func() {
		defer close(storedCh)
		defer close(errCh)

		// Declare our cat-writer and intend to close it on completion.
		// Holding the writer in this closure allows us to use the writer
		// as a batching writer, only opening and closing the target writers once.
		appCat := app.Cat{CatID: catID}
		writer, err := appCat.NewCatWriter()
		if err != nil {
			errCh <- err
			return
		}
		defer func() {
			if err := writer.StoreLastTrack(); err != nil {
				slog.Error("Failed to persist last track", "error", err)
				errCh <- err
			}
			if err := writer.Close(); err != nil {
				slog.Error("Failed to close track writer", "error", err)
				errCh <- err
			}
		}()

		wr, err := writer.TrackWriter()
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
			if err := writer.WriteTrack(wr, ct); err != nil {
				return err
			}

			slog.Log(context.Background(), -5, "Stored cat track", "track", ct.StringPretty())
			events.NewStoredTrackFeed.Send(ct)

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
