package api

import (
	"context"
	"github.com/rotblauer/catd/app"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/events"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
)

func storeTracksGZ(in <-chan *cattrack.CatTrack) <-chan any {

}

// Populate persists incoming CatTracks for one cat.
func Populate(ctx context.Context, in <-chan *cattrack.CatTrack) error {

	validated := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
		if err := ct.Validate(); err != nil {
			slog.Warn("Invalid track", "error", err)
			return false
		}
		return true
	}, in)

	sanitized := stream.Transform(ctx, func(ct *cattrack.CatTrack) *cattrack.CatTrack {
		ct.Sanitize()
		return ct
	}, validated)

	deduped := stream.Filter(ctx, cache.DedupePassLRU, sanitized)

	// Declare our cat-writer and intend to close it on completion.
	// Holding the writer in this closure allows us to use the writer
	// as a batching writer, only opening and closing the target writers once.
	var writer *app.CatWriter
	defer func() {
		if writer != nil {
			if err := writer.Close(); err != nil {
				slog.Error("Failed to close track writer", "error", err)
			}
		}
	}()

	initWriterFromCatTrack := func(ct *cattrack.CatTrack) error {
		cat := app.Cat{CatID: ct.CatID()}
		var err error
		writer, err = cat.NewCatWriter()
		if err != nil {
			return err
		}
		return nil
	}

	stored := stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
		// The first feature will define the Cat/Writer for the rest of the batch.
		if writer == nil {
			if err := initWriterFromCatTrack(ct); err != nil {
				return err
			}
		} else if writer.CatID != ct.CatID() {
			// If, for some reason, we're ingesting a mix of cats' tracks,
			// then we need to close and reassign the Cat/Writer.
			if err := writer.Close(); err != nil {
				return err
			}
			if err := initWriterFromCatTrack(ct); err != nil {
				return err
			}
		}

		if err := writer.WriteTrack(ct); err != nil {
			return err
		}

		// We did it!
		slog.Debug("Stored track", "track", ct.StringPretty())
		events.NewStoredTrackFeed.Send(ct)

		return ct
	}, deduped)

	/*



		// S2 Unique-Cell Indexing


		var s2Indexer *s2.Indexer
		defer func() {
			if s2Indexer != nil {
				if err := s2Indexer.Close(); err != nil {
					slog.Error("Failed to close S2-Indexer", "error", err)
				}
			}
		}()

		initS2IndexerFromCatTrack := func(ct *cattrack.CatTrack) (err error) {
			s2Indexer, err = s2.NewIndexer(ct.CatID(), app.DatadirRoot, []s2.CellLevel{
				s2.CellLevel23, s2.CellLevel16,
			})
			return
		}

		indexed := stream.Transform(ctx)

		// Tile generation.


		// Trip detection.

	*/

	var lastErr error
	stream.Sink(ctx, stored, func(result any) {
		if result == nil {
			return
		}
		switch t := result.(type) {
		case error:
			slog.Error("Failed to populate CatTrack", "error", t)
			lastErr = t
		case *cattrack.CatTrack:
		}
	})

	return lastErr
}
