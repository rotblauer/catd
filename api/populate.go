package api

import (
	"context"
	"github.com/rotblauer/catd/app"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/events"
	"github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
)

// PopulateCat persists incoming CatTracks for one cat.
func PopulateCat(ctx context.Context, cat conceptual.CatID, in <-chan *cattrack.CatTrack) error {

	validated := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
		if cat != ct.CatID() {
			slog.Warn("Invalid track, mismatched cat", "want", cat, "got", ct.CatID())
			return false
		}
		if err := ct.Validate(); err != nil {
			slog.Warn("Invalid track", "error", err)
			return false
		}
		return true
	}, in)

	sanitized := stream.Transform(ctx, cattrack.Sanitize, validated)
	sorted := stream.CatchSizeSorting(ctx, 1000, cattrack.Sorter, sanitized)
	deduped := stream.Filter(ctx, cache.NewDedupePassLRUFunc(), sorted)

	// Declare our cat-writer and intend to close it on completion.
	// Holding the writer in this closure allows us to use the writer
	// as a batching writer, only opening and closing the target writers once.
	catApp := app.Cat{CatID: cat}
	writer, err := catApp.NewCatWriter()
	if err != nil {
		return err
	}
	defer func() {
		if err := writer.Close(); err != nil {
			slog.Error("Failed to close track writer", "error", err)
		}
	}()

	stored := stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
		if err := writer.WriteTrack(ct); err != nil {
			return err
		}

		slog.Debug("Stored cat track", "track", ct.StringPretty())
		events.NewStoredTrackFeed.Send(ct)

		return ct
	}, deduped)

	a, b := stream.Tee(ctx, stored)

	catIndexer, err := s2.NewIndexer(cat, app.DatadirRoot, s2.DefaultCellLevels)
	if err != nil {
		return err
	}

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
	stream.Sink(ctx, func(result any) {
		if result == nil {
			return
		}
		switch t := result.(type) {
		case error:
			slog.Error("Failed to populate CatTrack", "error", t)
			lastErr = t
		case *cattrack.CatTrack:
		}
	}, stored)

	return lastErr
}
