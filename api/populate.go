package api

import (
	"context"
	"github.com/rotblauer/catd/app"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/events"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
)

// PopulateCat persists incoming CatTracks for one cat.
func PopulateCat(ctx context.Context, catID conceptual.CatID, in <-chan *cattrack.CatTrack) (lastErr error) {

	validated := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
		if catID != ct.CatID() {
			slog.Warn("Invalid track, mismatched cat", "want", catID, "got", ct.CatID())
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
	catApp := app.Cat{CatID: catID}
	writer, err := catApp.NewCatWriter()
	if err != nil {
		return err
	}
	defer func() {
		if err := writer.Close(); err != nil {
			slog.Error("Failed to close track writer", "error", err)
		}
	}()

	storeResults := stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
		if err := writer.WriteTrack(ct); err != nil {
			return err
		}

		slog.Debug("Stored cat track", "track", ct.StringPretty())
		events.NewStoredTrackFeed.Send(ct)

		return ct
	}, deduped)

	a, b := stream.Tee(ctx, storeResults)

	storedOK := stream.Transform(ctx, func(t any) *cattrack.CatTrack {
		return t.(*cattrack.CatTrack)
	}, stream.Filter(ctx, func(t any) bool {
		_, ok := t.(*cattrack.CatTrack)
		return ok
	}, a))

	errs := stream.Filter(ctx, func(t any) bool {
		_, ok := t.(error)
		return ok
	}, b)

	catIndexer, err := s2.NewIndexer(catID, app.DatadirRoot, params.S2DefaultCellLevels)
	if err != nil {
		return err
	}
	go func() {
		if err := catIndexer.Index(ctx, storedOK); err != nil {
			slog.Error("Indexer errored", "error", err)
		}
		if err := catIndexer.Close(); err != nil {
			slog.Error("Failed to close indexer", "error", err)
		}
	}()

	// Blocking.
	stream.Sink(ctx, func(t any) {
		lastErr = t.(error)
		slog.Error("Failed to populate CatTrack", "error", lastErr)
	}, errs)
	return lastErr
}
