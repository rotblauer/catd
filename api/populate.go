package api

import (
	"context"
	"fmt"
	"github.com/rotblauer/catd/app"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
)

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

	stored := stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
		slog.Info("Storing track", "track", ct.StringPretty())

		// The first feature will define the Cat for the rest of the batch.
		if writer == nil {
			cat := app.Cat{CatID: ct.CatID()}
			var err error
			writer, err = cat.NewCatWriter()
			if err != nil {
				return err
			}
		}
		if writer.CatID != ct.CatID() {
			return fmt.Errorf("mismatched cats' tracks")
		}
		if err := writer.WriteTrack(ct); err != nil {
			return err
		}
		return ct
	}, deduped)

	var lastErr error
	for result := range stored {
		if result == nil {
			continue
		}
		switch t := result.(type) {
		case error:
			slog.Error("Failed to populate CatTrack", "error", t)
			lastErr = t
		case *cattrack.CatTrack:
		}
	}
	return lastErr
}
