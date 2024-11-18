package api

import (
	"context"
	"github.com/rotblauer/catd/app"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/events"
	"github.com/rotblauer/catd/geo/cleaner"
	"github.com/rotblauer/catd/geo/tripdetector"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"sync"
)

// PopulateCat persists incoming CatTracks for one cat.
func PopulateCat(ctx context.Context, catID conceptual.CatID, sort bool, enforceChronology bool, in <-chan *cattrack.CatTrack) (lastErr error) {

	// Declare our cat-writer and intend to close it on completion.
	// Holding the writer in this closure allows us to use the writer
	// as a batching writer, only opening and closing the target writers once.
	appCat := app.Cat{CatID: catID}
	writer, err := appCat.NewCatWriter()
	if err != nil {
		return err
	}
	defer func() {
		if err := writer.PersistLastTrack(); err != nil {
			slog.Error("Failed to persist last track", "error", err)
		}
		if err := writer.Close(); err != nil {
			slog.Error("Failed to close track writer", "error", err)
		}
	}()

	// enforceChronology requires us to reference persisted state
	// before we begin reading input in order to know where we left off.
	// We'll reassign the source channel if necessary.
	// This allows the cat populator to
	// 1. enforce chronology (which is kind of interesting; no edits!)
	// 2. import gracefully
	source := in
	if enforceChronology {
		if reader, err := appCat.NewCatReader(); err == nil {
			last, err := reader.ReadLastTrack()
			if err == nil {
				lastTrackTime, _ := last.Time()
				source = stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
					t, err := ct.Time()
					if err != nil {
						return false
					}
					return t.After(lastTrackTime)
				}, in)
			}
		}
	}

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
	}, source)

	sanitized := stream.Transform(ctx, cattrack.Sanitize, validated)

	// Sorting is obviously a little slower than not sorting.
	pipedLast := sanitized
	if sort {
		// Catch is the batch.
		sorted := stream.CatchSizeSorting(ctx, params.DefaultBatchSize, cattrack.SortFunc, sanitized)
		pipedLast = sorted
	}
	deduped := stream.Filter(ctx, cache.NewDedupePassLRUFunc(), pipedLast)

	storeResults := stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
		if err := writer.WriteTrack(ct); err != nil {
			return err
		}

		slog.Log(context.Background(), -5, "Stored cat track", "track", ct.StringPretty())
		events.NewStoredTrackFeed.Send(ct)

		return ct
	}, deduped)

	a, b := stream.Tee(ctx, storeResults)
	storeOKs := stream.Transform(ctx, func(t any) *cattrack.CatTrack {
		return t.(*cattrack.CatTrack)
	}, stream.Filter(ctx, func(t any) bool {
		_, ok := t.(*cattrack.CatTrack)
		return ok
	}, a))
	storeErrs := stream.Filter(ctx, func(t any) bool {
		_, ok := t.(error)
		return ok
	}, b)

	indexingCh, tripDetectingCh := stream.Tee(ctx, storeOKs)

	// TODO: This might be optional.
	// Normal track-pushing over HTTP probably doesn't need to wait.
	pipelining := sync.WaitGroup{}

	go func() {
		pipelining.Add(1)
		defer pipelining.Done()
		cellIndexer, err := s2.NewCellIndexer(catID, params.DatadirRoot, params.S2DefaultCellLevels, params.DefaultBatchSize)
		if err != nil {
			slog.Error("Failed to initialize indexer", "error", err)
			return
		}
		// Blocking.
		if err := cellIndexer.Index(ctx, indexingCh); err != nil {
			slog.Error("CellIndexer errored", "error", err)
		}
		if err := cellIndexer.Close(); err != nil {
			slog.Error("Failed to close indexer", "error", err)
		}
	}()

	go func() {
		// Lapomatic pipeline.
		// Implement me.
		pipelining.Add(1)
		defer pipelining.Done()

		noYeagerCh := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
			return ct.Properties.MustFloat64("Elevation") < common.ElevationOfTroposphere
		}, stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
			return ct.Properties.MustFloat64("Speed") < common.SpeedOfSound
		}, tripDetectingCh))

		uncanyoned := cleaner.WangUrbanCanyonFilter(ctx, noYeagerCh)

		_ = tripdetector.NewTripDetector(params.DefaultTripDetectorConfig)
	}()

	// Blocking on store.
	stream.Sink(ctx, func(t any) {
		lastErr = t.(error)
		slog.Error("Failed to populate CatTrack", "error", lastErr)
	}, storeErrs)

	pipelining.Wait()
	return lastErr
}
