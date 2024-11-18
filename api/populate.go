package api

import (
	"context"
	"github.com/rotblauer/catd/app"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/events"
	"github.com/rotblauer/catd/params"
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

	wr, err := writer.TrackWriter()
	if err != nil {
		slog.Error("Failed to create track writer", "error", err)
		return err
	}
	defer func() {
		if err := wr.Close(); err != nil {
			slog.Error("Failed to close track writer", "error", err)
		}
	}()

	storeResults := stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
		if err := writer.WriteTrack(wr, ct); err != nil {
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

	//go stream.Drain(ctx, storeOKs)
	//go func() {
	//	for range storeOKs {
	//		do nothing
	//}
	//}()

	indexingCh, tripdetectCh := stream.Tee(ctx, storeOKs)
	//go stream.Drain(ctx, tripdetectCh)

	// S2 indexing pipeline.
	pipelining := &sync.WaitGroup{}
	go S2IndexTracks(ctx, pipelining, catID, indexingCh)
	go func() {
		cleaned := CleanTracks(ctx, catID, tripdetectCh)
		TripDetectTracks(ctx, pipelining, catID, cleaned)

	}()
	//if err := TripDetectTracks(ctx, catID, tripdetectCh); err != nil {
	//	slog.Error("Failed to detect trips", "error", err)
	//	// return?
	//}

	// Blocking on store.
	slog.Warn("Blocking on store")
	stream.Sink(ctx, func(t any) {
		lastErr = t.(error)
		slog.Error("Failed to populate CatTrack", "error", lastErr)
	}, storeErrs)

	slog.Warn("Blocking on pipelining")
	pipelining.Wait()
	return lastErr
}
