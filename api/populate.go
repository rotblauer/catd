package api

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/app"
	"github.com/rotblauer/catd/catdb/cache"
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

	// S2 indexing pipeline.
	go func(in <-chan *cattrack.CatTrack) {
		pipelining.Add(1)
		defer pipelining.Done()
		cellIndexer, err := s2.NewCellIndexer(catID, params.DatadirRoot, params.S2DefaultCellLevels, params.DefaultBatchSize)
		if err != nil {
			slog.Error("Failed to initialize indexer", "error", err)
			return
		}
		// Blocking.
		if err := cellIndexer.Index(ctx, in); err != nil {
			slog.Error("CellIndexer errored", "error", err)
		}
		if err := cellIndexer.Close(); err != nil {
			slog.Error("Failed to close indexer", "error", err)
		}
	}(indexingCh)

	// Lap-o-matic pipeline.
	go func(in <-chan *cattrack.CatTrack) {
		pipelining.Add(1)
		defer pipelining.Done()

		accurate := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
			slog.Debug("Filtering accuracy", "track", ct.StringPretty())
			return cleaner.FilterAccuracy(ct)
		}, in)
		slow := stream.Filter(ctx, cleaner.FilterSpeed, accurate)
		low := stream.Filter(ctx, cleaner.FilterElevation, slow)
		uncanyoned := cleaner.WangUrbanCanyonFilter(ctx, low)
		unteleported := cleaner.TeleportationFilter(ctx, uncanyoned)

		td := tripdetector.NewTripDetector(params.DefaultTripDetectorConfig)
		tripdetected := stream.Transform(ctx, func(ct *cattrack.CatTrack) *cattrack.CatTrack {
			slog.Debug("Detecting trips", "track", ct.StringPretty())
			_ = td.Add(ct)
			ct.Properties["IsTrip"] = td.Tripping
			ct.Properties["MotionStateReason"] = td.MotionStateReason
			return ct
		}, unteleported)

		// Filter out the resets and inits.
		// Resets happen when the trip detector is reset after a signal loss.
		// Inits happen when the trip detector is initialized.
		tripdetectedValid := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
			return ct.Properties.MustString("MotionStateReason") != "init" &&
				ct.Properties.MustString("MotionStateReason") != "reset"
		}, tripdetected)

		toMoving, toStationary := stream.Tee(ctx, tripdetectedValid)
		moving := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
			return ct.Properties.MustBool("IsTrip")
		}, toMoving)
		stationary := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
			return !ct.Properties.MustBool("IsTrip")
		}, toStationary)

		// TODO: Coalesce moving points into linestrings, and stationary ones into stops.

		movingWriter, err := writer.CustomWriter("moving.geojson.gz")
		if err != nil {
			slog.Error("Failed to create moving writer", "error", err)
			return
		}
		defer movingWriter.Close()
		wroteMoving := stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
			slog.Debug("Writing moving track", "track", ct.StringPretty())
			if err := json.NewEncoder(movingWriter).Encode(ct); err != nil {
				slog.Error("Failed to write moving track", "error", err)
			}
			return nil
		}, moving)

		stationaryWriter, err := writer.CustomWriter("stationary.geojson.gz")
		if err != nil {
			slog.Error("Failed to create stationary writer", "error", err)
			return
		}
		defer stationaryWriter.Close()
		wroteStationary := stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
			slog.Debug("Writing stationary track", "track", ct.StringPretty())
			if err := json.NewEncoder(stationaryWriter).Encode(ct); err != nil {
				slog.Error("Failed to write stationary track", "error", err)
			}
			return nil
		}, stationary)

		go stream.Drain(ctx, wroteStationary)
		stream.Drain(ctx, wroteMoving)

	}(tripDetectingCh)

	// Blocking on store.
	stream.Sink(ctx, func(t any) {
		lastErr = t.(error)
		slog.Error("Failed to populate CatTrack", "error", lastErr)
	}, storeErrs)

	pipelining.Wait()
	return lastErr
}
