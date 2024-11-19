package api

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/app"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"sync"
)

// PopulateCat persists incoming CatTracks for one cat.
func PopulateCat(ctx context.Context, catID conceptual.CatID, sort bool, enforceChronology bool, in <-chan *cattrack.CatTrack) (lastErr error) {

	// enforceChronology requires us to reference persisted state
	// before we begin reading input in order to know where we left off.
	// We'll reassign the source channel if necessary.
	// This allows the cat populator to
	// 1. enforce chronology (which is kind of interesting; no edits!)
	// 2. import gracefully
	source := in
	if enforceChronology {
		appCat := app.Cat{CatID: catID}
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

	// Dedupe with hash cache.
	deduped := stream.Filter(ctx, cache.NewDedupePassLRUFunc(), pipedLast)

	// Store em! (Handle errors blocks this function).
	stored, storeErrs := Store(ctx, catID, deduped)

	indexingCh, tripdetectCh := stream.Tee(ctx, stored)

	// S2 indexing pipeline.
	pipelining := &sync.WaitGroup{}
	go S2IndexTracks(ctx, pipelining, catID, indexingCh)
	go func() {
		pipelining.Add(1)
		defer pipelining.Done()

		lapTracks := make(chan *cattrack.CatTrack)
		napTracks := make(chan *cattrack.CatTrack)
		defer close(lapTracks)
		defer close(napTracks)

		cleaned := CleanTracks(ctx, catID, tripdetectCh)
		tripdetected := TripDetectTracks(ctx, catID, cleaned)

		handleTripDetected(ctx, catID, tripdetected)

		// Synthesize new/derivative/aggregate features: LineStrings for laps, Points for naps.

		// Laps
		linestrings := LinestringsFromTracks(ctx, catID, lapTracks)
		laps := SimplifyLinestrings(ctx, catID, linestrings)

		/*
			for lap := range laps {
				// Only complete linestrings (complete trips/laps)
				// are channeled here.
				// Callers wanting the incomplete/partial/unfinished
				// linestring for a cat can use
				// catReader.
			}
		*/

		// Naps
		naps := ClusterPoints(ctx, catID, napTracks)

		for detected := range tripdetected {
			if detected.Properties.MustBool("IsTrip") {
				lapTracks <- detected
			} else {
				napTracks <- detected
			}
		}
	}()
	//if err := TripDetectTracks(ctx, catID, tripdetectCh); err != nil {
	//	slog.Error("Failed to detect trips", "error", err)
	//	// return?
	//}

	// Blocking on store.
	slog.Warn("Blocking on store")
	stream.Sink(ctx, func(e error) {
		lastErr = e
		slog.Error("Failed to populate CatTrack", "error", lastErr)
	}, storeErrs)

	slog.Warn("Blocking on pipelining")
	pipelining.Wait()
	return lastErr
}

func handleTripDetected(ctx context.Context, catID conceptual.CatID, in <-chan *cattrack.CatTrack) {
	appCat := app.Cat{CatID: catID}
	writer, err := appCat.NewCatWriter()
	if err != nil {
		slog.Error("Failed to create cat writer", "error", err)
		return
	}

	toMoving, toStationary := stream.Tee(ctx, in)
	moving := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
		//return ct.ID == 1
		return ct.Properties.MustBool("IsTrip")
	}, toMoving)
	stationary := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
		//return ct.ID == 0
		return !ct.Properties.MustBool("IsTrip")
	}, toStationary)

	// TODO: Coalesce moving points into linestrings, and stationary ones into stops.

	doneMoving := make(chan struct{})
	doneStationary := make(chan struct{})

	movingWriter, err := writer.CustomWriter("moving.geojson.gz")
	if err != nil {
		slog.Error("Failed to create moving writer", "error", err)
		return
	}
	defer movingWriter.Close()

	stationaryWriter, err := writer.CustomWriter("stationary.geojson.gz")
	if err != nil {
		slog.Error("Failed to create stationary writer", "error", err)
		return
	}
	defer stationaryWriter.Close()

	go func() {
		stream.Drain(ctx, stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
			slog.Debug("Writing moving track", "track", ct.StringPretty())
			if err := json.NewEncoder(movingWriter).Encode(ct); err != nil {
				slog.Error("Failed to write moving track", "error", err)
			}
			return nil
		}, moving))
		doneMoving <- struct{}{}
	}()

	go func() {
		stream.Drain(ctx, stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
			slog.Debug("Writing stationary track", "track", ct.StringPretty())
			if err := json.NewEncoder(stationaryWriter).Encode(ct); err != nil {
				slog.Error("Failed to write stationary track", "error", err)
			}
			return nil
		}, stationary))
		doneStationary <- struct{}{}
	}()

	// Block on both writers, unordered.
	for i := 0; i < 2; i++ {
		select {
		case <-doneMoving:
			doneMoving = nil
		case <-doneStationary:
			doneStationary = nil
		}
	}
}
