package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/paulmach/orb/simplify"
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
		checkCatID := ct.CatID()
		if catID != checkCatID {
			slog.Warn("Invalid track, mismatched cat", "want", fmt.Sprintf("%q", catID), "got", fmt.Sprintf("%q", checkCatID))
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
		sorted := stream.CatchSizeSorting(ctx, params.DefaultBatchSize,
			cattrack.SortFunc, sanitized)
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

		// Synthesize new/derivative/aggregate features: LineStrings for laps, Points for naps.

		// Laps
		completedLaps := LapTracks(ctx, catID, lapTracks)
		longCompletedLaps := stream.Filter(ctx, func(ct *cattrack.CatLap) bool {
			duration := ct.Properties["Time"].(map[string]any)["Duration"].(float64)
			return duration > 120
		}, completedLaps)
		simplifier := simplify.DouglasPeucker(params.DefaultSimplifierConfig.DouglasPeuckerThreshold)
		simplified := stream.Transform(ctx, func(ct *cattrack.CatLap) *cattrack.CatLap {
			ct.Geometry = simplifier.Simplify(ct.Geometry)
			return ct
		}, longCompletedLaps)
		go sinkToCatJSONGZFile(ctx, catID, "laps.geojson.gz", simplified)

		// Naps
		completedNaps := NapTracks(ctx, catID, napTracks)
		longCompletedNaps := stream.Filter(ctx, func(ct *cattrack.CatNap) bool {
			duration := ct.Properties["Time"].(map[string]any)["Duration"].(float64)
			return duration > 120
		}, completedNaps)
		go sinkToCatJSONGZFile(ctx, catID, "naps.geojson.gz", longCompletedNaps)

		// Block on tripdetect.
		for detected := range tripdetected {
			if detected.Properties.MustBool("IsTrip") {
				lapTracks <- detected
			} else {
				napTracks <- detected
			}
		}
	}()

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

func sinkToCatJSONGZFile[T any](ctx context.Context, catID conceptual.CatID, name string, in <-chan T) {
	appCat := app.Cat{CatID: catID}
	writer, err := appCat.NewCatWriter()
	if err != nil {
		slog.Error("Failed to create cat writer", "error", err)
		return
	}
	defer writer.Close()

	lapsWriter, err := writer.CustomWriter(name)
	if err != nil {
		slog.Error("Failed to create custom writer", "error", err)
		return
	}
	defer lapsWriter.Close()

	enc := json.NewEncoder(lapsWriter)

	// Blocking.
	stream.Sink(ctx, func(a T) {
		if err := enc.Encode(a); err != nil {
			slog.Error("Failed to write", "error", err)
		}
	}, in)
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
