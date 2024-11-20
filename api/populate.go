package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/paulmach/orb/simplify"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
)

// Populate persists incoming CatTracks for one cat.
func (c *Cat) Populate(ctx context.Context, sort bool, enforceChronology bool, in <-chan *cattrack.CatTrack) (lastErr error) {

	// Blocking.
	c.logger.Info("Populate blocking on lock state")
	_, err := c.WithState(false)
	if err != nil {
		c.logger.Error("Failed to create cat state", "error", err)
		return
	}
	c.logger.Info("Populate has the state conn")
	defer func() {
		if err := c.State.StoreLastTrack(); err != nil {
			c.logger.Error("Failed to persist last track", "error", err)
		}
		if err := c.State.Close(); err != nil {
			c.logger.Error("Failed to close cat state", "error", err)
		} else {
			c.logger.Info("Closed cat state")
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
		last, err := c.State.ReadLastTrack()
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

	validated := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
		checkCatID := ct.CatID()
		if c.CatID != checkCatID {
			c.logger.Warn("Invalid track, mismatched cat", "want", fmt.Sprintf("%q", c.CatID), "got", fmt.Sprintf("%q", checkCatID))
			return false
		}
		if err := ct.Validate(); err != nil {
			c.logger.Warn("Invalid track", "error", err)
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
	stored, storeErrs := c.Store(ctx, deduped)

	indexingCh, tripdetectCh := stream.Tee(ctx, stored)

	// S2 indexing pipeline.
	go c.S2IndexTracks(ctx, indexingCh)
	go func() {
		lapTracks := make(chan *cattrack.CatTrack)
		napTracks := make(chan *cattrack.CatTrack)
		defer close(lapTracks)
		defer close(napTracks)

		cleaned := c.CleanTracks(ctx, tripdetectCh)
		tripdetected := c.TripDetectTracks(ctx, cleaned)

		// Synthesize new/derivative/aggregate features: LineStrings for laps, Points for naps.

		// Laps
		completedLaps := c.LapTracks(ctx, lapTracks)
		longCompletedLaps := stream.Filter(ctx, func(ct *cattrack.CatLap) bool {
			duration := ct.Properties["Time"].(map[string]any)["Duration"].(float64)
			return duration > 120
		}, completedLaps)
		simplifier := simplify.DouglasPeucker(params.DefaultSimplifierConfig.DouglasPeuckerThreshold)
		simplified := stream.Transform(ctx, func(ct *cattrack.CatLap) *cattrack.CatLap {
			ct.Geometry = simplifier.Simplify(ct.Geometry)
			return ct
		}, longCompletedLaps)

		c.State.Waiting.Add(1)
		go sinkToCatJSONGZFile(ctx, c, "laps.geojson.gz", simplified)

		// Naps
		completedNaps := c.NapTracks(ctx, napTracks)
		longCompletedNaps := stream.Filter(ctx, func(ct *cattrack.CatNap) bool {
			duration := ct.Properties["Time"].(map[string]any)["Duration"].(float64)
			return duration > 120
		}, completedNaps)

		c.State.Waiting.Add(1)
		go sinkToCatJSONGZFile(ctx, c, "naps.geojson.gz", longCompletedNaps)

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
	c.logger.Info("Blocking on store cat tracks gz")
	stream.Sink(ctx, func(e error) {
		lastErr = e
		c.logger.Error("Failed to populate CatTrack", "error", lastErr)
	}, storeErrs)

	c.logger.Info("Blocking on cat pipelines")
	c.State.Waiting.Wait()
	return lastErr
}

func sinkToCatJSONGZFile[T any](ctx context.Context, c *Cat, name string, in <-chan *T) {
	if c.State == nil {
		_, err := c.WithState(false)
		if err != nil {
			c.logger.Error("Failed to create cat state", "error", err)
			return
		}
	}
	defer c.logger.Info("Sunk stream to gz file", "name", name)
	defer c.State.Waiting.Done()

	customWriter, err := c.State.CustomGZWriter(name)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		return
	}
	defer func() {
		if err := customWriter.Close(); err != nil {
			c.logger.Error("Failed to close writer", "error", err)
		}
	}()

	// Blocking.
	stream.Sink(ctx, func(a *T) {
		if a == nil {
			c.logger.Warn("Refusing to encode nil to gzip file", "file", name)
			return
		}
		enc := json.NewEncoder(customWriter)
		if err := enc.Encode(a); err != nil {
			c.logger.Error("Failed to write", "error", err)
		}
	}, in)
}

//func handleTripDetected(ctx context.Context, catID conceptual.CatID, in <-chan *cattrack.CatTrack) {
//	appCat := state.Cat{CatID: catID}
//	writer, err := appCat.NewCatState()
//	if err != nil {
//		slog.Error("Failed to create cat writer", "error", err)
//		return
//	}
//
//	toMoving, toStationary := stream.Tee(ctx, in)
//	moving := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
//		//return ct.ID == 1
//		return ct.Properties.MustBool("IsTrip")
//	}, toMoving)
//	stationary := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
//		//return ct.ID == 0
//		return !ct.Properties.MustBool("IsTrip")
//	}, toStationary)
//
//	// TODO: Coalesce moving points into linestrings, and stationary ones into stops.
//
//	doneMoving := make(chan struct{})
//	doneStationary := make(chan struct{})
//
//	movingWriter, err := writer.CustomGZWriter("moving.geojson.gz")
//	if err != nil {
//		slog.Error("Failed to create moving writer", "error", err)
//		return
//	}
//	defer movingWriter.Close()
//
//	stationaryWriter, err := writer.CustomGZWriter("stationary.geojson.gz")
//	if err != nil {
//		slog.Error("Failed to create stationary writer", "error", err)
//		return
//	}
//	defer stationaryWriter.Close()
//
//	go func() {
//		stream.Drain(ctx, stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
//			slog.Debug("Writing moving track", "track", ct.StringPretty())
//			if err := json.NewEncoder(movingWriter).Encode(ct); err != nil {
//				slog.Error("Failed to write moving track", "error", err)
//			}
//			return nil
//		}, moving))
//		doneMoving <- struct{}{}
//	}()
//
//	go func() {
//		stream.Drain(ctx, stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
//			slog.Debug("Writing stationary track", "track", ct.StringPretty())
//			if err := json.NewEncoder(stationaryWriter).Encode(ct); err != nil {
//				slog.Error("Failed to write stationary track", "error", err)
//			}
//			return nil
//		}, stationary))
//		doneStationary <- struct{}{}
//	}()
//
//	// Block on both writers, unordered.
//	for i := 0; i < 2; i++ {
//		select {
//		case <-doneMoving:
//			doneMoving = nil
//		case <-doneStationary:
//			doneStationary = nil
//		}
//	}
//}
