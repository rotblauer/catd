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
	"time"
)

// Populate persists incoming CatTracks for one cat.
func (c *Cat) Populate(ctx context.Context, sort bool, in <-chan *cattrack.CatTrack) (lastErr error) {

	// Blocking.
	c.logger.Info("Populate blocking on lock state")
	_, err := c.WithState(false)
	if err != nil {
		c.logger.Error("Failed to create cat state", "error", err)
		return
	}
	c.logger.Info("Populate has the lock on state conn")
	started := time.Now()
	defer func() {
		if err := c.State.Close(); err != nil {
			c.logger.Error("Failed to close cat state", "error", err)
		} else {
			c.logger.Info("Closed cat state")
		}
		c.logger.Info("Populate done", "elapsed", time.Since(started).Round(time.Millisecond))
	}()

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
	}, in)

	sanitized := stream.Transform(ctx, cattrack.Sanitize, validated)

	// Sorting is obviously a little slower than not sorting.
	pipedLast := sanitized
	if sort {
		// Catch is the new batch.
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
	go c.TripDetectionPipeline(ctx, tripdetectCh)

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

func (c *Cat) TripDetectionPipeline(ctx context.Context, in <-chan *cattrack.CatTrack) {
	lapTracks := make(chan *cattrack.CatTrack)
	napTracks := make(chan *cattrack.CatTrack)
	defer close(lapTracks)
	defer close(napTracks)

	cleaned := c.CleanTracks(ctx, in)
	tripdetected := c.TripDetectTracks(ctx, cleaned)

	// Synthesize new/derivative/aggregate features:
	// ... LineStrings for laps, Points for naps.

	// LapTracks will send completed laps. Incomplete laps are persisted in KV
	// and restored on cat restart.
	completedLaps := c.LapTracks(ctx, lapTracks)
	filterLaps := stream.Filter(ctx, func(ct *cattrack.CatLap) bool {
		duration := ct.Properties["Time"].(map[string]any)["Duration"].(float64)
		return duration > 120
	}, completedLaps)

	// Simplify the lap geometry.
	simplifier := simplify.DouglasPeucker(params.DefaultSimplifierConfig.DouglasPeuckerThreshold)
	simplified := stream.Transform(ctx, func(ct *cattrack.CatLap) *cattrack.CatLap {
		ct.Geometry = simplifier.Simplify(ct.Geometry)
		return ct
	}, filterLaps)

	// Tee the final laps for storage and tiling.
	sinkLaps, tileLaps := stream.Tee(ctx, simplified)
	// Storage
	go sinkToCatJSONGZFile(ctx, c, "laps.geojson.gz", sinkLaps)
	go sinkToCatJSONGZFile(ctx, c, "laps_edge.geojson.gz", tileLaps)

	// NapTracks will send completed naps. Incomplete naps are persisted in KV
	// and restored on cat restart.
	completedNaps := c.NapTracks(ctx, napTracks)
	filteredNaps := stream.Filter(ctx, func(ct *cattrack.CatNap) bool {
		duration := ct.Properties["Time"].(map[string]any)["Duration"].(float64)
		return duration > 120
	}, completedNaps)

	sinkNaps, tileNaps := stream.Tee(ctx, filteredNaps)
	// Storage
	go sinkToCatJSONGZFile(ctx, c, "naps.geojson.gz", sinkNaps)
	go sinkToCatJSONGZFile(ctx, c, "naps_edge.geojson.gz", tileNaps)

	// TODO I tink I want to trigger a global tile update here,
	// or to fire an event that can be listened to by a global tiling service.

	// Block on tripdetect.
	for detected := range tripdetected {
		if detected.Properties.MustBool("IsTrip") {
			lapTracks <- detected
		} else {
			napTracks <- detected
		}
	}
}

func sinkToCatJSONGZFile[T any](ctx context.Context, c *Cat, name string, in <-chan *T) {
	c.getOrInitState()

	c.State.Waiting.Add(1)
	defer c.State.Waiting.Done()

	defer c.logger.Info("Sunk stream to gz file", "name", name)

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
