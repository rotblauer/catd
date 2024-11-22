package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/paulmach/orb/simplify"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/tiler"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
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

	// StoreTracks em! (Handle errors blocks this function).
	stored, storeErrs := c.StoreTracks(ctx, deduped)

	indexingCh, tripdetectCh := stream.Tee(ctx, stored)

	// S2 indexing pipeline. Stateful/cat.
	go c.S2IndexTracks(ctx, indexingCh)
	// Trip detection pipeline. Laps, naps. Stateful/cat.
	go c.TripDetectionPipeline(ctx, tripdetectCh)

	// Block on any store errors, returning last.
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

	// Synthesize new/derivative/aggregate features:
	// ... LineStrings for laps, Points for naps.

	// TrackLaps will send completed laps. Incomplete laps are persisted in KV
	// and restored on cat restart.
	completedLaps := c.TrackLaps(ctx, lapTracks)
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

	// End of the line for all cat laps...
	sinkLaps, sendLaps := stream.Tee(ctx, simplified)

	c.State.Waiting.Add(1)
	go sinkToCatJSONGZFile(ctx, c, flat.LapsFileName, sinkLaps)

	c.State.Waiting.Add(1)
	go sendToCatRPCClient(ctx, c, &tiler.PushFeaturesRequestArgs{
		SourceSchema: tiler.SourceSchema{
			CatID:      c.CatID,
			SourceName: "laps",
			LayerName:  "laps",
		},
		TippeConfig: params.TippeConfigNameLaps,
	}, sendLaps)

	// TrackNaps will send completed naps. Incomplete naps are persisted in KV
	// and restored on cat restart.
	completedNaps := c.TrackNaps(ctx, napTracks)
	filteredNaps := stream.Filter(ctx, func(ct *cattrack.CatNap) bool {
		duration := ct.Properties["Time"].(map[string]any)["Duration"].(float64)
		return duration > 120
	}, completedNaps)

	// End of the line for all cat naps...
	sinkNaps, sendNaps := stream.Tee(ctx, filteredNaps)

	c.State.Waiting.Add(1)
	go sinkToCatJSONGZFile(ctx, c, flat.NapsFileName, sinkNaps)
	c.State.Waiting.Add(1)
	go sendToCatRPCClient(ctx, c, &tiler.PushFeaturesRequestArgs{
		SourceSchema: tiler.SourceSchema{
			CatID:      c.CatID,
			SourceName: "naps",
			LayerName:  "naps",
		},
		TippeConfig: params.TippeConfigNameNaps,
	}, sendNaps)

	// Block on tripdetect.
	cleaned := c.CleanTracks(ctx, in)
	tripdetected := c.TripDetectTracks(ctx, cleaned)

	c.logger.Info("Trip detector blocking")
	for detected := range tripdetected {
		/*
			fatal error: concurrent map read and map write

			goroutine 4787 [running]:
			github.com/paulmach/orb/geojson.Properties.MustBool(0xc00314e030?, {0xa48197?, 0xf60500?}, {0x0, 0x0, 0x16?})
			        /home/ia/go/pkg/mod/github.com/paulmach/orb@v0.11.1/geojson/properties.go:14 +0x3f
			github.com/rotblauer/catd/api.(*Cat).TripDetectionPipeline(0xc002a041e0, {0xb3a610, 0xc0000b6cd0}, 0xc00218ea80)
			        /home/ia/dev/rotblauer/catd/api/populate.go:209 +0x5c7
			created by github.com/rotblauer/catd/api.(*Cat).Populate in goroutine 22
			        /home/ia/dev/rotblauer/catd/api/populate.go:75 +0x4f7

		*/
		detected := detected // FIXME/FIXED?
		if detected.Properties.MustBool("IsTrip") {
			lapTracks <- detected
		} else {
			napTracks <- detected
		}
	}
}

func sinkToCatJSONGZFile[T any](ctx context.Context, c *Cat, name string, in <-chan *T) {
	c.getOrInitState()

	defer c.State.Waiting.Done()

	defer c.logger.Info("Sunk stream to gz file", "name", name)

	customWriter, err := c.State.NamedGZWriter(name)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		return
	}

	sinkJSONToWriter(ctx, c, customWriter.Writer(), in)
}

func sinkJSONToWriter[T any](ctx context.Context, c *Cat, writer io.WriteCloser, in <-chan *T) {
	defer func() {
		if err := writer.Close(); err != nil {
			c.logger.Error("Failed to close writer", "error", err)
		}
	}()

	enc := json.NewEncoder(writer)

	// Blocking.
	stream.Sink(ctx, func(a *T) {
		if err := enc.Encode(a); err != nil {
			c.logger.Error("Failed to write", "error", err)
		}
	}, in)
}

func sendToCatRPCClient[T any](ctx context.Context, c *Cat, args *tiler.PushFeaturesRequestArgs, in <-chan *T) {
	defer c.State.Waiting.Done()

	features := stream.Collect(ctx, in)
	if len(features) == 0 {
		return
	}

	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	for _, f := range features {
		if err := enc.Encode(f); err != nil {
			c.logger.Error("Failed to encode nap feature", "error", err)
			return
		}
	}
	args.JSONBytes = buf.Bytes()

	err := c.rpcClient.Call("Daemon.PushFeatures", args, nil)
	if err != nil {
		c.logger.Error("Failed to call RPC client",
			"method", "Daemon.PushFeatures", "source", args.SourceName, "features.len", len(features), "error", err)
	}
}
