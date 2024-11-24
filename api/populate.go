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
	"os"
	"time"
)

// Populate persists incoming CatTracks for one cat.
func (c *Cat) Populate(ctx context.Context, sort bool, in <-chan cattrack.CatTrack) (lastErr error) {

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

	validated := stream.Filter(ctx, func(ct cattrack.CatTrack) bool {
		if ct.IsEmpty() {
			c.logger.Warn("Invalid track, empty")
			return false
		}
		if err := ct.Validate(); err != nil {
			c.logger.Warn("Invalid track", "error", err)
			return false
		}
		checkCatID := ct.CatID()
		if c.CatID != checkCatID {
			c.logger.Warn("Invalid track, mismatched cat", "want", fmt.Sprintf("%q", c.CatID), "got", fmt.Sprintf("%q", checkCatID))
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

	// Tee for storage globally (master) and per cat.
	//master, myCat := stream.Tee(ctx, deduped)

	// Sink ALL tracks (from ALL CATS) to master.geojson.gz.
	// Thread safe because gz file locks.
	// Cat pushes will be stored in cat push/populate-batches.
	gzftwMaster, err := flat.NewFlatWithRoot(params.DatadirRoot).
		NamedGZWriter("master.geojson.gz", nil)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		return err
	}
	sunkMaster := pipeThroughFlatJSONGZFile(ctx, c, gzftwMaster, deduped)

	// StoreTracks for each cat.
	// This gets its own special cat-method for now because
	// cat/track storage is very important and there might be
	// events or something else cat-related to do.
	// This method will block on her error handling (storeErrs).
	stored, storeErrs := c.StoreTracks(ctx, sunkMaster)

	passSnaps, setLast := stream.Tee(ctx, stored)

	truncate := flat.DefaultGZFileWriterConfig()
	truncate.Flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	gzftwLast, err := c.State.Flat.NamedGZWriter("last_tracks.geojson.gz", truncate)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		return err
	}
	sunkLast := pipeThroughFlatJSONGZFile(ctx, c, gzftwLast, setLast)
	sendToCatRPCClient(ctx, c, &tiler.PushFeaturesRequestArgs{
		SourceSchema: tiler.SourceSchema{
			CatID:      c.CatID,
			SourceName: "tracks",
			LayerName:  "tracks",
		},
		TippeConfig: params.TippeConfigNameTracks,
	}, sunkLast)

	yesSnaps := make(chan cattrack.CatTrack)
	noSnaps := make(chan cattrack.CatTrack)

	go func() {
		defer close(yesSnaps)
		defer close(noSnaps)
		for element := range passSnaps {
			track := element
			if track.IsSnap() {
				yesSnaps <- track
			} else {
				noSnaps <- track
			}
		}
	}()

	//// Fork stream into snaps/no-snaps.
	//ifSnaps, ifNoSnaps := stream.Tee(ctx, passSnaps)
	//yesSnaps := stream.Filter(ctx, func(c cattrack.CatTrack) bool {
	//	return c.IsSnap()
	//}, ifSnaps)
	//noSnaps := stream.Filter(ctx, func(c cattrack.CatTrack) bool {
	//	return !c.IsSnap()
	//}, ifNoSnaps)
	//
	snapped, snapErrs := c.StoreSnaps(ctx, yesSnaps)
	storeErrs = stream.Merge(ctx, storeErrs, snapErrs)
	gzftwSnaps, err := c.State.Flat.NamedGZWriter("snaps.geojson.gz", nil)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		return err
	}
	sunkSnaps := pipeThroughFlatJSONGZFile(ctx, c, gzftwSnaps, snapped)
	sendToCatRPCClient(ctx, c, &tiler.PushFeaturesRequestArgs{
		SourceSchema: tiler.SourceSchema{
			CatID:      c.CatID,
			SourceName: "snaps",
			LayerName:  "snaps",
		},
		TippeConfig: params.TippeConfigNameSnaps,
	}, sunkSnaps)

	// S2 indexing pipeline. Stateful/cat.
	// Trip detection pipeline. Laps, naps. Stateful/cat.
	indexingCh, tripdetectCh := stream.Tee(ctx, noSnaps)
	go c.S2IndexTracks(ctx, indexingCh)
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

func (c *Cat) TripDetectionPipeline(ctx context.Context, in <-chan cattrack.CatTrack) {
	lapTracks := make(chan cattrack.CatTrack)
	napTracks := make(chan cattrack.CatTrack)
	defer close(lapTracks)
	defer close(napTracks)

	cleaned := c.CleanTracks(ctx, in)
	tripdetected := c.TripDetectTracks(ctx, cleaned)

	// Synthesize new/derivative/aggregate features:
	// ... LineStrings for laps, Points for naps.

	// TrackLaps will send completed laps. Incomplete laps are persisted in KV
	// and restored on cat restart.
	completedLaps := c.TrackLaps(ctx, lapTracks)
	filterLaps := stream.Filter(ctx, func(ct cattrack.CatLap) bool {
		duration := ct.Properties["Duration"].(float64)
		return duration > 120
	}, completedLaps)

	// Simplify the lap geometry.
	simplifier := simplify.DouglasPeucker(params.DefaultSimplifierConfig.DouglasPeuckerThreshold)
	simplified := stream.Transform(ctx, func(ct cattrack.CatLap) cattrack.CatLap {
		cp := new(cattrack.CatLap)
		*cp = ct
		geom := simplifier.Simplify(ct.Geometry)
		cp.Geometry = geom
		return *cp
	}, filterLaps)

	// End of the line for all cat laps...

	wr, err := c.State.Flat.NamedGZWriter("laps.geojson.gz", nil)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		return
	}
	sunkLaps := pipeThroughFlatJSONGZFile(ctx, c, wr, simplified)
	sendToCatRPCClient(ctx, c, &tiler.PushFeaturesRequestArgs{
		SourceSchema: tiler.SourceSchema{
			CatID:      c.CatID,
			SourceName: "laps",
			LayerName:  "laps",
		},
		TippeConfig: params.TippeConfigNameLaps,
	}, sunkLaps)

	// TrackNaps will send completed naps. Incomplete naps are persisted in KV
	// and restored on cat restart.
	completedNaps := c.TrackNaps(ctx, napTracks)
	filteredNaps := stream.Filter(ctx, func(ct cattrack.CatNap) bool {
		return ct.Properties.MustFloat64("Duration", 0) > 120
	}, completedNaps)

	// End of the line for all cat naps...
	wr, err = c.State.Flat.NamedGZWriter("naps.geojson.gz", nil)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		return
	}
	sunkNaps := pipeThroughFlatJSONGZFile(ctx, c, wr, filteredNaps)
	sendToCatRPCClient(ctx, c, &tiler.PushFeaturesRequestArgs{
		SourceSchema: tiler.SourceSchema{
			CatID:      c.CatID,
			SourceName: "naps",
			LayerName:  "naps",
		},
		TippeConfig: params.TippeConfigNameNaps,
	}, sunkNaps)

	tripFork, tripDetectedPass := stream.Tee(ctx, tripdetected)
	wr, err = c.State.Flat.NamedGZWriter("tripdetected.geojson.gz", nil)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		return
	}
	tdSunk := pipeThroughFlatJSONGZFile(ctx, c, wr, tripDetectedPass)
	sendToCatRPCClient(ctx, c, &tiler.PushFeaturesRequestArgs{
		SourceSchema: tiler.SourceSchema{
			CatID:      c.CatID,
			SourceName: "tripdetected",
			LayerName:  "tripdetected",
		},
		TippeConfig: params.TippeConfigNameTripDetected,
	}, tdSunk)

	// Block on tripdetect.
	c.logger.Info("Trip detector blocking")
	stream.Sink(ctx, func(ct cattrack.CatTrack) {
		if ct.Properties.MustBool("IsTrip") {
			lapTracks <- ct
		} else {
			napTracks <- ct
		}
	}, tripFork)
}

func pipeThroughFlatJSONGZFile[T any](ctx context.Context, c *Cat, wr *flat.GZFileWriter, in <-chan T) (out chan T) {
	c.State.Waiting.Add(1)
	out = make(chan T)
	go func() {
		defer c.State.Waiting.Done()
		defer close(out)
		defer c.logger.Info("Sunk stream to gz file", "path", wr.Path())
		defer func() {
			if err := wr.Close(); err != nil {
				c.logger.Error("Failed to close writer", "error", err)
			}
		}()
		w := wr.Writer()
		enc := json.NewEncoder(w)
		for element := range in {
			el := element
			if err := enc.Encode(el); err != nil {
				c.logger.Error("Failed to write", "error", err)
			}
			select {
			case out <- el:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func sinkToFlatJSONGZFile[T any](ctx context.Context, c *Cat, wr *flat.GZFileWriter, in <-chan T) {
	c.State.Waiting.Add(1)
	go func() {
		defer c.State.Waiting.Done()

		defer c.logger.Info("Sunk stream to gz file", "path", wr.Path())
		defer func() {
			if err := wr.Close(); err != nil {
				c.logger.Error("Failed to close writer", "error", err)
			}
		}()

		// TODO
		w := wr.Writer()
		enc := json.NewEncoder(w)

		// Blocking.
		stream.Sink(ctx, func(a T) {
			if err := enc.Encode(a); err != nil {
				c.logger.Error("Failed to write", "error", err)
			}
		}, in)

		//all := stream.Collect(ctx, in)
		//for _, el := range all {
		//	if err := enc.Encode(el); err != nil {
		//		c.logger.Error("Failed to write", "error", err)
		//		return
		//	}
		//}

		//batches := stream.Batch(ctx, nil, func(b []T) bool {
		//	//return len(b) == 100 // panics every time
		//	return len(b) == 1 // ok sometimes
		//}, in)
		//for batch := range batches {
		//	batch := batch
		//	for _, el := range batch {
		//		el := el
		//		if err := enc.Encode(el); err != nil {
		//			c.logger.Error("Failed to write", "error", err)
		//			return
		//		}
		//	}
		//}
	}()
}

func sendToCatRPCClient[T any](ctx context.Context, c *Cat, args *tiler.PushFeaturesRequestArgs, in <-chan T) {
	c.State.Waiting.Add(1)
	go func() {
		defer c.State.Waiting.Done()

		all := stream.Collect(ctx, in)
		if len(all) == 0 {
			return
		}

		buf := new(bytes.Buffer)
		enc := json.NewEncoder(buf)
		for _, el := range all {
			cp := el
			if err := enc.Encode(cp); err != nil {
				c.logger.Error("Failed to encode feature", "error", err)
				return
			}
		}
		args.JSONBytes = buf.Bytes()

		err := c.rpcClient.Call("Daemon.PushFeatures", args, nil)
		if err != nil {
			c.logger.Error("Failed to call RPC client",
				"method", "Daemon.PushFeatures", "source", args.SourceName, "all.len", len(all), "error", err)
		}
	}()
}

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
