package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/paulmach/orb/simplify"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"os"
	"time"
)

func (c *Cat) PopulateReader(ctx context.Context, sort bool, in io.Reader) (err error) {

	send := make(chan cattrack.CatTrack)
	errs := make(chan error, 2)

	go func() {
		dec := json.NewDecoder(in)
		for {
			ct := cattrack.CatTrack{}
			err := dec.Decode(&ct)
			if err == io.EOF {
				close(send)
				errs <- err
				return
			}
			if err == nil {
				send <- ct
				continue
			}
			// else try decoding/umarshaling as trackpoint...
		}
	}()

	go func() {
		errs <- c.Populate(ctx, sort, send)
	}()

	for i := 0; i < 2; i++ {
		select {
		case err = <-errs:
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

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
			c.logger.Debug("Closed cat state")
		}
		c.logger.Info("Populate done",
			"elapsed", time.Since(started).Round(time.Millisecond))
	}()

	validated := stream.Filter(ctx, func(ct cattrack.CatTrack) bool {
		if ct.IsEmpty() {
			c.logger.Error("Invalid track: track is empty")
			return false
		}
		if err := ct.Validate(); err != nil {
			c.logger.Error("Invalid track", "error", err)
			return false
		}
		checkCatID := ct.CatID()
		if c.CatID != checkCatID {
			c.logger.Error("Invalid track, mismatched cat", "want", fmt.Sprintf("%q", c.CatID), "got", fmt.Sprintf("%q", checkCatID))
			return false
		}
		return true
	}, in)

	sanitized := stream.Transform(ctx, cattrack.Sanitize, validated)

	// Sorting is blocking. (!)
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
	master, myCat := stream.Tee(ctx, deduped)

	// Sink ALL tracks (from ALL CATS) to master.geojson.gz.
	// Cat/thread safe because gz file locks.
	// Cat pushes will be stored in cat push/populate-batches.
	gzftwMaster, err := flat.NewFlatWithRoot(params.DatadirRoot).
		NamedGZWriter("master.geojson.gz", nil)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		return err
	}
	sinkStreamToJSONGZWriter(ctx, c, gzftwMaster, master)

	// StoreTracks for each cat in catid/track.geojson.gz.
	// This gets its own special cat-method for now because
	// Populate blocks on its error handling.
	// These errors are particularly important.
	stored, storeErrs := c.StoreTracks(ctx, myCat)

	passSnaps, passLast := stream.Tee(ctx, stored)

	// Sink this batch of tracks to cat's last_tracks store.
	// Opens store file with flag OS_TRUNC.
	// This batch also headed toward the tiler RPC. (TODO: Tiler streaming.)
	sinkLastTracks, sendTracks := stream.Tee(ctx, passLast)

	truncate := flat.DefaultGZFileWriterConfig()
	truncate.Flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	gzftwLast, err := c.State.Flat.NamedGZWriter("last_tracks.geojson.gz", truncate)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		return err
	}
	sinkStreamToJSONGZWriter(ctx, c, gzftwLast, sinkLastTracks)
	sendBatchToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
		SourceSchema: tiled.SourceSchema{
			CatID:      c.CatID,
			SourceName: "tracks",
			LayerName:  "tracks",
		},
		TippeConfig: params.TippeConfigNameTracks,
	}, sendTracks)

	//// Fork stream into snaps/no-snaps.
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

	//ifSnaps, ifNoSnaps := stream.Tee(ctx, passSnaps)
	//yesSnaps := stream.Filter(ctx, func(c cattrack.CatTrack) bool {
	//	return c.IsSnap()
	//}, ifSnaps)
	//noSnaps := stream.Filter(ctx, func(c cattrack.CatTrack) bool {
	//	return !c.IsSnap()
	//}, ifNoSnaps)

	//go stream.Sink(ctx, func(ct cattrack.CatTrack) {
	//	if ct.IsSnap() {
	//		yesSnaps <- ct
	//	} else {
	//		noSnaps <- ct
	//	}
	//}, passSnaps)

	snapped, snapErrs := c.StoreSnaps(ctx, yesSnaps)
	storeErrs = stream.Merge(ctx, storeErrs, snapErrs) // rly? TODO... more of?

	sinkSnaps, sendSnaps := stream.Tee(ctx, snapped)
	gzftwSnaps, err := c.State.Flat.NamedGZWriter("snaps.geojson.gz", nil)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		return err
	}
	sinkStreamToJSONGZWriter(ctx, c, gzftwSnaps, sinkSnaps)
	sendBatchToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
		SourceSchema: tiled.SourceSchema{
			CatID:      c.CatID,
			SourceName: "snaps",
			LayerName:  "snaps",
		},
		TippeConfig: params.TippeConfigNameSnaps,
	}, sendSnaps)

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
	// TODO Send incomplete lap on close. This will be nice to have.
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
	sinkLaps, sendLaps := stream.Tee(ctx, simplified)

	wr, err := c.State.Flat.NamedGZWriter("laps.geojson.gz", nil)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		return
	}
	sinkStreamToJSONGZWriter(ctx, c, wr, sinkLaps)
	sendBatchToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
		SourceSchema: tiled.SourceSchema{
			CatID:      c.CatID,
			SourceName: "laps",
			LayerName:  "laps",
		},
		TippeConfig: params.TippeConfigNameLaps,
	}, sendLaps)

	// TrackNaps will send completed naps. Incomplete naps are persisted in KV
	// and restored on cat restart.
	completedNaps := c.TrackNaps(ctx, napTracks)
	filteredNaps := stream.Filter(ctx, func(ct cattrack.CatNap) bool {
		return ct.Properties.MustFloat64("Duration", 0) > 120
	}, completedNaps)

	// End of the line for all cat naps...
	sinkNaps, sendNaps := stream.Tee(ctx, filteredNaps)

	wr, err = c.State.Flat.NamedGZWriter("naps.geojson.gz", nil)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		return
	}
	sinkStreamToJSONGZWriter(ctx, c, wr, sinkNaps)
	sendBatchToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
		SourceSchema: tiled.SourceSchema{
			CatID:      c.CatID,
			SourceName: "naps",
			LayerName:  "naps",
		},
		TippeConfig: params.TippeConfigNameNaps,
	}, sendNaps)

	tripDetectedFork, tripDetectedPass := stream.Tee(ctx, tripdetected)
	tripDetectedSink, tripDetectedSend := stream.Tee(ctx, tripDetectedPass)

	wr, err = c.State.Flat.NamedGZWriter("tripdetected.geojson.gz", nil)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		return
	}
	sinkStreamToJSONGZWriter(ctx, c, wr, tripDetectedSink)
	sendBatchToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
		SourceSchema: tiled.SourceSchema{
			CatID:      c.CatID,
			SourceName: "tripdetected",
			LayerName:  "tripdetected",
		},
		TippeConfig: params.TippeConfigNameTripDetected,
	}, tripDetectedSend)

	// Block on tripdetect.
	c.logger.Info("Trip detector blocking")
	stream.Sink(ctx, func(ct cattrack.CatTrack) {
		if ct.Properties.MustBool("IsTrip") {
			lapTracks <- ct
		} else {
			napTracks <- ct
		}
	}, tripDetectedFork)
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

func sinkStreamToJSONGZWriter[T any](ctx context.Context, c *Cat, wr *flat.GZFileWriter, in <-chan T) {
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
		//	return len(b) == 10_000 // ok sometimes
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

func sendBatchToCatRPCClient[T any](ctx context.Context, c *Cat, args *tiled.PushFeaturesRequestArgs, in <-chan T) {
	if c.rpcClient == nil {
		c.logger.Debug("Cat RPC client not configured (noop)", "method", "PushFeatures")
		go stream.Sink(ctx, nil, in)
		return
	}
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

		err := c.rpcClient.Call("TileDaemon.PushFeatures", args, nil)
		if err != nil {
			c.logger.Error("Failed to call RPC client",
				"method", "PushFeatures", "source", args.SourceName, "all.len", len(all), "error", err)
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
