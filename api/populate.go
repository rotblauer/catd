package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
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

func (c *Cat) Close() {
	c.State.Close()
	if c.rpcClient != nil {
		c.rpcClient.Close()
	}
}

// Populate persists incoming CatTracks for one cat.
// FIXME: All functions should return errors, and this function should return first of any errors.
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

	dedupeCache := cache.NewDedupePassLRUFunc()
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
		// Dedupe with hash cache.
		if !dedupeCache(ct) {
			c.logger.Warn("Deduped track", "track", ct.StringPretty())
			return false
		}
		return true
	}, in)

	sanitized := stream.Transform(ctx, cattrack.Sanitize, validated)

	// Sorting is blocking.
	pipedLast := sanitized
	if sort {
		// Catch is the new batch.
		sorted := stream.BatchSort(ctx, params.DefaultBatchSize,
			cattrack.SortFunc, sanitized)
		pipedLast = sorted
	}

	// Fork stream into snaps/no-snaps.
	yesSnaps, noSnaps := stream.Fork(ctx, func(ct cattrack.CatTrack) bool {
		return ct.IsSnap()
	}, pipedLast)

	storeCh := make(chan cattrack.CatTrack)
	sendTiledCh := make(chan cattrack.CatTrack)
	indexingCh := make(chan cattrack.CatTrack)
	tripdetectCh := make(chan cattrack.CatTrack)
	stream.Split(ctx, noSnaps,
		storeCh,
		sendTiledCh,
		indexingCh,
		tripdetectCh,
	)

	// StoreTracks for each cat in catid/track.geojson.gz.
	// This gets its own special cat-method for now because
	// Populate blocks on its error handling.
	// These errors are particularly important.
	storeErrs := c.StoreTracks(ctx, storeCh)
	snapped, snapErrs := c.StoreSnaps(ctx, yesSnaps)
	storeErrs = stream.Merge(ctx, storeErrs, snapErrs)

	sendBatchToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
		SourceSchema: tiled.SourceSchema{
			CatID:      c.CatID,
			SourceName: "tracks",
			LayerName:  "tracks",
		},
		TippeConfig: params.TippeConfigNameTracks,
	}, sendTiledCh)

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
	go c.S2IndexTracks(ctx, indexingCh)
	//go c.TripDetectionPipeline(ctx, tripdetectCh)
	go c.ActDetectionPipeline(ctx, tripdetectCh)

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
	}()
}

func sendBatchToCatRPCClient[T any](ctx context.Context, c *Cat, args *tiled.PushFeaturesRequestArgs, in <-chan T) {
	if c.rpcClient == nil {
		c.logger.Debug("Cat RPC client not configured (noop)", "method", "PushFeatures")
		//go stream.Sink(ctx, nil, in)
		return
	}
	c.State.Waiting.Add(1)
	go func() {
		defer c.State.Waiting.Done()

		all := stream.Collect(ctx, in)
		if len(all) == 0 {
			return
		}

		buf := bytes.NewBuffer([]byte{})
		defer buf.Reset()
		enc := json.NewEncoder(buf)

		for _, el := range all {
			cp := el
			if err := enc.Encode(cp); err != nil {
				c.logger.Error("Failed to encode feature", "error", err)
				return
			}
		}

		args.JSONBytes = buf.Bytes()
		defer func() {
			args.JSONBytes = nil
		}()

		err := c.rpcClient.Call("TileDaemon.PushFeatures", args, nil)
		if err != nil {
			c.logger.Error("Failed to call RPC client",
				"method", "PushFeatures", "source", args.SourceName, "all.len", len(all), "error", err)
		}
		return
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
