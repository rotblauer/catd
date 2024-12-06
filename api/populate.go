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
	"net/rpc"
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
	if err := c.State.Close(); err != nil {
		c.logger.Error("Failed to close cat state", "error", err)
	}
	if c.rpcClient != nil {
		if err := c.rpcClient.Close(); err != nil {
			c.logger.Error("Failed to close RPC client", "error", err)
		}
	}
}

func (c *Cat) dialRPCServer() error {
	client, err := rpc.DialHTTP(c.tiledConf.RPCNetwork, c.tiledConf.RPCAddress)
	if err != nil {
		return err
	}
	c.rpcClient = client
	return nil
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

	if c.tiledConf != nil {
		if err := c.dialRPCServer(); err != nil {
			c.logger.Error("Failed to dial RPC client", "error", err)
			return err
		}
		c.logger.Info("Dialed RPC client", "network", c.tiledConf.RPCNetwork, "address", c.tiledConf.RPCAddress)
	} else {
		c.logger.Debug("No tiled config, not dialing RPC client")
	}

	started := time.Now()
	defer func() {
		c.Close()
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
	// Snaps are a different animal than normal cat tracks.
	yesSnaps, noSnaps := stream.TeeFilter(ctx, func(ct cattrack.CatTrack) bool {
		return ct.IsSnap()
	}, pipedLast)

	// All non-snap tracks flow to these channels.
	storeCh := make(chan cattrack.CatTrack)
	//sendTiledCh := make(chan cattrack.CatTrack)
	areaPipeCh := make(chan cattrack.CatTrack)
	vectorPipeCh := make(chan cattrack.CatTrack)
	stream.TeeMany(ctx, noSnaps,
		storeCh,
		//sendTiledCh,
		areaPipeCh,
		vectorPipeCh,
	)

	storeErrs := c.StoreTracks(ctx, storeCh)

	// Snap storage mutates the original snap tracks.
	snapped, snapErrs := c.StoreSnaps(ctx, yesSnaps)
	storeErrs = stream.Merge(ctx, storeErrs, snapErrs)

	//// P.S. Don't send all tracks to tiled unless development.
	//sendBatchToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
	//	SourceSchema: tiled.SourceSchema{
	//		CatID:      c.CatID,
	//		SourceName: "tracks",
	//		LayerName:  "tracks",
	//	},
	//	TippeConfigName: params.TippeConfigNameTracks,
	//	Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
	//	SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
	//}, sendTiledCh)

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
		TippeConfigName: params.TippeConfigNameSnaps,
		Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
		SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
	}, sendSnaps)

	// S2 indexing pipeline. Stateful/cat.
	go c.S2IndexTracks(ctx, areaPipeCh)
	// Laps, naps.
	go c.CatActPipeline(ctx, vectorPipeCh)
	//go c.TripDetectionPipeline(ctx, vectorPipeCh)

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

// sendBatchToCatRPCClient sends a batch of features to the Cat RPC client.
// It is a non-blocking function, and registers itself with the Cat Waiting state.
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
		return
	}()
}
