package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/activity"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"math"
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
			if err != nil {
				close(send)
				errs <- err
				return
			}
			send <- ct
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

func (c *Cat) SubscribeFancyLogs() {
	// Let's celebrate laps.
	onLaps := make(chan cattrack.CatLap)
	c.completedLaps.Subscribe(onLaps)
	//defer close(onLaps)
	go func() {
		for lap := range onLaps {
			a := activity.FromString(lap.Properties.MustString("Activity", "Unknown"))
			c.logger.Info(fmt.Sprintf(
				"%s Completed lap", a.Emoji()),
				"time", lap.Properties.MustString("Time_Start_RFC3339", "XXX"),
				"count", lap.Properties.MustInt("RawPointCount", -1),
				"duration", lap.Duration().Truncate(time.Second),
				"meters", humanize.SIWithDigits(lap.DistanceTraversed(), 2, "m"),
				"activity", a.String(),
			)
		}
	}()
	// And naps...
	onNaps := make(chan cattrack.CatNap)
	c.completedNaps.Subscribe(onNaps)
	//defer close(onNaps)
	go func() {
		for nap := range onNaps {
			area := nap.Properties.MustFloat64("Area", 0)
			edge := math.Sqrt(area)
			seconds := nap.Properties.MustFloat64("Duration", 0)
			duration := time.Duration(seconds * float64(time.Second))
			c.logger.Info(fmt.Sprintf("%s Completed nap", activity.TrackerStateStationary.Emoji()),
				"time", nap.Properties.MustString("Time_Start_RFC3339", "XXX"),
				"count", nap.Properties.MustInt("RawPointCount", -1),
				"duration", duration.Round(time.Second),
				"area", humanize.SIWithDigits(area, 0, "m²"),
				"edge", humanize.SIWithDigits(edge, 0, "m"),
			)
		}
	}()
}

// Populate persists incoming CatTracks for one cat.
// FIXME: All functions should return errors, and this function should return first of any errors.
func (c *Cat) Populate(ctx context.Context, sort bool, in <-chan cattrack.CatTrack) error {

	c.SubscribeFancyLogs()

	// Blocking.
	c.logger.Info("Populate blocking on lock state")
	_, err := c.WithState(false)
	if err != nil {
		c.logger.Error("Failed to get or create cat state", "error", err)
		return err
	}
	c.logger.Info("Populate has the lock on state conn")

	started := time.Now()
	defer func() {
		c.Close()
		c.logger.Info("Populate done",
			"elapsed", time.Since(started).Round(time.Millisecond))
	}()

	dedupeCache := cattrack.NewDedupeLRUFunc()
	validated := stream.Filter(ctx, func(ct cattrack.CatTrack) bool {
		if ct.IsEmpty() {
			c.logger.Error("Invalid track: track is empty")
			return false
		}
		if err := ct.Validate(); err != nil {
			c.logger.Error("Invalid track", "error", err)
			return false
		}
		if id := ct.CatID(); c.CatID != id {
			c.logger.Error("Invalid track, mismatched cat", "want", fmt.Sprintf("%q", c.CatID), "got", fmt.Sprintf("%q", id))
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
	pipedLast := sanitized
	if sort {
		// Sorting is blocking.
		pipedLast = stream.BatchSort(ctx, params.DefaultBatchSize, cattrack.SortFunc, sanitized)
		//pipedLast = stream.SortRing1(ctx, cattrack.SortFunc, params.DefaultBatchSize, sanitized)
	}

	// Fork stream into snaps/no-snaps.
	// Snaps are a different animal than normal cat tracks.
	yesSnaps, noSnaps := stream.TeeFilter(ctx, func(ct cattrack.CatTrack) bool {
		return ct.IsSnap()
	}, pipedLast)

	// Snap storage mutates the original snap tracks.
	snapped, snapErrs := c.StoreSnaps(ctx, yesSnaps)
	sinkSnaps, sendSnaps := stream.Tee(ctx, snapped)

	sinkSnapErrs := make(chan error, 1)
	go func() {
		defer close(sinkSnapErrs)
		gzftwSnaps, err := c.State.Flat.NamedGZWriter(params.SnapsGZFileName, nil)
		if err != nil {
			c.logger.Error("Failed to create custom writer", "error", err)
			sinkSnapErrs <- err
			return
		}
		if err := sinkStreamToJSONGZWriter(ctx, c, gzftwSnaps, sinkSnaps); err != nil {
			c.logger.Error("Failed to write snaps", "error", err)
			sinkSnapErrs <- err
		}
	}()

	sendSnapErrs := make(chan error, 1)
	go func() {
		defer close(sendSnapErrs)
		err := sendToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
			SourceSchema: tiled.SourceSchema{
				CatID:      c.CatID,
				SourceName: "snaps",
				LayerName:  "snaps",
			},
			TippeConfigName: params.TippeConfigNameSnaps,
			Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
			SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
		}, sendSnaps)
		if err != nil {
			c.logger.Error("Failed to send snaps", "error", err)
			sendSnapErrs <- err
		}
	}()

	// All non-snaps flow to these channels.
	//storeCh, pipelineChan := stream.Tee(ctx, noSnaps)
	storeCh := make(chan cattrack.CatTrack, params.DefaultBatchSize)
	pipelineChan := make(chan cattrack.CatTrack, params.DefaultBatchSize)
	stream.TeeMany(ctx, noSnaps, storeCh, pipelineChan)

	storeErrs := make(chan error, 1)
	go func() {
		defer close(storeErrs)
		err := <-c.StoreTracks(ctx, storeCh)
		if err != nil {
			c.logger.Error("Failed to store tracks", "error", err)
			storeErrs <- err
		}
	}()

	pipeLineErrs := make(chan error, 1)
	go func() {
		defer close(pipeLineErrs)
		if err := c.ProducerPipelines(ctx, pipelineChan); err != nil {
			c.logger.Error("Failed to run producer pipelines", "error", err)
			pipeLineErrs <- err
		}
	}()

	//// P.S. Don't send all tracks to tiled unless development.
	//sendToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
	//	SourceSchema: tiled.SourceSchema{
	//		CatID:      c.CatID,
	//		SourceName: "tracks",
	//		LayerName:  "tracks",
	//	},
	//	TippeConfigName: params.TippeConfigNameTracks,
	//	Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
	//	SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
	//}, sendTiledCh)

	// Block on any store errors, returning first.
	c.logger.Info("Blocking on store cat tracks+snaps gz")
	handledErrorsN := 0
	for {
		select {
		case err, open := <-storeErrs:
			if err != nil {
				return fmt.Errorf("storeErrs: %w", err)
			}
			if !open {
				handledErrorsN++
				storeErrs = nil
			}
		case err, open := <-snapErrs:
			if err != nil {
				return fmt.Errorf("snapErrs: %w", err)
			}
			if !open {
				handledErrorsN++
				snapErrs = nil
			}
		case err, open := <-sinkSnapErrs:
			if err != nil {
				return fmt.Errorf("sinkSnapErrs: %w", err)
			}
			if !open {
				handledErrorsN++
				sinkSnapErrs = nil
			}
		case err, open := <-sendSnapErrs:
			if err != nil {
				return fmt.Errorf("sendSnapErrs: %w", err)
			}
			if !open {
				handledErrorsN++
				sendSnapErrs = nil
			}
		case err, open := <-pipeLineErrs:
			if err != nil {
				return fmt.Errorf("pipeLineErrs: %w", err)
			}
			if !open {
				handledErrorsN++
				pipeLineErrs = nil
			}
		}
		if handledErrorsN == 5 {
			break
		}
	}
	return nil
}

func sinkStreamToJSONGZWriter[T any](ctx context.Context, c *Cat, wr *flat.GZFileWriter, in <-chan T) error {

	defer c.logger.Info("Sunk stream to gz file", "path", wr.Path())
	defer func() {
		if err := wr.Close(); err != nil {
			c.logger.Error("Failed to close writer", "error", err)
		}
	}()

	w := wr.Writer()
	enc := json.NewEncoder(w)

	// Blocking.
	for a := range in {
		if err := enc.Encode(a); err != nil {
			c.logger.Error("Failed to write", "error", err)
			return err
		}
	}
	return nil
}

func (c *Cat) IsRPCEnabled() bool {
	return c.rpcClient != nil
}

// sendToCatRPCClient sends a batch of features to the Cat RPC client.
// It is a non-blocking function, and registers itself with the Cat Waiting state.
func sendToCatRPCClient[T any](ctx context.Context, c *Cat, args *tiled.PushFeaturesRequestArgs, in <-chan T) error {
	if !c.IsRPCEnabled() {
		c.logger.Warn("Cat RPC client not configured (noop)", "method", "PushFeatures")
		go stream.Sink(ctx, nil, in)
		return nil
	}

	all := stream.Collect(ctx, in)
	if len(all) == 0 {
		c.logger.Warn("No features to send", "source", args.SourceName)
		return nil
	}

	buf := bytes.NewBuffer([]byte{})
	enc := json.NewEncoder(buf)

	for _, el := range all {
		cp := el
		if err := enc.Encode(cp); err != nil {
			c.logger.Error("Failed to encode feature", "error", err)
			return err
		}
	}

	args.JSONBytes = buf.Bytes()

	err := c.rpcClient.Call("TileDaemon.PushFeatures", args, nil)
	if err != nil {
		c.logger.Error("Failed to call RPC client",
			"method", "PushFeatures", "source", args.SourceName, "all.len", len(all), "error", err)
	}
	return err
}

// sendGZippedToCatRPCClient sends a batch of gzipped features to the Cat RPC client.
// It is a non-blocking function, and registers itself with the Cat Waiting state.
func sendGZippedToCatRPCClient[T any](ctx context.Context, c *Cat, args *tiled.PushFeaturesRequestArgs, in <-chan T) error {
	if !c.IsRPCEnabled() {
		c.logger.Warn("Cat RPC client not configured (noop)", "method", "PushFeatures")
		go stream.Sink(ctx, nil, in)
		return nil
	}

	all := stream.Collect(ctx, in)
	if len(all) == 0 {
		c.logger.Warn("No features to send", "source", args.SourceName)
		return nil
	}

	buf := bytes.NewBuffer([]byte{})
	gz, err := gzip.NewWriterLevel(buf, params.DefaultGZipCompressionLevel)
	if err != nil {
		c.logger.Error("Failed to create gzip writer", "error", err)
		return err
	}
	enc := json.NewEncoder(gz)

	for _, el := range all {
		if err := enc.Encode(el); err != nil {
			c.logger.Error("Failed to encode feature", "error", err)
			return err
		}
	}

	args.GzippedJSONBytes = buf.Bytes()

	err = c.rpcClient.Call("TileDaemon.PushFeatures", args, nil)
	if err != nil {
		c.logger.Error("Failed to call RPC client",
			"method", "PushFeatures", "source", args.SourceName, "all.len", len(all), "error", err)
	}
	return err
}
