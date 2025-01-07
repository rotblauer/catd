package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/metrics/influxdb"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/activity"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"log/slog"
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
				"meters", humanize.SIWithDigits(lap.Properties.MustFloat64("Distance_Traversed"), 2, "m"),
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

func (c *Cat) ExportInfluxDB(tracks []cattrack.CatTrack) error {
	if params.INFLUXDB_URL == "" {
		return errors.New("InfluxDB not configured")
	}
	return influxdb.ExportCatTracks(tracks)
}

// Populate persists incoming CatTracks for one cat.
func (c *Cat) Populate(ctx context.Context, sort bool, in <-chan cattrack.CatTrack) error {
	var cancelCtx context.CancelFunc
	ctx, cancelCtx = context.WithCancel(ctx)

	c.SubscribeFancyLogs()

	c.logger.Info("Populate blocking on lock state")
	err := c.LockOrLoadState(false)
	if err != nil {
		c.logger.Error("Failed to get or create cat state", "error", err)
		return err
	}
	c.logger.Info("Populate has the lock on state conn")

	started := time.Now()
	defer func() {
		l := c.logger.Info
		if err := c.Close(); err != nil {
			c.logger.Error("Failed to close cat state", "error", err)
			l = c.logger.Error
		}
		l("Populate done",
			"elapsed", time.Since(started).Round(time.Millisecond))
	}()

	// Validate, dedupe, sanitize.
	valid, invalid := c.Validate(ctx, in)
	c.waitHandleInvalid(ctx, invalid, c.State.Waiting)
	deduped := c.dedupe(ctx, params.DedupeCacheSize, valid)
	sanitized := stream.Transform(ctx, cattrack.Sanitize, deduped)

	// The catdReceivedAt stamp will indicate the time Populate was called
	// for this batch of tracks (not exactly the time this function receives it).
	receivedAt := time.Now().Unix()
	stamped := stream.Transform(ctx, func(ct cattrack.CatTrack) cattrack.CatTrack {
		ct.SetPropertySafe("catdReceivedAt", receivedAt)
		return ct
	}, sanitized)

	normalized := stamped
	if sort {
		normalized = stream.BatchSort(ctx, params.DefaultSortSize, cattrack.SlicesSortFunc, stamped)
		//normalized = stream.BatchSortaBetter(ctx, params.DefaultSortSize, cattrack.SlicesSortFunc, stamped)
		//normalized = stream.RingSort(ctx, params.DefaultSortSize, cattrack.SlicesSortFunc, stamped)
	}

	// Tee for processing and InfluxDB metrics.
	// TODO: Should the InfluxDB metrics be called on successful store instead? Also?
	//normalA, normalB := stream.Tee(ctx, normalized)
	//stream.Blackhole(normalB)

	// Fork normalized stream into snaps/no-snaps.
	// Snaps are a different animal than normal cat tracks.
	// Base 64 images should be stripped, converted into better formats,
	// stored, and trigger some events, like tiling updates.
	// We don't want to send snaps to the places normal tracks go.
	yesSnaps, noSnaps := stream.TeeFilter(ctx, func(ct cattrack.CatTrack) bool {
		return ct.IsSnap()
	}, normalized)

	//// Unbacktrack drops tracks that are older than the last known track,
	//// or otherwise within the window of seen tracks; critically: per cat/uuid.
	//// WARN: This WILL DROP MEANINGFUL TRACKS, and is ONLY for testing/development.
	//// Bad network connections, or other issues, can cause clients to upload
	//// tracks out of order, and this will drop them. See 2023-06 for some examples, eg. biking Coeur d'Alene.
	//// Sketchy tracks/weird uploads are often associated with awesome tracks,
	//// so better to not use this in production.
	//unbacktracked, onCloseBack := c.Unbacktrack(ctx, noSnaps)
	//defer func() {
	//	if err := onCloseBack(); err != nil {
	//		c.logger.Error("Failed to close cat window unbacktracker", "error", err)
	//	}
	//}()

	// Snap storage mutates the original snap tracks.
	snapped, snapErrs := c.StoreSnaps(ctx, yesSnaps)
	sinkSnaps, sendSnaps := stream.Tee(ctx, snapped)

	sinkSnapErrs := make(chan error, 1)
	go func() {
		defer close(sinkSnapErrs)
		gzftwSnaps, err := c.State.Flat.NewGZFileWriter(params.SnapsGZFileName, nil)
		if err != nil {
			c.logger.Error("Failed to create custom writer", "error", err)
			sinkSnapErrs <- err
			return
		}
		defer gzftwSnaps.Close()
		if n, err := sinkStreamToJSONWriter(ctx, gzftwSnaps, sinkSnaps); err != nil {
			c.logger.Error("Failed to write snaps", "error", err, "written", n)
			sinkSnapErrs <- err
		} else {
			c.logger.Info("Wrote snaps", "count", n)
		}
	}()

	sendSnapErrs := make(chan error, 1)
	go func() {
		defer close(sendSnapErrs)
		err := sendToCatTileD(ctx, c, &tiled.PushFeaturesRequestArgs{
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
	storeCh := make(chan cattrack.CatTrack, params.DefaultChannelCap)
	pipelineChan := make(chan cattrack.CatTrack, params.DefaultChannelCap)
	stream.TeeMany(ctx, noSnaps, storeCh, pipelineChan)

	storeErrs := make(chan error, 1)
	go func() {
		defer close(storeErrs)
		err := <-c.StoreTracksYYYYMM(ctx, storeCh)
		//err := <-c.StoreTracks(ctx, storeCh)
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

	// Block on any store errors, returning first.
	c.logger.Info("Blocking on store cat tracks+snaps gz")
	handledErrorsN := 0
	for {
		var err error
		var open bool
		if err != nil {
			cancelCtx()
			return err
		}
		if handledErrorsN == 5 {
			break
		}
		select {
		case err, open = <-storeErrs:
			if err != nil {
				err = fmt.Errorf("storeErrs: %w", err)
				break
			}
			if !open {
				handledErrorsN++
				storeErrs = nil
			}
		case err, open = <-snapErrs:
			if err != nil {
				err = fmt.Errorf("snapErrs: %w", err)
				break
			}
			if !open {
				handledErrorsN++
				snapErrs = nil
			}
		case err, open = <-sinkSnapErrs:
			if err != nil {
				err = fmt.Errorf("sinkSnapErrs: %w", err)
				break
			}
			if !open {
				handledErrorsN++
				sinkSnapErrs = nil
			}
		case err, open = <-sendSnapErrs:
			if err != nil {
				err = fmt.Errorf("sendSnapErrs: %w", err)
				break
			}
			if !open {
				handledErrorsN++
				sendSnapErrs = nil
			}
		case err, open = <-pipeLineErrs:
			if err != nil {
				err = fmt.Errorf("pipeLineErrs: %w", err)
				break
			}
			if !open {
				handledErrorsN++
				pipeLineErrs = nil
			}
		}
	}
	return nil
}

// sendToCatTileD sends a batch of features to the Cat RPC client.
// It is a blocking function.
func sendToCatTileD[T any](ctx context.Context, c *Cat, args *tiled.PushFeaturesRequestArgs, in <-chan T) error {
	if !c.IsTilingRPCEnabled() {
		c.logger.Warn("Cat RPC client not configured (noop)", "method", "PushFeatures")
		go stream.Sink(ctx, nil, in) // Black hole, does not block.
		return nil
	}
	buf := new(bytes.Buffer)
	n, err := sinkStreamToJSONGZWriter(ctx, buf, in)
	if err != nil {
		c.logger.Error("Failed to sink stream to JSON GZ writer", "error", err)
		return err
	}
	// This can happen all the time.
	// Pipe direct in here and don't see any laps, naps, snaps or any other goodies
	// and this will fire.
	if n == 0 {
		c.logger.Debug("No features to send", "source", args.SourceName)
		return nil
	}
	if buf.Len() == 0 {
		c.logger.Error("Empty buffer, nonzero features", "source", args.SourceName, "count", n)
		return nil
	}
	c.logger.Info("Sending features to tiled RPC client", "source", args.SourceName,
		"count", n, "gz.len", humanize.Bytes(uint64(buf.Len())))
	args.GzippedJSONBytes = buf.Bytes()
	client, err := c.dialTiledRPC()
	if err != nil {
		return err
	}
	defer client.Close()
	reply := &tiled.PushFeaturesResponse{}
	err = client.Call("TileD.PushFeatures", args, reply)
	if err != nil {
		c.logger.Error("Failed to call RPC client",
			"method", "PushFeatures", "source", args.SourceName, "count", n, "error", err)
	}
	if reply.Error != nil {
		c.logger.Error("RPC client returned error", "error", err)
		return reply.Error
	}
	// Pulverize args, maybe sizeable.
	args = nil
	return nil
}

// sinkStreamToJSONGZWriter sinks a stream of T encoded as JSON to an adhoc gzip.Writer on wr.
// The gzip writer is closed before returning, and uses the default compression level.
// It is a blocking function, and returns the number of items written and/or the first error.
func sinkStreamToJSONGZWriter[T any](ctx context.Context, wr io.Writer, in <-chan T) (items int, err error) {
	defer func() {
		if items > 0 || err != nil {
			slog.Debug("Sunk stream to JSON GZ writer", "items", items, "error", err)
		}
	}()
	var gz *gzip.Writer
	gz, err = gzip.NewWriterLevel(wr, params.DefaultGZipCompressionLevel)
	if err != nil {
		return
	}
	defer gz.Close() // Ignore error, ensure assign returning error below.
	enc := json.NewEncoder(gz)
	for a := range in {
		if err := enc.Encode(a); err != nil {
			slog.Error("Failed to write", "error", err)
			return items, err
		}
		items++
	}
	// Assignment returning error, defer above.
	err = gz.Close()
	return items, err
}

// sinkStreamToJSONWriter sinks a stream of T encoded as JSON to wr.
// It is a blocking function, and returns the number of items encoded and/or the first error.
func sinkStreamToJSONWriter[T any](ctx context.Context, wr io.Writer, in <-chan T) (items int, err error) {
	defer func() {
		if items > 0 || err != nil {
			slog.Debug("Sunk stream to JSON writer", "items", items, "error", err)
		}
	}()
	enc := json.NewEncoder(wr)
	for a := range in {
		if err := enc.Encode(a); err != nil {
			slog.Error("Failed to write", "error", err)
			return items, err
		}
		items++
	}
	return items, nil
}

// sendGZippedToCatRPCClient sends a batch of gzipped features to the Cat RPC client.
// It is a non-blocking function, and registers itself with the Cat Waiting state.
//func sendGZippedToCatRPCClient[T any](ctx context.Context, c *Cat, args *tiled.PushFeaturesRequestArgs, in <-chan T) error {
//	if !c.IsTilingRPCEnabled() {
//		c.logger.Warn("Cat RPC client not configured (noop)", "method", "PushFeatures")
//		stream.Sink(ctx, nil, in)
//		return nil
//	}
//
//	all := stream.Collect(ctx, in)
//	if len(all) == 0 {
//		c.logger.Warn("No features to send", "source", args.SourceName)
//		return nil
//	}
//
//	buf := bytes.NewBuffer([]byte{})
//	gz, err := gzip.NewWriterLevel(buf, params.DefaultGZipCompressionLevel)
//	if err != nil {
//		c.logger.Error("Failed to create gzip writer", "error", err)
//		return err
//	}
//	enc := json.NewEncoder(gz)
//
//	for _, el := range all {
//		if err := enc.Encode(el); err != nil {
//			c.logger.Error("Failed to encode feature", "error", err)
//			return err
//		}
//	}
//
//	if err := gz.Flush(); err != nil {
//		c.logger.Error("Failed to flush gzip writer", "error", err)
//		return err
//	}
//
//	args.GzippedJSONBytes = buf.Bytes()
//
//	client, err := c.dialTiledRPC()
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//	err = client.Call("TileDaemon.PushFeatures", args, nil)
//	if err != nil {
//		c.logger.Error("Failed to call RPC client",
//			"method", "PushFeatures", "source", args.SourceName, "all.len", len(all), "error", err)
//	}
//	return err
//}
