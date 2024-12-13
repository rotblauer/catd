package api

import (
	"context"
	"github.com/paulmach/orb/simplify"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/geo/act"
	"github.com/rotblauer/catd/geo/clean"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/activity"
	"github.com/rotblauer/catd/types/cattrack"
	"sync"
	"time"
)

func (c *Cat) CatActPipeline(ctx context.Context, in <-chan cattrack.CatTrack) error {

	lapTracks := make(chan cattrack.CatTrack)
	napTracks := make(chan cattrack.CatTrack)

	// TrackLaps will send completed laps. Incomplete laps are persisted in KV
	// and restored on cat restart.
	// Act-detection logic below will flush the last lap if the cat is sufficiently napping.
	ls, completedLaps := c.TrackLaps(ctx, lapTracks)
	filterLaps := stream.Filter(ctx, clean.FilterLaps, completedLaps)

	// Simplify the lap geometry.
	simplifier := simplify.DouglasPeucker(params.DefaultLineStringSimplificationConfig.DouglasPeuckerThreshold)
	simplified := stream.Transform(ctx, func(ct cattrack.CatLap) cattrack.CatLap {
		cp := new(cattrack.CatLap)
		*cp = ct
		geom := simplifier.Simplify(ct.Geometry)
		cp.Geometry = geom
		return *cp
	}, filterLaps)

	// End of the line for all cat laps...
	sinkLaps := make(chan cattrack.CatLap)
	sendLaps := make(chan cattrack.CatLap)
	notifyLaps := make(chan cattrack.CatLap)
	stream.TeeMany(ctx, simplified, sinkLaps, sendLaps, notifyLaps)

	// TrackNaps will send completed naps. Incomplete naps are persisted in KV
	// and restored on cat restart.
	completedNaps := c.TrackNaps(ctx, napTracks)
	filteredNaps := stream.Filter(ctx, clean.FilterNaps, completedNaps)

	// End of the line for all cat naps...
	sinkNaps := make(chan cattrack.CatNap)
	sendNaps := make(chan cattrack.CatNap)
	notifyNaps := make(chan cattrack.CatNap)
	stream.TeeMany(ctx, filteredNaps, sinkNaps, sendNaps, notifyNaps)

	expectedErrsN := 4
	errCh := make(chan error, expectedErrsN)
	go func() {
		wr, err := c.State.Flat.NewGZFileWriter(params.LapsGZFileName, nil)
		if err != nil {
			c.logger.Error("Failed to create custom writer", "error", err)
			errCh <- err
			return
		}
		errCh <- sinkStreamToJSONGZWriter(ctx, c, wr, sinkLaps)
	}()
	go func() {
		wr, err := c.State.Flat.NewGZFileWriter(params.NapsGZFileName, nil)
		if err != nil {
			c.logger.Error("Failed to create custom writer", "error", err)
			errCh <- err
			return
		}
		errCh <- sinkStreamToJSONGZWriter(ctx, c, wr, sinkNaps)
	}()
	go func() {
		errCh <- sendToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
			SourceSchema: tiled.SourceSchema{
				CatID:      c.CatID,
				SourceName: "laps",
				LayerName:  "laps",
			},
			TippeConfigName: params.TippeConfigNameLaps,
			Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
			SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
		}, sendLaps)
	}()
	go func() {
		errCh <- sendToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
			SourceSchema: tiled.SourceSchema{
				CatID:      c.CatID,
				SourceName: "naps",
				LayerName:  "naps",
			},
			TippeConfigName: params.TippeConfigNameNaps,
			Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
			SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
		}, sendNaps)
	}()

	go stream.Sink(ctx, func(ct cattrack.CatLap) {
		c.completedLaps.Send(ct)
	}, notifyLaps)

	go stream.Sink(ctx, func(ct cattrack.CatNap) {
		c.completedNaps.Send(ct)
	}, notifyNaps)

	sinkWG := sync.WaitGroup{}
	sinkWG.Add(1)
	sinkErr := make(chan error, expectedErrsN)
	go func() {
		c.logger.Info("Act detection waiting on errors")
		defer sinkWG.Done()
		defer func() {
			close(sinkErr)
			sinkErr = nil
		}()
		defer func() {
			c.logger.Debug("Act detection pipeline errors complete")
		}()
		for i := 0; i < expectedErrsN; i++ {
			select {
			case err := <-errCh:
				if err != nil {
					sinkErr <- err
					c.logger.Error("Act detection pipeline error (looper: err)", "error", err)
				}
			}
		}
	}()

	// Blocking.
	c.logger.Info("Act detection pipeline blocking")
	defer func() {
		c.logger.Info("Act detection pipeline unblocked")
	}()
	lastActiveTime := time.Time{}
	for ct := range in {
		select {
		case <-ctx.Done():
			return nil
		case err, open := <-sinkErr:
			if !open {
				panic("impossible")
			}
			if err != nil {
				c.logger.Error("Act detection pipeline error (looper: in)", "error", err)
				return err
			}
		default:
			a := activity.FromString(ct.Properties.MustString("Activity", ""))
			if act.IsActivityActive(a) {

				lastActiveTime = ct.MustTime()
				lapTracks <- ct

			} else {

				// Flush last lap if cat is sufficiently napping.
				if !lastActiveTime.IsZero() &&
					ct.MustTime().Sub(lastActiveTime) > params.DefaultLapConfig.Interval {
					ls.Flush()
					lastActiveTime = time.Time{}
				}

				napTracks <- ct
			}
		}
	}
	close(lapTracks)
	close(napTracks)
	lapTracks = nil
	napTracks = nil
	sinkWG.Wait()
	return nil
}
