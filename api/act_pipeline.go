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

	lapTracks := make(chan cattrack.CatTrack, params.DefaultChannelCap)
	napTracks := make(chan cattrack.CatTrack, params.DefaultChannelCap)

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

	expectedErrsN := 4 // 2 sink, 2 send.
	errCh := make(chan error, expectedErrsN)
	lapsNapsMap := map[string]<-chan cattrack.CatTrack{
		params.LapsGZFileName: stream.Transform(ctx, cattrack.Lap2Track, sinkLaps),
		params.NapsGZFileName: stream.Transform(ctx, cattrack.Nap2Track, sinkNaps),
	}
	for to, ch := range lapsNapsMap {
		go func(ch <-chan cattrack.CatTrack, path string) {
			wr, err := c.State.Flat.NewGZFileWriter(path, nil)
			if err != nil {
				c.logger.Error("Failed to create gz file writer", "path", path, "error", err)
				errCh <- err
				return
			}
			_, err = sinkStreamToJSONWriter(ctx, wr, ch)
			if err != nil {
				c.logger.Error("Failed to sink stream", "path", path, "error", err)
				wr.Close()
			} else {
				err = wr.Close()
			}
			errCh <- err
		}(ch, to)
	}

	go func() {
		errCh <- sendToCatTileD(ctx, c, &tiled.PushFeaturesRequestArgs{
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
		errCh <- sendToCatTileD(ctx, c, &tiled.PushFeaturesRequestArgs{
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

	// There's no way the waitgroups are necessary, but they probably don't hurt.
	notifyWG := sync.WaitGroup{}
	notifyWG.Add(2)
	go func() {
		defer notifyWG.Done()
		stream.Sink(ctx, func(ct cattrack.CatLap) {
			c.completedLaps.Send(ct)
		}, notifyLaps)
	}()
	go func() {
		defer notifyWG.Done()
		stream.Sink(ctx, func(ct cattrack.CatNap) {
			c.completedNaps.Send(ct)
		}, notifyNaps)
	}()

	sinkWG := sync.WaitGroup{}
	sinkWG.Add(1)
	sinkErr := make(chan error, expectedErrsN)
	go func() {
		defer sinkWG.Done()
		defer close(sinkErr)
		defer c.logger.Debug("Act detection pipeline errors complete")
		c.logger.Info("Act detection waiting on errors")
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
			close(lapTracks)
			close(napTracks)
			return nil
		case err, open := <-sinkErr:
			if !open {
				panic("impossible")
			}
			if err != nil {
				c.logger.Error("Act detection pipeline error (looper: in)", "error", err)
				close(lapTracks)
				close(napTracks)
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
					ls.Bump()
					lastActiveTime = time.Time{}
				}
				napTracks <- ct
			}
		}
	}
	close(lapTracks)
	close(napTracks)
	sinkWG.Wait()
	notifyWG.Wait()
	return nil
}
