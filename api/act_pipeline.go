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
	"time"
)

func (c *Cat) CatActPipeline(ctx context.Context, in <-chan cattrack.CatTrack) {
	lapTracks := make(chan cattrack.CatTrack)
	napTracks := make(chan cattrack.CatTrack)
	defer close(lapTracks)
	defer close(napTracks)

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
		TippeConfigName: params.TippeConfigNameLaps,
		Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
		SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
	}, sendLaps)
	go stream.Sink(ctx, func(ct cattrack.CatLap) {
		c.completedLaps.Send(ct)
	}, notifyLaps)

	// TrackNaps will send completed naps. Incomplete naps are persisted in KV
	// and restored on cat restart.
	completedNaps := c.TrackNaps(ctx, napTracks)
	filteredNaps := stream.Filter(ctx, clean.FilterNaps, completedNaps)

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
		TippeConfigName: params.TippeConfigNameNaps,
		Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
		SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
	}, sendNaps)

	c.logger.Info("Act detection pipeline blocking")
	defer func() {
		c.logger.Info("Act detection pipeline unblocked")
	}()

	lastActiveTime := time.Time{}

	// Blocking.
	stream.Sink[cattrack.CatTrack](ctx, func(ct cattrack.CatTrack) {
		a := activity.FromString(ct.Properties.MustString("Activity", ""))
		if act.IsActivityActive(a) {

			lastActiveTime = ct.MustTime()
			lapTracks <- ct

		} else {

			// Flush last lap if cat is sufficiently napping.
			if !lastActiveTime.IsZero() &&
				ct.MustTime().Sub(lastActiveTime) > params.DefaultLapConfig.DwellInterval {
				ls.Flush()
				lastActiveTime = time.Time{}
			}

			napTracks <- ct
		}
	}, in)
}
