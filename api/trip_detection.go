package api

//
//import (
//	"context"
//	"github.com/paulmach/orb/simplify"
//	"github.com/rotblauer/catd/daemon/tiled"
//	"github.com/rotblauer/catd/params"
//	"github.com/rotblauer/catd/stream"
//	"github.com/rotblauer/catd/types/cattrack"
//)
//
//func (c *Cat) TripDetectionPipeline(ctx context.Context, in <-chan cattrack.CatTrack) {
//	lapTracks := make(chan cattrack.CatTrack)
//	napTracks := make(chan cattrack.CatTrack)
//	defer close(lapTracks)
//	defer close(napTracks)
//
//	cleaned := c.CleanTracks(ctx, in)
//	tripdetected := c.TripDetectTracks(ctx, cleaned)
//
//	// Synthesize new/derivative/aggregate features:
//	// ... LineStrings for laps, Points for naps.
//
//	// TrackLaps will send completed laps. Incomplete laps are persisted in KV
//	// and restored on cat restart.
//	// TODO Send incomplete lap on close. This will be nice to have.
//	_, completedLaps := c.TrackLaps(ctx, lapTracks)
//	filterLaps := stream.Filter(ctx, func(ct cattrack.CatLap) bool {
//		duration := ct.Properties["Duration"].(float64)
//		if duration < 120 {
//			return false
//		}
//		dist := ct.Properties.MustFloat64("Distance_Traversed", 0)
//		if dist < 100 {
//			return false
//		}
//		return true
//	}, completedLaps)
//
//	// Simplify the lap geometry.
//	simplifier := simplify.DouglasPeucker(params.DefaultLineStringSimplificationConfig.DouglasPeuckerThreshold)
//	simplified := stream.Transform(ctx, func(ct cattrack.CatLap) cattrack.CatLap {
//		cp := new(cattrack.CatLap)
//		*cp = ct
//		geom := simplifier.Simplify(ct.Geometry)
//		cp.Geometry = geom
//		return *cp
//	}, filterLaps)
//
//	// End of the line for all cat laps...
//	sinkLaps, sendLaps := stream.Tee(ctx, simplified)
//
//	wr, err := c.State.Flat.NamedGZWriter("laps.geojson.gz", nil)
//	if err != nil {
//		c.logger.Error("Failed to create custom writer", "error", err)
//		return
//	}
//	sinkStreamToJSONGZWriter(ctx, c, wr, sinkLaps)
//	sendBatchToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
//		SourceSchema: tiled.SourceSchema{
//			CatID:      c.CatID,
//			SourceName: "laps",
//			LayerName:  "laps",
//		},
//		TippeConfigName: params.TippeConfigNameLaps,
//		Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
//		SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
//	}, sendLaps)
//
//	// TrackNaps will send completed naps. Incomplete naps are persisted in KV
//	// and restored on cat restart.
//	completedNaps := c.TrackNaps(ctx, napTracks)
//	filteredNaps := stream.Filter(ctx, func(ct cattrack.CatNap) bool {
//		return ct.Properties.MustFloat64("Duration", 0) > 120
//	}, completedNaps)
//
//	// End of the line for all cat naps...
//	sinkNaps, sendNaps := stream.Tee(ctx, filteredNaps)
//
//	wr, err = c.State.Flat.NamedGZWriter("naps.geojson.gz", nil)
//	if err != nil {
//		c.logger.Error("Failed to create custom writer", "error", err)
//		return
//	}
//	sinkStreamToJSONGZWriter(ctx, c, wr, sinkNaps)
//	sendBatchToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
//		SourceSchema: tiled.SourceSchema{
//			CatID:      c.CatID,
//			SourceName: "naps",
//			LayerName:  "naps",
//		},
//		TippeConfigName: params.TippeConfigNameNaps,
//		Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
//		SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
//	}, sendNaps)
//
//	tripDetectedFork, tripDetectedPass := stream.Tee(ctx, tripdetected)
//	tripDetectedSink, tripDetectedSend := stream.Tee(ctx, tripDetectedPass)
//
//	wr, err = c.State.Flat.NamedGZWriter("tripdetected.geojson.gz", nil)
//	if err != nil {
//		c.logger.Error("Failed to create custom writer", "error", err)
//		return
//	}
//	sinkStreamToJSONGZWriter(ctx, c, wr, tripDetectedSink)
//	sendBatchToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
//		SourceSchema: tiled.SourceSchema{
//			CatID:      c.CatID,
//			SourceName: "tripdetected",
//			LayerName:  "tripdetected",
//		},
//		TippeConfigName: params.TippeConfigNameTripDetected,
//		Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
//		SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
//	}, tripDetectedSend)
//
//	// Block on tripdetect.
//	c.logger.Info("Trip detector blocking")
//	stream.Sink(ctx, func(ct cattrack.CatTrack) {
//		if ct.Properties.MustBool("IsTrip") {
//			lapTracks <- ct
//		} else {
//			napTracks <- ct
//		}
//	}, tripDetectedFork)
//}
