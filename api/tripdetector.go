package api

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/app"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/geo/cleaner"
	"github.com/rotblauer/catd/geo/tripdetector"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"sync"
)

// TripDetectTracks detects trips in incoming CatTracks.
func TripDetectTracks(ctx context.Context, wg *sync.WaitGroup, catID conceptual.CatID, in <-chan *cattrack.CatTrack) {
	if wg != nil {
		wg.Add(1)
		defer wg.Done()
	}

	appCat := app.Cat{CatID: catID}
	writer, err := appCat.NewCatWriter()
	if err != nil {
		slog.Error("Failed to create cat writer", "error", err)
		return
	}

	accurate := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
		slog.Debug("Filtering accuracy", "track", ct.StringPretty())
		return cleaner.FilterAccuracy(ct)
	}, in)
	slow := stream.Filter(ctx, cleaner.FilterSpeed, accurate)
	low := stream.Filter(ctx, cleaner.FilterElevation, slow)
	uncanyoned := cleaner.WangUrbanCanyonFilter(ctx, low)
	unteleported := cleaner.TeleportationFilter(ctx, uncanyoned)

	td := tripdetector.NewTripDetector(params.DefaultTripDetectorConfig)
	tripdetected := stream.Transform(ctx, func(ct *cattrack.CatTrack) *cattrack.CatTrack {
		slog.Debug("Detecting trips", "track", ct.StringPretty())
		if err := td.Add(ct); err != nil {
			slog.Error("Failed to add track to trip detector", "error", err)
			return nil
		}

		// FIXME: These are causing a fatal concurrent map read and map write.
		// Can we use ID instead? Or some other hack?
		// Why hasn't this issue happened before? (e.g. Sanitized tracks)
		ct.Properties["IsTrip"] = td.Tripping
		ct.Properties["MotionStateReason"] = td.MotionStateReason

		//if td.Tripping {
		//	ct.ID = 1
		//} else {
		//	ct.ID = 0
		//}

		return ct
	}, unteleported)

	// Filter out the resets and inits.
	// Resets happen when the trip detector is reset after a signal loss.
	// Inits happen when the trip detector is initialized.
	tripdetectedValid := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
		//return true
		return ct.Properties.MustString("MotionStateReason") != "init" &&
			ct.Properties.MustString("MotionStateReason") != "reset"
	}, tripdetected)

	toMoving, toStationary := stream.Tee(ctx, tripdetectedValid)
	moving := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
		//return ct.ID == 1
		return ct.Properties.MustBool("IsTrip")
	}, toMoving)
	stationary := stream.Filter(ctx, func(ct *cattrack.CatTrack) bool {
		//return ct.ID == 0
		return !ct.Properties.MustBool("IsTrip")
	}, toStationary)

	// TODO: Coalesce moving points into linestrings, and stationary ones into stops.

	movingWriter, err := writer.CustomWriter("moving.geojson.gz")
	if err != nil {
		slog.Error("Failed to create moving writer", "error", err)
		return
	}
	defer movingWriter.Close()

	wroteMoving := stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
		slog.Debug("Writing moving track", "track", ct.StringPretty())
		if err := json.NewEncoder(movingWriter).Encode(ct); err != nil {
			slog.Error("Failed to write moving track", "error", err)
		}
		return nil
	}, moving)

	stationaryWriter, err := writer.CustomWriter("stationary.geojson.gz")
	if err != nil {
		slog.Error("Failed to create stationary writer", "error", err)
		return
	}
	defer stationaryWriter.Close()
	wroteStationary := stream.Transform(ctx, func(ct *cattrack.CatTrack) any {
		slog.Debug("Writing stationary track", "track", ct.StringPretty())
		if err := json.NewEncoder(stationaryWriter).Encode(ct); err != nil {
			slog.Error("Failed to write stationary track", "error", err)
		}
		return nil
	}, stationary)

	// Block.
	go stream.Drain(ctx, wroteMoving)
	stream.Drain(ctx, wroteStationary)

	return
}
