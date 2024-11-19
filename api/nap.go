package api

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/app"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/geo/nap"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
)

func NapTracks(ctx context.Context, catID conceptual.CatID, in <-chan *cattrack.CatTrack) <-chan *cattrack.CatNap {
	out := make(chan *cattrack.CatNap)

	ns := nap.NewState(params.DefaultTripDetectorConfig.DwellInterval)

	// Attempt to restore lap-builder state.
	appCat := app.Cat{CatID: catID}
	if reader, err := appCat.NewCatReader(); err == nil {
		if data, err := reader.ReadKV([]byte("napstate")); err == nil && data != nil {
			if err := json.Unmarshal(data, ns); err != nil {
				slog.Error("Failed to unmarshal nap state", "error", err)
			} else {
				slog.Info("Restored nap state", "len", len(ns.Tracks), "last", ns.Tracks[len(ns.Tracks)-1].MustTime())
			}
		}
	}

	go func() {
		defer close(out)

		// Persist lap-builder state on completion.
		defer func() {
			writer, err := appCat.NewCatWriter()
			if err != nil {
				slog.Error("Failed to create cat writer", "error", err)
				return
			}
			data, err := json.Marshal(ns)
			if err != nil {
				slog.Error("Failed to marshal nap state", "error", err)
				return
			}
			if err := writer.WriteKV([]byte("napstate"), data); err != nil {
				slog.Error("Failed to write nap state", "error", err)
			}
			if err := writer.Close(); err != nil {
				slog.Error("Failed to close cat writer", "error", err)
			}
		}()

		completed := ns.Stream(ctx, in)
		for complete := range completed {
			out <- complete
		}
	}()

	return out
}
