package api

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/app"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/geo/lap"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
)

func LapTracks(ctx context.Context, catID conceptual.CatID, in <-chan *cattrack.CatTrack) <-chan *cattrack.CatLap {
	out := make(chan *cattrack.CatLap)

	lb := lap.NewState(params.DefaultTripDetectorConfig.DwellInterval)

	// Attempt to restore lap-builder state.
	appCat := app.Cat{CatID: catID}
	if reader, err := appCat.NewCatReader(); err == nil {
		if data, err := reader.ReadKV([]byte("lapstate")); err == nil && data != nil {
			if err := json.Unmarshal(data, lb); err != nil {
				slog.Error("Failed to unmarshal lap state", "error", err)
			} else {
				slog.Info("Restored lap state")
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
			data, err := json.Marshal(lb)
			if err != nil {
				slog.Error("Failed to marshal lap state", "error", err)
				return
			}
			if err := writer.WriteKV([]byte("lapstate"), data); err != nil {
				slog.Error("Failed to write lap state", "error", err)
			}
			if err := writer.Close(); err != nil {
				slog.Error("Failed to close cat writer", "error", err)
			}
		}()

		completed := lb.Stream(ctx, in)
		for complete := range completed {
			out <- complete
		}
	}()

	return out
}
