package node

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/api"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"log/slog"
	"math"
	"net/http"
	"slices"
)

func handleLastKnown(w http.ResponseWriter, r *http.Request) {
	result := api.LastKnown(api.LastKnownQuery{})
	if err := json.NewEncoder(w).Encode(result); err != nil {
		slog.Warn("Failed to write response", "error", err)
	}
}

func handleGetCatSnaps(w http.ResponseWriter, r *http.Request) {
	if err := json.NewEncoder(w).Encode([]byte("implement me")); err != nil {
		slog.Warn("Failed to write response", "error", err)
	}
}

// handlePopulate is a handler for the /populate endpoint.
// It is where Cat Tracks get posted and persisted for-ev-er.
// Due to legacy support requirements it supports a variety of
// input formats.
// Android (GCPS) posts a GeoJSON FeatureCollection.
// iOS (v.CustomizeableCatHat) posts an array of O.G. TrackPoints.
func handlePopulate(w http.ResponseWriter, r *http.Request) {

	var body []byte
	var err error

	if r.Body == nil {
		slog.Warn("No request body", "method", r.Method, "url", r.URL)
		http.Error(w, "Please send a request body", 500)
		return
	}

	body, err = io.ReadAll(r.Body)
	if err != nil {
		slog.Warn("Failed to read request body", "error", err)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	slog.Debug("Decoding", len(body), "bytes: ", string(body)[:int(math.Min(80, float64(len(body))))])

	features, err := types.DecodeCatTracksShotgun(body)
	if err != nil {
		slog.Warn("Failed to decode", "error", err)
		http.Error(w, "Failed to decode", http.StatusUnprocessableEntity)
		return
	}

	// FIXME: Running a sort on unvalidated data is a bad idea.
	slices.SortFunc(features, cattrack.Sorter)

	ctx := context.Background()
	ch := stream.Slice(ctx, features)
	err = api.Populate(ctx, ch)
	if err != nil {
		slog.Warn("Failed to populate", "error", err)
		http.Error(w, "Failed to populate", http.StatusInternalServerError)
		return
	}

	// This weirdness is to satisfy the legacy clients,
	// but it's not an API pattern I like.
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("[]")); err != nil {
		slog.Warn("Failed to write response", "error", err)
	}
}
