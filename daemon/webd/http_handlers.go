package webd

import (
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/api"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
)

func pingPong(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("pong"))
}

func lastTracks(w http.ResponseWriter, r *http.Request) {
	catID := r.URL.Query().Get("cat")
	cat := &api.Cat{CatID: conceptual.CatID(catID)}
	result, err := cat.LastKnown()
	if err != nil {
		slog.Warn("Failed to get last known", "error", err)
		http.Error(w, "Failed to get last known", http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(result); err != nil {
		slog.Warn("Failed to write response", "error", err)
	}
}

func getCatSnaps(w http.ResponseWriter, r *http.Request) {
	if err := json.NewEncoder(w).Encode([]byte("implement me")); err != nil {
		slog.Warn("Failed to write response", "error", err)
	}
}

// s2Dump is a debug tool.
func s2Dump(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	// https://github.com/ipfs/kubo/issues/3737
	// https://stackoverflow.com/questions/57301886/what-is-the-suitable-http-content-type-for-consuming-an-asynchronous-stream-of-d
	// w.Header().Set("Content-Type", "application/stream+json")

	catID := r.URL.Query().Get("cat")
	if catID == "" {
		http.Error(w, "Missing cat", http.StatusBadRequest)
		return
	}
	level := r.URL.Query().Get("level")
	l, err := strconv.ParseInt(level, 10, 64)
	if err != nil {
		slog.Warn("Failed to parse level", "error", err)
		http.Error(w, "Failed to parse level", http.StatusBadRequest)
		return
	}
	cat := &api.Cat{CatID: conceptual.CatID(catID)}
	err = cat.S2DumpLevel(w, s2.CellLevel(l))
	if err != nil {
		slog.Warn("Failed to write S2 index dump", "error", err)
		http.Error(w, "Failed to write S2 index dump", http.StatusInternalServerError)
		return
	}
}

// s2Collect is a debug tool.
func s2Collect(w http.ResponseWriter, r *http.Request) {
	catID := r.URL.Query().Get("cat")
	if catID == "" {
		http.Error(w, "Missing cat", http.StatusBadRequest)
		return
	}
	level := r.URL.Query().Get("level")
	l, err := strconv.ParseInt(level, 10, 64)
	if err != nil {
		slog.Warn("Failed to parse level", "error", err)
		http.Error(w, "Failed to parse level", http.StatusBadRequest)
		return
	}
	cat := &api.Cat{CatID: conceptual.CatID(catID)}
	indexedTracks, err := cat.S2CollectLevel(context.Background(), s2.CellLevel(l))
	if err != nil {
		slog.Warn("Failed to get S2 index dump", "error", err)
		http.Error(w, "Failed to get S2 index dump", http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(indexedTracks); err != nil {
		slog.Warn("Failed to write response", "error", err)
	}
}

// populate is a handler for the /populate endpoint.
// It is where Cat Tracks get posted and persisted for-ev-er.
// Due to legacy support requirements it supports a variety of
// input formats.
// Android (GCPS) posts a GeoJSON FeatureCollection.
// iOS (v.CustomizeableCatHat) posts an array of O.G. TrackPoints.
func (s *WebDaemon) populate(w http.ResponseWriter, r *http.Request) {

	var body []byte
	var err error

	if r.Body == nil {
		s.logger.Error("No request body", "method", r.Method, "url", r.URL)
		http.Error(w, "Please send a request body", 500)
		return
	}

	// TODO.
	// Could potentially use a streaming JSON decoder here.
	// customizeableTrackHat sends array of trackpoints.
	// gcps sends a feature collection.
	// Like herding cats.
	body, err = io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Failed to read request body", "error", err)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	s.logger.Debug("Decoding", len(body), "bytes: ", string(body)[:int(math.Min(80, float64(len(body))))])

	features, err := types.DecodeCatTracksShotgun(body)
	if err != nil || len(features) == 0 {
		s.logger.Error("Failed to decode", "error", err)
		http.Error(w, "Failed to decode", http.StatusUnprocessableEntity)
		return
	}

	// TODO: Assert/ensure WHICH CAT better.
	catID := features[0].CatID()
	cat := api.NewCat(catID, s.Config.TileDaemonConfig)

	// Collect values for streaming.
	featureVals := make([]cattrack.CatTrack, len(features))
	for i, f := range features {
		featureVals[i] = *f
	}

	ctx := r.Context()
	err = cat.Populate(ctx, true, stream.Slice(ctx, featureVals))
	if err != nil {
		s.logger.Error("Failed to populate", "error", err)
		http.Error(w, "Failed to populate", http.StatusInternalServerError)
		return
	}

	// This weirdness is to satisfy the legacy clients,
	// but it's not an API pattern I like.
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("[]")); err != nil {
		s.logger.Warn("Failed to write response", "error", err)
	}

	s.feedPopulated.Send(features)
}
