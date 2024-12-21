package webd

import (
	"github.com/rotblauer/catd/api"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"math"
	"net/http"
)

// populate is a handler for the /populate endpoint.
// It is where Cat Tracks get posted and persisted for-ev-er.
// Due to legacy support requirements it supports a variety of input formats.
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

	truncatedBytes := string(body)[:int(math.Min(80, float64(len(body))))]
	s.logger.Debug("Decoding", "body.len", len(body), "bytes: ", truncatedBytes)

	features, err := types.DecodeCatTracksShotgun(body)
	if err != nil || len(features) == 0 {
		s.logger.Error("Failed to decode", "error", err)
		http.Error(w, "Failed to decode", http.StatusUnprocessableEntity)
		return
	}

	// TODO: Assert/ensure WHICH CAT, ie. conceptual cat.
	catID := features[0].CatID()
	cat, err := api.NewCat(catID, params.DefaultCatDataDir(catID.String()), s.Config.CatBackendConfig)
	if err != nil {
		s.logger.Error("Failed to create cat", "error", err)
		http.Error(w, "Failed to create cat", http.StatusInternalServerError)
		return
	}

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
