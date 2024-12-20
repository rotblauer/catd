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
	"sort"
	"strconv"
)

func pingPong(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("pong"))
}

func lastKnown(w http.ResponseWriter, r *http.Request) {
	catID := r.URL.Query().Get("cat")
	cat := &api.Cat{CatID: conceptual.CatID(catID)}
	tracks, err := cat.S2CollectLevel(r.Context(), s2.CellLevel(0))
	if err != nil {
		slog.Warn("Failed to get last known", "error", err)
		http.Error(w, "Failed to get last known", http.StatusInternalServerError)
		return
	}
	if len(tracks) == 0 {
		http.Error(w, "No tracks found", http.StatusNotFound)
		return
	}
	// Freshest first.
	sort.SliceStable(tracks, func(i, j int) bool {
		return tracks[i].MustTime().Unix() > tracks[j].MustTime().Unix()
	})

	bs, err := json.Marshal(tracks[0])
	if err != nil {
		slog.Warn("Failed to marshal response", "error", err)
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}

	//// Remove TrackStacker (indexed) properties by hand, for now. (?)
	//for _, p := range []string{
	//	"Count",
	//	"VisitCount",
	//	"FirstTime",
	//	"LastTime",
	//	"ActivityMode",
	//	"TotalTimeOffset",
	//	"ActivityMode.Automotive",
	//	"ActivityMode.Bike",
	//	"ActivityMode.Fly",
	//	"ActivityMode.Running",
	//	"ActivityMode.Stationary",
	//	"ActivityMode.Unknown",
	//	"ActivityMode.Walking",
	//} {
	//	bs, err = sjson.DeleteBytes(bs, "properties."+p)
	//	if err != nil {
	//		slog.Warn("Failed to remove property", "error", err)
	//		http.Error(w, "Failed to remove geometry", http.StatusInternalServerError)
	//		return
	//	}
	//}

	if _, err := w.Write(bs); err != nil {
		slog.Error("Failed to write response", "error", err)
		http.Error(w, "Failed to write response", http.StatusInternalServerError)
		return
	}
}

func getStackerIndex(w http.ResponseWriter, r *http.Request) {
	catID := r.URL.Query().Get("cat")
	cat := &api.Cat{CatID: conceptual.CatID(catID)}
	if _, err := cat.WithState(true); err != nil {
		slog.Warn("Failed to get cat state", "error", err)
		http.Error(w, "Failed to get cat state", http.StatusInternalServerError)
		return
	}
	indexed := cattrack.StackerV1{}
	if err := cat.State.ReadKVUnmarshalJSON([]byte("state"), []byte("stacker"), &indexed); err != nil {
		slog.Warn("Failed to read stacker index", "error", err)
		http.Error(w, "Failed to read stacker index", http.StatusInternalServerError)
		return
	}
	ct := indexed.ApplyToCatTrack(&indexed, cattrack.CatTrack{})
	if err := json.NewEncoder(w).Encode(ct); err != nil {
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

func rGeoCollect(w http.ResponseWriter, r *http.Request) {
	catID := r.URL.Query().Get("cat")
	if catID == "" {
		http.Error(w, "Missing cat", http.StatusBadRequest)
		return
	}
	level := r.URL.Query().Get("dataset")
	l, err := strconv.ParseInt(level, 10, 64)
	if err != nil {
		slog.Warn("Failed to parse level", "error", err)
		http.Error(w, "Failed to parse level", http.StatusBadRequest)
		return
	}
	cat := &api.Cat{CatID: conceptual.CatID(catID)}
	indexedTracks, err := cat.RgeoCollectLevel(context.Background(), int(l))
	if err != nil {
		slog.Warn("Failed to get rgeo index dump", "error", err)
		http.Error(w, "Failed to get rgeo index dump", http.StatusInternalServerError)
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
	cat, err := api.NewCat(catID, s.Config.TileDaemonConfig)
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
