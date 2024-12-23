package webd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/rotblauer/catd/api"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"time"
)

func pingPong(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("pong"))
}

type webDaemonStatus struct {
	StartedAt time.Time               `json:"started_at"`
	Uptime    string                  `json:"uptime"`
	Config    *params.WebDaemonConfig `json:"config"`
	WSOpen    bool                    `json:"ws_open"`
	WSConns   int                     `json:"ws_conns"`
}

func (s *WebDaemon) statusReport(w http.ResponseWriter, r *http.Request) {
	st := webDaemonStatus{
		StartedAt: s.started,
		Uptime:    time.Since(s.started).Round(time.Second).String(),
		Config:    s.Config,
	}
	if s.melodyInstance != nil {
		st.WSOpen = !s.melodyInstance.IsClosed()
		st.WSConns = s.melodyInstance.Len()
	}
	j, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		s.logger.Error("Failed to marshal config", "error", err)
		http.Error(w, "Failed to marshal config", http.StatusInternalServerError)
		return
	}
	_, err = w.Write(j)
	if err != nil {
		s.logger.Error("Failed to write response", "error", err)
		http.Error(w, "Failed to write response", http.StatusInternalServerError)
	}
}

func getRequestCatID(r *http.Request) conceptual.CatID {
	vars := mux.Vars(r)
	catID, ok := vars["cat"]
	if ok {
		return conceptual.CatID(catID)
	}
	catID = r.URL.Query().Get("cat")
	return conceptual.CatID(catID)
}

// handleGetCatForRequest looks like middleware.
func (s *WebDaemon) handleGetCatForRequest(w http.ResponseWriter, r *http.Request) (*api.Cat, bool) {
	catID := getRequestCatID(r)
	if catID.IsEmpty() {
		slog.Warn("Missing cat", "url", r.URL)
		http.Error(w, "Missing cat", http.StatusBadRequest)
		return nil, false
	}
	return &api.Cat{
		CatID:   catID,
		DataDir: s.Config.DataDir,
	}, true
}

// catIndex returns the last known, cumulative offset index for a cat.
// Gives last-known track merged with total track count and time offset data.
// Failure to find such cat results in a 'no cat that' 204 error.
func (s *WebDaemon) catIndex(w http.ResponseWriter, r *http.Request) {
	cat, ok := s.handleGetCatForRequest(w, r)
	if !ok {
		return
	}
	if err := cat.LockOrLoadState(true); err != nil {
		slog.Warn("Failed to get cat state (no cat that?)", "cat", cat.CatID, "error", err)
		http.Error(w, fmt.Sprintf("Failed to get cat state '%s' (no cat that?)", cat.CatID.String()), http.StatusNoContent)
		return
	}
	defer cat.State.Close()

	indexed := cattrack.CatTrack{}
	if err := cat.State.ReadKVUnmarshalJSON(params.CatStateBucket, params.CatStateKey_OffsetIndexer, &indexed); err != nil {
		slog.Warn("Failed to read offset index", "error", err)
		http.Error(w, "Failed to read offset index", http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(indexed); err != nil {
		slog.Warn("Failed to write response", "error", err)
	}
}

// catPushedJSON writes the last-push tracks for a cat.
// Writes a JSON array of the last n [eg. 100] pushed tracks.
// The JSON-array limit is defined only to avoid massive batches during testing;
// real cats won't push 100k tracks at a time.
func (s *WebDaemon) catPushedJSON(w http.ResponseWriter, r *http.Request) {
	cat, ok := s.handleGetCatForRequest(w, r)
	if !ok {
		return
	}
	if err := cat.LockOrLoadState(true); err != nil {
		slog.Warn("Failed to get cat state", "error", err)
		http.Error(w, "Failed to get cat state", http.StatusInternalServerError)
		return
	}
	defer cat.State.Close()

	lr, err := cat.State.Flat.NewGZFileReader(params.LastTracksGZFileName)
	if err != nil {
		slog.Warn("Failed to get last tracks", "error", err)
		http.Error(w, "Failed to get last tracks", http.StatusInternalServerError)
		return
	}
	defer lr.Close()

	// Write a JSON array of the n [eg. 100] last-pushed tracks.
	//limit := 100
	//buf := common.NewRingBuffer[cattrack.CatTrack](limit)
	tracks := []cattrack.CatTrack{}
	dec := json.NewDecoder(lr)
	for {
		track := cattrack.CatTrack{}
		if err := dec.Decode(&track); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			slog.Error("Failed to decode track", "error", err)
			http.Error(w, "Failed to decode track", http.StatusInternalServerError)
			return
		}
		tracks = append(tracks, track)
		//buf.Add(track)
	}
	//if err := json.NewEncoder(w).Encode(buf.Get()); err != nil {
	//	slog.Error("Failed to write response", "error", err)
	//	http.Error(w, "Failed to write response", http.StatusInternalServerError)
	//}
	if err := json.NewEncoder(w).Encode(tracks); err != nil {
		slog.Error("Failed to write response", "error", err)
		http.Error(w, "Failed to write response", http.StatusInternalServerError)
	}
}

// catPushedNDJSON writes the last-push tracks for a cat.
// Writes a NDJSON stream of the last batch pushed. No limit.
func (s *WebDaemon) catPushedNDJSON(w http.ResponseWriter, r *http.Request) {
	cat, ok := s.handleGetCatForRequest(w, r)
	if !ok {
		return
	}
	if err := cat.LockOrLoadState(true); err != nil {
		slog.Warn("Failed to get cat state", "error", err)
		http.Error(w, "Failed to get cat state", http.StatusInternalServerError)
		return
	}
	defer cat.State.Close()

	lr, err := cat.State.Flat.NewGZFileReader(params.LastTracksGZFileName)
	if err != nil {
		slog.Warn("Failed to get last tracks", "error", err)
		http.Error(w, "Failed to get last tracks", http.StatusInternalServerError)
		return
	}
	defer lr.Close()

	// Stream JSON tracks.
	_, err = io.Copy(w, lr)
	if err != nil {
		slog.Error("Failed to write response", "error", err)
		http.Error(w, "Failed to write response", http.StatusInternalServerError)
		return
	}
}

func (s *WebDaemon) getCatSnaps(w http.ResponseWriter, r *http.Request) {
	cat, ok := s.handleGetCatForRequest(w, r)
	if !ok {
		return
	}
	if err := cat.LockOrLoadState(true); err != nil {
		slog.Warn("Failed to get cat state", "error", err)
		http.Error(w, "Failed to get cat state", http.StatusInternalServerError)
		return
	}
	defer cat.State.Close()

	// TODO
	// Snaps are in the bucket but no way yet of scanning/reading them.
	//
	//snaps := []cattrack.CatTrack{}
	//err := cat.State.ReadKVUnmarshalJSON(params.CatStateBucket, params.CatStateKey_Snaps, &snaps)
	//if err != nil {
	//	slog.Warn("Failed to read snaps", "error", err)
	//	http.Error(w, "Failed to read snaps", http.StatusInternalServerError)
	//	return
	//}
	//if err := json.NewEncoder(w).Encode(snaps); err != nil {
	//	slog.Warn("Failed to write response", "error", err)
	//	http.Error(w, "Failed to write response", http.StatusInternalServerError)
	//	return
	//}
}

// lastKnown2 returns the most recent track for a cat using the S2 index.
func lastKnownS2(w http.ResponseWriter, r *http.Request) {
	catID := r.URL.Query().Get("cat")
	cat := &api.Cat{CatID: conceptual.CatID(catID)}

	// HACK: Return the freshest face from an S2 dump at level 0.
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

	if _, err := w.Write(bs); err != nil {
		slog.Error("Failed to write response", "error", err)
		http.Error(w, "Failed to write response", http.StatusInternalServerError)
		return
	}
}
