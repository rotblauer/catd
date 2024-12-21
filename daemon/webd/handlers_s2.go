package webd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/rotblauer/catd/api"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/s2"
	"log/slog"
	"net/http"
	"strconv"
)

func (s *WebDaemon) handleS2ParseCatLevel(w http.ResponseWriter, r *http.Request) (conceptual.CatID, s2.CellLevel, bool) {
	catID, ok := s.handleGetCatForRequest(w, r)
	if !ok {
		return "", 0, false
	}
	level, ok := mux.Vars(r)["level"]
	if !ok {
		slog.Warn("Missing level", "url", r.URL)
		http.Error(w, "Missing level", http.StatusBadRequest)
		return "", 0, false
	}
	l, err := strconv.ParseInt(level, 10, 64)
	if err != nil {
		slog.Warn("Failed to parse level", "error", err)
		http.Error(w, "Failed to parse level", http.StatusBadRequest)
		return "", 0, false
	}
	levelOK := false
	for _, c := range s2.DefaultCellLevels {
		if c == s2.CellLevel(l) {
			levelOK = true
		}
	}
	if !levelOK {
		slog.Warn("Invalid level", "level", l)
		http.Error(w, fmt.Sprintf("Invalid level, supported levels: %v", s2.DefaultCellLevels), http.StatusBadRequest)
		return "", 0, false
	}
	return conceptual.CatID(catID), s2.CellLevel(l), true
}

// s2Dump streams the S2 indices for a cat at a given level.
func (s *WebDaemon) s2Dump(w http.ResponseWriter, r *http.Request) {
	catID, l, ok := s.handleS2ParseCatLevel(w, r)
	if !ok {
		return
	}
	cat := &api.Cat{CatID: conceptual.CatID(catID)}

	// Dump the level to the response writer.
	err := cat.S2DumpLevel(w, s2.CellLevel(l))
	if err != nil {
		slog.Warn("Failed to write S2 index dump", "error", err)
		http.Error(w, "Failed to write S2 index dump", http.StatusInternalServerError)
		return
	}
}

// s2Collect writes a JSON array of the S2 indices for a cat at a given level.
func (s *WebDaemon) s2Collect(w http.ResponseWriter, r *http.Request) {
	catID, l, ok := s.handleS2ParseCatLevel(w, r)
	if !ok {
		return
	}
	// Limit the level for Collection because there could be millions.
	// 393K cells at level 8.
	if l > 8 {
		slog.Warn("Level too high (limit 8)", "level", l)
		http.Error(w, "Level too high (limit 8)", http.StatusBadRequest)
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
