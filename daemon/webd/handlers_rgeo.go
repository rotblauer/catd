package webd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/rotblauer/catd/rgeo"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
)

func (s *WebDaemon) rGeoCollect(w http.ResponseWriter, r *http.Request) {
	cat, ok := s.handleGetCatForRequest(w, r)
	if !ok {
		return
	}
	datasetParam, ok := mux.Vars(r)["datasetRe"]
	if !ok {
		slog.Warn("Failed to get dataset from request")
		http.Error(w, "Failed to get dataset from request", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(datasetParam, "(?i)") {
		datasetParam = "(?i)" + datasetParam
	}
	re, err := regexp.Compile(datasetParam)
	if err != nil {
		slog.Warn("Failed to compile dataset regexp", "error", err)
		http.Error(w, "Failed to compile dataset regexp", http.StatusBadRequest)
		return
	}
	matched := false
	datasetLevel := 0
	for i, n := range rgeo.DatasetNamesStable {
		if re.MatchString(n) {
			datasetLevel = i
			matched = true
		}
	}
	if !matched {
		slog.Warn("Failed to match dataset", "dataset", datasetParam)
		http.Error(w, fmt.Sprintf("Failed to match dataset, available datasets: %v",
			rgeo.DatasetNamesStable), http.StatusBadRequest)
		return
	}
	indexedTracks, err := cat.RgeoCollectLevel(context.Background(), int(datasetLevel))
	if err != nil {
		slog.Warn("Failed to get rgeo index dump", "error", err)
		http.Error(w, "Failed to get rgeo index dump", http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(indexedTracks); err != nil {
		slog.Warn("Failed to write response", "error", err)
	}
}
