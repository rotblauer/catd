package webd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/rotblauer/catd/api"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"net/http"
)

// populate is a handler for the /populate endpoint.
// It is where Cat Tracks get posted.
// It supports a variety of input formats;
// Android (GCPS) posts a GeoJSON FeatureCollection (object).
// iOS (v.CustomizeableCatHat) posts an array of O.G. TrackPoints.
func (s *WebDaemon) populate(w http.ResponseWriter, r *http.Request) {
	var err error
	if r.Body == nil {
		s.logger.Error("No request body", "method", r.Method, "url", r.URL)
		http.Error(w, "Please send a request body", 500)
		return
	}

	cp := new(bytes.Buffer)
	tee := io.TeeReader(r.Body, cp)
	i, err := api.Master(s.Config.DataDir, tee)
	if err != nil {
		s.logger.Error("Failed to store master tracks", "error", err)
		http.Error(w, "Failed to store master tracks", http.StatusInternalServerError)
		return
	} else {
		s.logger.Info("Stored master tracks", "bytes", i, "path", params.MasterGZFileName)
	}

	buf := bufio.NewReader(cp)
	peek, _ := buf.Peek(80)
	s.logger.Info("Peeked request body", "peek", fmt.Sprintf("%s...", peek))

	var cat *api.Cat
	var catID conceptual.CatID

	first := make(chan cattrack.CatTrack, 1)
	tracks := make(chan cattrack.CatTrack, params.DefaultChannelCap)
	errs := make(chan error, 1)
	n := 0
	go func() {
		defer close(tracks)
		defer close(errs)
		s.logger.Debug("Scanning cat pop request")
		defer s.logger.Debug("Scanned cat pop request")
		er := types.ScanJSONMessages(buf, func(message json.RawMessage) error {
			return types.DecodingJSONTrackObject(message, func(ct *cattrack.CatTrack) error {
				if n == 0 {
					s.logger.Info("First track", "track", ct)
					first <- *ct
					close(first)
				}
				n++
				tracks <- *ct
				return nil
			})
		})
		if er != nil {
			s.logger.Error("Failed to scan cat pop message/s", "error", er)
		}
		errs <- er
	}()

	select {
	case t := <-first:
		if t.IsEmpty() {
			s.logger.Error("Empty first track", "method", r.Method, "url", r.URL)
			http.Error(w, "Empty first track", http.StatusBadRequest)
			return
		}
		catID = t.CatID()
		cat, err = api.NewCat(catID, params.DefaultCatDataDirRooted(s.Config.DataDir, catID.String()), s.Config.CatBackendConfig)
		if err != nil {
			s.logger.Error("Failed to get/create cat", "error", err)
			http.Error(w, "Failed to get/create cat", http.StatusInternalServerError)
			return
		}
	case err := <-errs:
		if err != nil {
			s.logger.Error("Failed to scan messages", "error", err)
			http.Error(w, "Failed to scan messages", http.StatusInternalServerError)
			return
		}
	}

	s.logger.Info("Populating", "cat", catID, "tracks", n)

	ctx := r.Context()
	err = cat.Populate(ctx, true, tracks)
	if err != nil {
		s.logger.Error("Failed to populate", "error", err)
		http.Error(w, "Failed to populate", http.StatusInternalServerError)
		return
	}

	// This weirdness satisfies the legacy clients.
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("[]")); err != nil {
		s.logger.Warn("Failed to write response", "error", err)
	}
}
