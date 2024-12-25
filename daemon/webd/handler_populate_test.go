package webd

import (
	"github.com/dustin/go-humanize"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/testing/testdata"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestWebDaemon_populate(t *testing.T) {
	d, teardown := newTestWebDaemon("")
	defer teardown()

	// rye: 100 features
	t.Run("rye_20241221", testPopulate_source(d, "rye", testdata.Path(testdata.Source_LastPush_rye_20241221), 100))
	// ia: 53 features
	t.Run("ia_20241221", testPopulate_source(d, "ia", testdata.Path(testdata.Source_LastPush_ia_20241221), 53))

	// master: 153 features, 2 lines
	reader, err := catz.NewGZFileReader(filepath.Join(d.Config.DataDir, params.MasterGZFileName))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.MaybeClose()
	count, err := reader.LineCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("wrong number of lines in master, got=%d, want=2", count)
	}
}

func testPopulate_source(d *WebDaemon, cat, source string, nFeatures int) func(t *testing.T) {
	return func(t *testing.T) {
		fi, err := os.Open(source)
		if err != nil {
			t.Fatal(err)
		}
		defer fi.Close()

		req := httptest.NewRequest("GET", "http://catsonmaps.org/populate", fi)
		w := httptest.NewRecorder()
		d.populate(w, req)
		fi.Close()
		resp := w.Result()
		t.Log(resp.StatusCode)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status code not ok")
		}

		// check this (last) push was stored
		catd := filepath.Join(d.Config.DataDir, params.CatsDir, cat)
		catdLastTracks := filepath.Join(catd, params.LastTracksGZFileName)
		lastFi, err := os.Stat(catdLastTracks)
		if err != nil {
			t.Fatal(err)
		}
		if lastFi.Size() < 1000 {
			t.Fatal("last tracks file is empty or too small")
		}

		// check this push has a nonempty canonical yyyy-mm track file
		matches, err := filepath.Glob(filepath.Join(catd, params.CatTracksDir, "20[0-9][0-9]-[0-1][0-9].geojson.gz"))
		if err != nil {
			t.Fatal(err)
		}
		if len(matches) == 0 {
			t.Fatal("no yyyy-mm track files")
		}
		for _, match := range matches {
			fi, err := os.Stat(match)
			if err != nil {
				t.Fatal(err)
			}
			size := fi.Size()
			if size < 1000 {
				t.Fatalf("track file %s is empty or too small, size=%d", match, size)
			}
			t.Logf("found ok yyyy-mm track file: %v size=%s", match, humanize.Bytes(uint64(size)))
		}

		// check the number of features in the last push
		reader, err := catz.NewGZFileReader(catdLastTracks)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.MaybeClose()
		count, err := reader.LineCount()
		if err != nil {
			t.Fatal(err)
		}
		if count != nFeatures {
			t.Fatalf("wrong number of features in last push, got=%d, want=%d", count, nFeatures)
		}
	}
}
