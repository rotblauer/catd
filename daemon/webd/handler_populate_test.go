package webd

import (
	"github.com/dustin/go-humanize"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/testing/testdata"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestWebDaemon_populate(t *testing.T) {
	t.Run("rye_20241221", testPopulate_source("rye", testdata.Path(testdata.Source_LastPush_rye_20241221)))
	t.Run("ia_20241221", testPopulate_source("ia", testdata.Path(testdata.Source_LastPush_ia_20241221)))
}

func testPopulate_source(cat, source string) func(t *testing.T) {
	return func(t *testing.T) {
		d, teardown := newTestWebDaemon("")
		defer teardown()

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

		// check this push was stored
		catd := filepath.Join(d.Config.DataDir, params.CatsDir, cat)
		catdLastTracks := filepath.Join(catd, params.LastTracksGZFileName)
		lastFi, err := os.Stat(catdLastTracks)
		if err != nil {
			t.Fatal(err)
		}
		if lastFi.Size() == 0 {
			t.Fatal("last tracks file is empty")
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
			if size == 0 {
				t.Fatalf("track file %s is empty", match)
			}
			t.Logf("found ok yyyy-mm track file: %v size=%s", match, humanize.Bytes(uint64(size)))
		}
	}
}
