package webd

import (
	"context"
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/rotblauer/catd/api"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/testing/testdata"
	"github.com/rotblauer/catd/types/cattrack"
	"github.com/tidwall/gjson"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestWebDaemon_ping(t *testing.T) {
	req := httptest.NewRequest("GET", "http://catsonmaps.org/ping", nil)
	w := httptest.NewRecorder()
	pingPong(w, req)
	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)
	t.Log(resp.StatusCode)
	t.Log(resp.Header.Get("Content-Type"))
	t.Log(string(body))
	if resp.StatusCode != 200 {
		t.Fatalf("status code not 200")
	}
	if string(body) != "pong" {
		t.Errorf("body is not pong: %s", string(body))
	}
}

func TestWebDaemon_statusReport(t *testing.T) {
	req := httptest.NewRequest("GET", "http://catsonmaps.org/status", nil)
	w := httptest.NewRecorder()
	d, teardown := newTestWebDaemon("")
	defer teardown()
	time.Sleep(1 * time.Second)
	d.statusReport(w, req)
	resp := w.Result()
	t.Log(resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	status := webDaemonStatus{}
	err := json.Unmarshal(body, &status)
	if err != nil {
		t.Fatal(err)
	}
	if status.Uptime == "" {
		t.Fatal("uptime is empty")
	}
}

func TestWebDaemon_catIndex_NoCatThat(t *testing.T) {
	defer common.SlogResetLevel(slog.Level(slog.LevelWarn + 1))()
	req := httptest.NewRequest("GET", "http://catsonmaps.org/kitty/last.json", nil)
	req = mux.SetURLVars(req, map[string]string{"cat": "kitty"}) // hack
	w := httptest.NewRecorder()
	d, teardown := newTestWebDaemon("")
	defer teardown()
	d.catIndex(w, req)
	resp := w.Result()
	t.Log(resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status code not 204")
	}
	if !strings.Contains(string(body), "no cat that") {
		t.Errorf("body does not contain 'no cat that': %s", string(body))
	}
}

// TestWebDaemon_catIndex_Populated tests the catIndex handler with a populated index.
// It will not test the populate handler. Instead, it pushes tracks
// through api.Populate (direct). The index is then queried.
// WebDaemon can run on data it's never seen before.
func TestWebDaemon_catIndex_Populated(t *testing.T) {
	tc := api.NewTestCatWriter(t, "rye", nil)
	c := tc.Cat()
	defer tc.CloseAndDestroy()

	ctx := context.Background()
	tracks, errs := testdata.ReadSourceJSONGZ[cattrack.CatTrack](ctx, testdata.Path(testdata.Source_EDGE20241217))

	peek := <-tracks
	if peek.IsEmpty() {
		t.Fatal("empty first track")
	}
	t.Log("stream ok", peek.StringPretty())

	err := c.Populate(ctx, true, stream.Filter[cattrack.CatTrack](ctx,
		func(track cattrack.CatTrack) bool {
			return track.CatID() == "rye"
		}, tracks))
	if err != nil {
		t.Fatal(err)
	}
	err = <-errs
	if err != nil {
		t.Fatal(err)
	}
	err = c.Close()
	if !strings.Contains(err.Error(), "not open") {
		t.Fatal("expected error (already close)")
	}
	fi, err := os.Stat(c.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("cat data dir", c.DataDir, fi.Size())

	d, _ := newTestWebDaemon(c.DataDir)
	req := httptest.NewRequest("GET", "http://catsonmaps.org/xxx/last.json", nil)
	w := httptest.NewRecorder()
	req = mux.SetURLVars(req, map[string]string{"cat": "rye"})
	d.catIndex(w, req)
	resp := w.Result()
	t.Log(resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code not 200")
	}
	at, _ := time.Parse(time.RFC3339, "2024-12-16T22:59:42.959Z")
	count := 23561
	if gjson.GetBytes(body, "type").String() != "Feature" {
		t.Errorf("body does not contain type Feature")
	}
	if !gjson.GetBytes(body, "properties.Time").Time().Equal(at) {
		t.Errorf("body has wrong time Expected/Got\n%s\n%s\n", at.Format(time.RFC3339), gjson.Get(string(body), "properties.Time").String())
	}
	if gjson.GetBytes(body, "properties.Count").Int() != int64(count) {
		t.Errorf("body has wrong count Expected/Got\n%d\n%d\n", count, gjson.Get(string(body), "properties.Count").Int())
	}
	t.Log(string(body))
}
