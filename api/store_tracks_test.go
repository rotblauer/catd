package api

import (
	"context"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var testdataPathGZ = "../testing/testdata/private/rye_2024-12.geojson.gz"

/*
zcat testing/testdata/private/rye_2024-12.geojson.gz | wc -l
444358

zcat testing/testdata/private/rye_2024-12.geojson.gz | head -1 | jj -p
{
  "id": 0,
  "type": "Feature",
  "geometry": {
    "type": "Point",
    "coordinates": [-93.25556182861328, 44.98896408081055]
  },
  "properties": {
    "Accuracy": 2,
    "Activity": "Stationary",
    "Alias": "rye",
    "AverageActivePace": 0.81,
    "BatteryLevel": 0.8,
    "BatteryStatus": "charging",
    "CurrentCadence": 1.49,
    "CurrentPace": 0.87,
    "CurrentTripStart": "2024-11-22T20:41:23.217Z",
    "Distance": 48336.42,
    "Elevation": 322.39,
    "FloorsAscended": 48,
    "FloorsDescended": 50,
    "Heading": -1,
    "HeartRate": 73,
    "Name": "Rye16",
    "NetworkInfo": "{\"ssidData\":\"{length = 12, bytes = 0x42616e616e6120486f74656c}\",\"bssid\":\"6c:70:9f:de:59:89\",\"ssid\":\"Banana Hotel\"}",
    "NumberOfSteps": 52981,
    "Pressure": 98.46,
    "Speed": -1,
    "Time": "2024-12-01T07:00:10.406Z",
    "UUID": "5D37B5DA-6E0B-41FE-8A72-2BB681D661DA",
    "UnixTime": 1733036410,
    "Version": "V.customizableCatTrackHat",
    "catdReceivedAt": 1734285147
  }
}
*/
var testdataPathGZLines = 444358

var TestDatadirRoot = filepath.Join(os.TempDir(), "catd_test")

func init() {
	params.DatadirRoot = TestDatadirRoot
}

func NewTestCat(t *testing.T) *Cat {
	c, err := NewCat("rye", nil)
	if err != nil {
		t.Fatal(err)
	}
	st, err := c.WithState(false)
	if err != nil || st == nil {
		t.Fatal(err)
	}

	return c
}

func TestCat_StoreTracks(t *testing.T) {
	os.RemoveAll(TestDatadirRoot)
	c := NewTestCat(t)
	defer c.Close()

	gzr, err := catz.NewGZFileReader(testdataPathGZ)
	if err != nil {
		t.Fatal(err)
	}
	defer gzr.Close()

	ctx := context.Background()
	tracks, errs := stream.NDJSON[cattrack.CatTrack](ctx, gzr)

	first := <-tracks
	if first.IsEmpty() {
		t.Fatal("no tracks")
	}
	t.Log("stream ok", first.StringPretty())

	err = <-c.StoreTracks(ctx, tracks)
	if err != nil {
		t.Fatal(err)
	}
	err = <-errs
	if err != nil {
		t.Fatal(err)
	}

	validateTrackGZFile(t, filepath.Join(c.State.Flat.Path(), params.LastTracksGZFileName))
	validateTrackGZFile(t, filepath.Join(c.State.Flat.Path(), params.TracksGZFileName))
}

func TestCat_StoreTracksYYYYMM(t *testing.T) {
	os.RemoveAll(TestDatadirRoot)
	c := NewTestCat(t)
	defer c.Close()

	testdataPathGZ := "../testing/testdata/private/rye_2024-12.geojson.gz"
	gzr, err := catz.NewGZFileReader(testdataPathGZ)
	if err != nil {
		t.Fatal(err)
	}
	defer gzr.Close()

	ctx := context.Background()
	tracks, errs := stream.NDJSON[cattrack.CatTrack](ctx, gzr)

	first := <-tracks
	if first.IsEmpty() {
		t.Fatal("no tracks")
	}
	t.Log("stream ok", first.StringPretty())

	err = <-c.StoreTracksYYYYMM(ctx, tracks)
	if err != nil {
		t.Fatal(err)
	}
	err = <-errs
	if err != nil {
		t.Fatal(err)
	}

	validateTrackGZFile(t, filepath.Join(c.State.Flat.Path(), params.LastTracksGZFileName))
	validateTrackGZFile(t, filepath.Join(c.State.Flat.Path(), "tracks", "2024-12.geojson.gz"))
}

func TestCat_StoreTracksYYYYMM_ManyTimes(t *testing.T) {
	os.RemoveAll(TestDatadirRoot)
	c := NewTestCat(t)
	defer c.Close()

	testdataPathGZ := "../testing/testdata/private/rye_2024-12.geojson.gz"
	gzr, err := catz.NewGZFileReader(testdataPathGZ)
	if err != nil {
		t.Fatal(err)
	}
	defer gzr.Close()

	ctx := context.Background()
	tracks, errs := stream.NDJSON[cattrack.CatTrack](ctx, gzr)

	firstFakeTime := time.Now().Round(time.Second)
	fakeTime := firstFakeTime
	tracksToFuture := stream.Transform(ctx, func(ct cattrack.CatTrack) cattrack.CatTrack {
		// Just a sanity check.
		if !ct.IsValid() {
			t.Fatal("invalid track", ct)
		}
		// Bump time ahead by 1 hour.
		// Must set both Time and UnixTime, cattrack.CatTrack.Time() will prefer the latter.
		ct.SetPropertySafe("Time", fakeTime)
		ct.SetPropertySafe("UnixTime", fakeTime.Unix())
		fakeTime = fakeTime.Add(1 * time.Hour)
		return ct
	}, tracks)

	first := <-tracksToFuture
	if first.IsEmpty() {
		t.Fatal("no tracks")
	}
	t.Log("stream ok", first.StringPretty())
	if !first.MustTime().Equal(firstFakeTime) {
		t.Fatal("time not set", first.MustTime(), firstFakeTime)
	}

	err = <-c.StoreTracksYYYYMM(ctx, tracksToFuture)
	if err != nil {
		t.Fatal(err)
	}
	err = <-errs
	if err != nil {
		t.Fatal(err)
	}

	validateTrackGZFile(t, filepath.Join(c.State.Flat.Path(), params.LastTracksGZFileName))
	matches, err := filepath.Glob(filepath.Join(c.State.Flat.Path(), "tracks", "*.geojson.gz"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) < 14 {
		t.Fatal("no track files")
	}
	foundFirst, foundFuture := false, false
	for _, path := range matches {
		if strings.Contains(path, filepath.Join("tracks", firstFakeTime.Format("2006-01")+".geojson.gz")) {
			foundFirst = true
		}
		if strings.Contains(path, filepath.Join("tracks", fakeTime.Format("2006-01")+".geojson.gz")) {
			foundFuture = true
		}
		validateTrackGZFile(t, path)
	}
	if !foundFirst || !foundFuture {
		t.Fatal("track files missing")
	}
	t.Log("found all track files",
		len(matches), matches[0], "...", matches[len(matches)-1])
}

func validateTrackGZFile(t *testing.T, path string) {
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() == 0 {
		t.Fatal("file is empty")
	}

	gzr, err := catz.NewGZFileReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer gzr.Close()

	ctx := context.Background()
	tracks, errs := stream.NDJSON[cattrack.CatTrack](context.Background(), gzr)
	out := stream.Collect[cattrack.CatTrack](ctx, tracks)
	if len(out) == 0 {
		t.Fatal("no tracks")
	}
	for _, tr := range out {
		if !tr.IsValid() {
			t.Fatal("invalid track", tr)
		}
	}
	err = <-errs
	if err != nil {
		t.Fatal(err)
	}
}
