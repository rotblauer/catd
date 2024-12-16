package api

import (
	"bytes"
	"context"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"testing"
)

func TestSinkStreamToJSONWriter(t *testing.T) {

	testdataPathGZ := "../testing/testdata/private/edge_1000.json.gz"
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

	all := stream.Collect(ctx, tracks)
	want := len(all)

	buf := new(bytes.Buffer)
	n, err := sinkStreamToJSONWriter(ctx, buf, stream.Slice(ctx, all))
	if err != nil {
		t.Log(n)
		t.Error(err)
	}
	if n != want {
		t.Errorf("got %d, want %d", n, want)
	}
	if buf.Len() == 0 {
		t.Error("empty buffer")
	}
	err = <-errs
	if err != nil {
		t.Fatal(err)
	}
}
