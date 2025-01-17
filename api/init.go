package api

import (
	"context"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var TestDatadirRoot = filepath.Join(os.TempDir(), "catd_api_test")

type TestCat Cat

func (c *TestCat) Cat() *Cat {
	return (*Cat)(c)
}

// NewTestCatWriter creates a new cat with a writable state
// in a temporary directory inside the TestDatadirRoot.
func NewTestCatWriter(t *testing.T, name string, backend *params.CatRPCServices) *TestCat {
	err := os.MkdirAll(TestDatadirRoot, 0770)
	if err != nil {
		t.Fatal(err)
	}
	tmpCatD, err := os.MkdirTemp(TestDatadirRoot, name)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewCat(conceptual.CatID(name), tmpCatD, backend)
	if err != nil {
		t.Fatal(err)
	}
	err = c.LockOrLoadState(false)
	if err != nil {
		t.Fatal(err)
	}
	return (*TestCat)(c)
}

func NewTestCatReader(t *testing.T, name, d string, backend *params.CatRPCServices) *TestCat {
	c, err := NewCat(conceptual.CatID(name), d, backend)
	if err != nil {
		t.Fatal(err)
	}
	err = c.LockOrLoadState(true)
	if err != nil {
		t.Fatal(err)
	}
	return (*TestCat)(c)
}

func (c *TestCat) CloseAndDestroy() error {
	err := (*Cat)(c).State.Close()
	if err != nil {
		return err
	}
	p := (*Cat)(c).State.Flat.Path()
	if len(p) < 5 {
		panic("path too short, refusing rm -rf")
	}
	if strings.Count(p, "/") < 3 {
		panic("path too shallow, refusing rm -rf")
	}
	return os.RemoveAll(p)
}

func assertGZFileValidTracks(t *testing.T, path string) {
	stat, err := os.Stat(path)
	if err != nil {
		t.Error(err)
		return
	}
	if stat.Size() == 0 {
		t.Error("file is empty")
		return
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
		t.Error("no tracks")
		return
	}
	for _, tr := range out {
		if !tr.IsValid() {
			t.Error("invalid track", tr)
		}
	}
	err = <-errs
	if err != nil {
		t.Error(err)
	}
}
