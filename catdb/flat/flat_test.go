package flat

import (
	cattesting "github.com/rotblauer/catd/testing"
	cattestingdata "github.com/rotblauer/catd/testing/testdata"
	"os"
	"testing"
)

func TestFlat(t *testing.T) {
	f := NewFlatWithRoot(cattesting.DefaultTestDir())
	tracks, err := f.Join(CatsDir, "kitty1").TracksGZWriter()
	if err != nil {
		t.Fatal(err)
	}
	defer tracks.Close()
	defer os.Remove(tracks.Path())
	if _, err := tracks.Writer().Write([]byte(cattestingdata.GeoJSONTrack)); err != nil {
		t.Fatal(err)
	}
}
