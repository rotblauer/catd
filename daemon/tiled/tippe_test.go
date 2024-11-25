package tiled

import (
	"github.com/rotblauer/catd/params"
	"testing"
)

func TestTippeArgs(t *testing.T) {
	args := params.TippeLapsArgs.Copy()
	args.SetPair("-l", "my_layer")
	args.SetPair("-n", "my_tileset")
	args.SetPair("-o", "output.mbtiles")
	t.Log(args)
}
