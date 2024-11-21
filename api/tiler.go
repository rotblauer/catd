package api

import (
	"context"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/tiler"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"path/filepath"
	"time"
)

// TODO Need global tiling service.
/*
Debounce impossible. Eternally out of scope for short-lived Populating API cat.
A cat is basically a db transaction and its context, with room for user info,
permissions, and other metadata. It's a stateful, long-lived object.
But not long enough. It's not a service. It's not a daemon. It's not a server.
So it doesn't have a way of saying "Run tippecanoe 5 seconds after the last
populate event for some/any cat." Which we kind of want to do, since we can't expect
even the slower-than-importing normal cat pushes to be only every 120 seconds.
(Think about all those pushes after hunting camp.)
Tippecanoe can take a long time, especially with the maps we want to draw.
So we need, I think, a global tiling service. It can be a daemon. It is the daemon.
And we need to be able to call functions outside the cat hat.
This is what events are for.
*/

// TileEdgeLaps requires state and is non-blocking.
func (c *Cat) TileEdgeLaps(ctx context.Context, in <-chan *cattrack.CatLap) {
	c.getOrInitState()

	laps := stream.Collect(ctx, in)
	if len(laps) == 0 {
		return
	}

	c.logger.Info("TileEdgeLaps", "laps", len(laps))

	edgeGZ := "laps_edge.geojson.gz"
	sinkToCatJSONGZFile(ctx, c, edgeGZ, stream.Slice(ctx, laps))

	config := &params.TippeConfig{
		LayerName:     "laps_edge",
		TilesetName:   "laps_edge",
		OutputMBTiles: filepath.Join(c.State.Flat.Path(), "laps_edge.mbtiles"),
		Args:          params.DefaultTippeLapsArgs(),
	}

	start := time.Now()
	c.tippeGZ(edgeGZ, config)
	elapsed := time.Since(start)

	if elapsed > time.Minute {

	}
}

// tippeGZ tips a GZ file into tippe. Blocking.
// TODO? Expose an alternate function for tipping arbitrary reader, like HTTP request bodies.
func (c *Cat) tippeGZ(nameGZ string, config *params.TippeConfig) {
	r, w := io.Pipe()

	go func() {
		defer w.Close()

		readEdge, err := c.State.Flat.NamedGZReader(nameGZ)
		if err != nil {
			c.logger.Error("Failed to open laps edge file", "error", err)
			return
		}
		defer readEdge.Close()

		_, err = io.Copy(w, readEdge.Reader())
		if err != nil {
			c.logger.Error("Failed to copy laps edge file", "error", err)
		}
	}()

	config.Args.SetPair("-l", config.LayerName)
	config.Args.SetPair("-n", config.TilesetName)
	mbtiles, _ := filepath.Abs(config.OutputMBTiles)
	config.Args.SetPair("-o", mbtiles)

	c.State.Waiting.Add(1)
	defer c.State.Waiting.Done()
	err := tiler.RunTippe(r, config.Args)
	if err != nil {
		c.logger.Error("Failed to run tippe laps", "error", err)
	}
}
