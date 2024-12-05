package api

import (
	"context"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"os"
)

// StoreTracks stores incoming CatTracks for one cat to disk.
func (c *Cat) StoreTracks(ctx context.Context, in <-chan cattrack.CatTrack) (errs <-chan error) {
	c.getOrInitState()

	errCh := make(chan error, 1)
	defer close(errCh)

	c.logger.Info("Storing cat tracks gz", "cat", c.CatID)

	// Tee for storage globally (master) and per cat.
	master := make(chan cattrack.CatTrack)
	myCat := make(chan cattrack.CatTrack)
	pushLast := make(chan cattrack.CatTrack)
	count := make(chan cattrack.CatTrack)
	stream.TeeMany(ctx, in, master, myCat, pushLast, count)

	c.State.Waiting.Add(1)
	go func() {
		defer c.State.Waiting.Done()
		// Sink ALL tracks (from ALL CATS) to master.geojson.gz.
		// Cat/thread safe because gz file locks.
		// Cat pushes will be stored in cat push/populate-batches.
		gzftwMaster, err := flat.NewFlatWithRoot(params.DatadirRoot).
			NamedGZWriter("master.geojson.gz", nil)
		if err != nil {
			c.logger.Error("Failed to create custom writer", "error", err)
			select {
			case errCh <- err:
			default:
			}
			return
		}
		sinkStreamToJSONGZWriter(ctx, c, gzftwMaster, master)
	}()

	c.State.Waiting.Add(1)
	go func() {
		defer c.State.Waiting.Done()
		truncate := flat.DefaultGZFileWriterConfig()
		truncate.Flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
		gzftwLast, err := c.State.Flat.NamedGZWriter("last_tracks.geojson.gz", truncate)
		if err != nil {
			c.logger.Error("Failed to create custom writer", "error", err)
			select {
			case errCh <- err:
			default:
			}
		}
		sinkStreamToJSONGZWriter(ctx, c, gzftwLast, pushLast)
	}()

	c.State.Waiting.Add(1)
	go func() {
		defer c.State.Waiting.Done()
		wr, err := c.State.NamedGZWriter(flat.TracksFileName)
		if err != nil {
			c.logger.Error("Failed to create track writer", "error", err)
			select {
			case errCh <- err:
			default:
			}
			return
		}
		sinkStreamToJSONGZWriter(ctx, c, wr, myCat)
	}()

	c.State.Waiting.Add(1)
	go func() {
		defer c.State.Waiting.Done()
		countN := int64(0)
		defer func() {
			c.logger.Info("Stored cat tracks gz", "count", countN)
		}()
		stream.Sink[cattrack.CatTrack](ctx, func(ct cattrack.CatTrack) {
			countN++
		}, count)
	}()
	return errCh
}
