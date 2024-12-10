package api

import (
	"context"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"os"
)

// StoreTracks stores incoming CatTracks for one cat to disk.
func (c *Cat) StoreTracks(ctx context.Context, in <-chan cattrack.CatTrack) (errs <-chan error) {
	c.getOrInitState(false)

	errCh := make(chan error, 1)
	defer close(errCh)

	c.logger.Info("Storing cat tracks gz", "cat", c.CatID)

	// Sink ALL tracks (from ALL CATS) to master.geojson.gz.
	// Cat/thread safe because gz file locks.
	// Cat pushes will be stored in cat push/populate-batches.
	//c.logger.Info("Waiting on master locker...")
	//gzftwMaster, err := flat.NewFlatWithRoot(params.DatadirRoot).
	//	NamedGZWriter(params.MasterTracksGZFileName, nil)
	//if err != nil {
	//	c.logger.Error("Failed to create custom writer", "error", err)
	//	select {
	//	case errCh <- err:
	//	default:
	//	}
	//	return
	//}

	truncate := flat.DefaultGZFileWriterConfig()
	truncate.Flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	gzftwLast, err := c.State.Flat.NamedGZWriter(params.TracksLastGZFileName, truncate)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		select {
		case errCh <- err:
		default:
		}
	}

	cattracks, err := c.State.NamedGZWriter(params.TracksGZFileName)
	if err != nil {
		c.logger.Error("Failed to create track writer", "error", err)
		select {
		case errCh <- err:
		default:
		}
		return
	}

	// Tee for storage globally (master) and per cat.
	//master := make(chan cattrack.CatTrack)
	//myCat := make(chan cattrack.CatTrack)
	//pushLast := make(chan cattrack.CatTrack)
	//count := make(chan cattrack.CatTrack)
	//stream.TeeMany(ctx, in, master, myCat, pushLast, count)

	write, count := stream.Tee(ctx, in)
	writeAll := io.MultiWriter( /* gzftwMaster.Writer(), */ gzftwLast.Writer(), cattracks.Writer())

	sinkStreamToJSONWriter(ctx, c, writeAll, write)

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
