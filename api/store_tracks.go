package api

import (
	"context"
	"encoding/json"
	"github.com/dustin/go-humanize"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"os"
)

// StoreTracks stores incoming CatTracks for one cat to disk.
func (c *Cat) StoreTracks(ctx context.Context, in <-chan cattrack.CatTrack) (errCh chan error) {
	c.getOrInitState(false)

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

	errCh = make(chan error, 1)
	defer close(errCh)

	truncate := flat.DefaultGZFileWriterConfig()
	truncate.Flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	gzftwLast, err := c.State.Flat.NamedGZWriter(params.TracksLastGZFileName, truncate)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		errCh <- err
		return
	}
	rLast := gzftwLast.Writer()
	defer rLast.Close()

	cattracks, err := c.State.NamedGZWriter(params.TracksGZFileName)
	if err != nil {
		c.logger.Error("Failed to create track writer", "error", err)
		errCh <- err
		return
	}
	rTracks := cattracks.Writer()
	defer rTracks.Close()

	writeAll := io.MultiWriter( /* gzftwMaster.Writer(), */ rTracks, rLast)
	enc := json.NewEncoder(writeAll)

	count := metrics.NewCounter()
	meter := metrics.NewMeter()
	defer meter.Stop()

	// Blocking.
	for ct := range in {
		if err := enc.Encode(ct); err != nil {
			c.logger.Error("Failed to write", "error", err)
			errCh <- err
			return
		}
		count.Inc(1)
		meter.Mark(1)
	}

	countSnap := count.Snapshot()
	meterSnap := meter.Snapshot()
	c.logger.Info("Stored cat tracks gzs",
		"count", humanize.Comma(countSnap.Count()),
		"tps", humanize.CommafWithDigits(meterSnap.RateMean(), 0),
	)
	return
}
