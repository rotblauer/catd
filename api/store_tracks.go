package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/rotblauer/catd/catz"
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
	//	NewGZFileWriter(params.MasterTracksGZFileName, nil)
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

	truncate := catz.DefaultGZFileWriterConfig()
	truncate.Flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	lastGZ, err := c.State.Flat.NewGZFileWriter(params.LastTracksGZFileName, truncate)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		errCh <- err
		return
	}
	defer func() {
		if err := lastGZ.Close(); err != nil {
			c.logger.Error("Failed to close last-tracks writer", "error", err)
		}
	}()

	tracksGZ, err := c.State.Flat.NewGZFileWriter(params.TracksGZFileName, nil)
	if err != nil {
		c.logger.Error("Failed to create track writer", "error", err)
		errCh <- err
		return
	}
	defer func() {
		if err := tracksGZ.Close(); err != nil {
			c.logger.Error("Failed to close tracks writer", "error", err)
		}
	}()

	writeAll := io.MultiWriter(
		/* gzftwMaster.Writer(), */
		lastGZ,
		tracksGZ,
	)
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

func (c *Cat) StoreTracksYYYYMM(ctx context.Context, in <-chan cattrack.CatTrack) (errCh chan error) {
	c.getOrInitState(false)

	c.logger.Info("Storing cat tracks gz yyyy-mm", "cat", c.CatID)

	errCh = make(chan error, 1)
	defer close(errCh)

	truncate := catz.DefaultGZFileWriterConfig()
	truncate.Flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	lastGZ, err := c.State.Flat.NewGZFileWriter(params.LastTracksGZFileName, truncate)
	if err != nil {
		c.logger.Error("Failed to create custom writer", "error", err)
		errCh <- err
		return
	}
	defer func() {
		if err := lastGZ.Close(); err != nil {
			c.logger.Error("Failed to close last-tracks writer", "error", err)
		}
	}()
	lastEnc := json.NewEncoder(lastGZ)

	var ymWriter *catz.GZFileWriter
	var ymEnc *json.Encoder

	count := metrics.NewCounter()
	meter := metrics.NewMeter()
	defer meter.Stop()

	// Blocking.
	lastYMPath := ""
	for ct := range in {
		ct := ct

		if err := lastEnc.Encode(ct); err != nil {
			c.logger.Error("Failed to write", "error", err)
			errCh <- err
			return
		}

		ymPath := fmt.Sprintf("tracks/%s.geojson.gz", ct.MustTime().Format("2006-01"))

		if lastYMPath != ymPath || ymWriter == nil {
			// close last writer
			if ymWriter != nil {
				if err := ymWriter.Close(); err != nil {
					c.logger.Error("Failed to close last-tracks writer", "error", err)
					errCh <- err
					return
				}
			}
			ymWriter, err = c.State.Flat.NewGZFileWriter(ymPath, nil)
			if err != nil {
				c.logger.Error("Failed to create track writer", "error", err)
				errCh <- err
				return
			}
			ymEnc = json.NewEncoder(ymWriter)
		}
		lastYMPath = ymPath

		// Failsafe for nil writer?
		if ymWriter == nil || ymEnc == nil {
			ymWriter, err = c.State.Flat.NewGZFileWriter(ymPath, nil)
			if err != nil {
				c.logger.Error("Failed to create track writer", "error", err)
				errCh <- err
				return
			}
			ymEnc = json.NewEncoder(ymWriter)
		}
		
		if err := ymEnc.Encode(ct); err != nil {
			c.logger.Error("Failed to write", "error", err)
			errCh <- err
			return
		}
		count.Inc(1)
		meter.Mark(1)
	}
	if ymWriter != nil {
		if err := ymWriter.Close(); err != nil {
			c.logger.Error("Failed to close last-tracks writer", "error", err)
			errCh <- err
			return
		}
	}

	countSnap := count.Snapshot()
	meterSnap := meter.Snapshot()
	c.logger.Info("Stored cat tracks gzs",
		"count", humanize.Comma(countSnap.Count()),
		"tps", humanize.CommafWithDigits(meterSnap.RateMean(), 0),
	)
	return
}
