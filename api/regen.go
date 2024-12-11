package api

import (
	"context"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"os"
	"path/filepath"
)

// ReproducePipelines causes the cat to regenerate ("re-produce") application (catd)-generated data.
// It assumes that source data has already been stored
// in its state, so it skips those initial steps.
// It manua manually deletes their output files,
// and sends them to the ProducerPipelines.
// New source data is regenearated for services like laps and naps.
func (c *Cat) ReproducePipelines() error {
	// Dump all tracks from cat/tracks.gz to a transformer
	// for JSON decoding then on to the CatActPipeline.
	r := c.State.Flat.Path()
	tdata := filepath.Join(r, params.TracksGZFileName)
	if _, err := os.Stat(tdata); err != nil {
		c.logger.Error("Tracks file does not exist", "error", err)
		return err
	}

	reader, err := c.State.Flat.NamedGZReader(params.TracksGZFileName)
	if err != nil {
		c.logger.Error("Failed to create custom reader", "error", err)
		return err
	}

	laps := filepath.Join(c.State.Flat.Path(), params.LapsGZFileName)
	lapsFi, err := os.Stat(laps)
	if err == nil && lapsFi.Size() > 0 {
		if err := os.Remove(laps); err != nil {
			c.logger.Error("Failed to remove laps file", "error", err)
			return err
		}
	}
	naps := filepath.Join(c.State.Flat.Path(), params.NapsGZFileName)
	napsFi, err := os.Stat(naps)
	if err == nil && napsFi.Size() > 0 {
		if err := os.Remove(naps); err != nil {
			c.logger.Error("Failed to remove naps file", "error", err)
			return err
		}
	}

	c.SubscribeFancyLogs()

	ctx := context.Background()
	gzr := reader.Reader()
	defer gzr.Close()
	if err := c.ProducerPipelines(ctx, stream.NDJSON[cattrack.CatTrack](ctx, gzr)); err != nil {
		c.logger.Error("Failed to regenerate pipelines", "error", err)
		return err
	}
	return nil
}
