package api

import (
	"context"
	"github.com/dustin/go-humanize"
	"github.com/rotblauer/catd/params"
	catS2 "github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"os"
	"path/filepath"
	"strings"
)

// ReproducePipelines causes the cat to regenerate ("re-produce") application (catd)-generated data.
// This is a destructive operation; it will delete indexes, laps, naps, and tiling source files.
// It reads cat/tracks.gz data and sends it through the producer pipelines.
func (c *Cat) ReproducePipelines() error {
	// Dump all tracks from cat/tracks.gz to a transformer
	// for JSON decoding then on to the CatActPipeline.
	r := c.State.Flat.Path()
	c.logger.Warn("Reproducing pipelines", "path", r)
	tdata := filepath.Join(r, params.TracksGZFileName)
	if _, err := os.Stat(tdata); err != nil {
		c.logger.Error("Tracks file does not exist", "error", err)
		return err
	}
	reader, err := c.State.Flat.NamedGZReader(params.TracksGZFileName)
	if err != nil {
		c.logger.Error("Failed to create tracks reader", "error", err)
		return err
	}

	// carefully rm -rf tiled/source
	tiledSource := filepath.Join(c.tiledConf.RootDir, "source")
	fi, err := os.Stat(tiledSource)
	if err != nil {
		if os.IsNotExist(err) {
			c.logger.Warn("No tiled source dir to remove", "path", tiledSource)
		} else {
			c.logger.Error("Failed to stat tiled source dir", "error", err)
			return err
		}
	} else if fi.IsDir() && strings.Contains(tiledSource, "tiled/source") {
		if err := os.RemoveAll(tiledSource); err != nil {
			c.logger.Warn("Failed to remove tiled source dir", "error", err)
		} else {
			c.logger.Warn("Removed tiled source dir", "path", tiledSource)
		}
	}

	s2IndexDBPath := filepath.Join(c.State.Flat.Path(), catS2.DBName)
	laps := filepath.Join(c.State.Flat.Path(), params.LapsGZFileName)
	naps := filepath.Join(c.State.Flat.Path(), params.NapsGZFileName)
	remove := func(path string) {
		fi, err := os.Stat(path)
		if os.IsNotExist(err) {
			c.logger.Warn("No file to remove", "path", path)
			return
		} else if err != nil {
			c.logger.Error("Failed to stat file", "error", err)
		}
		if fi != nil {
			c.logger.Warn("Checked file", "path", path, "size", humanize.Bytes(uint64(fi.Size())))
		}
		if !os.IsNotExist(err) {
			if err := os.Remove(path); err != nil {
				c.logger.Error("Failed to remove file", "error", err)
				return
			}
			c.logger.Warn("Removed file", "path", path)
			return
		}
	}
	remove(s2IndexDBPath)
	remove(laps)
	remove(naps)

	c.SubscribeFancyLogs()

	ctx := context.Background()
	gzr := reader.Reader()
	defer gzr.Close()
	if err := c.ProducerPipelines(ctx, stream.NDJSON[cattrack.CatTrack](ctx, gzr)); err != nil {
		c.logger.Error("Failed to regenerate pipelines", "error", err)
		return err
	}
	c.State.Waiting.Wait()
	return nil
}
