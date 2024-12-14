package api

import (
	"context"
	"github.com/dustin/go-humanize"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/params"
	catS2 "github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ReproducePipelines causes the cat to regenerate ("re-produce") application (catd)-generated data.
// This is a destructive operation; it will delete indexes, laps, naps, and tiling source files.
// Note that it does not remove the cat's TILES, tiled will overwrite them.
// It reads cat/tracks.gz data and sends it through the producer pipelines.
//
// FIXME/TODO: On an {devop,edge} run, this takes 1m20s. cat.Populate takes 1m30s.
// FIXME: This assumes that the tiled/source is accessible and writable. No offshore tilers.
func (c *Cat) ReproducePipelines() error {
	// Dump all tracks from cat/tracks.gz to a transformer
	// for JSON decoding then on to the CatActPipeline.
	catTDataPath := c.State.Flat.Path()
	c.logger.Warn("Reproducing pipelines", "path", catTDataPath)
	matches, err := filepath.Glob(filepath.Join(catTDataPath, "tracks", "*.geojson.gz"))
	if err != nil {
		return err
	}

	readers := make([]io.Reader, 0, len(matches))
	for _, match := range matches {
		reader, err := c.State.Flat.NamedGZReader(match)
		if err != nil {
			c.logger.Error("Failed to create tracks reader", "error", err)
			return err
		}
		readers = append(readers, reader)
	}
	defer func() {
		for _, reader := range readers {
			r := reader.(*catz.GZFileReader)
			if err := r.Close(); err != nil {
				c.logger.Error("Failed to close tracks reader", "error", err)
			}
		}
	}()

	mr := io.MultiReader(readers...)

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
	if err := c.ProducerPipelines(ctx, stream.NDJSON[cattrack.CatTrack](ctx, mr)); err != nil {
		c.logger.Error("Failed to regenerate pipelines", "error", err)
		return err
	}
	c.State.Waiting.Wait()
	return nil
}
