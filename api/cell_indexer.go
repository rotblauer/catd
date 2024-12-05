package api

import (
	"context"
	"fmt"
	"github.com/golang/geo/s2"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/params"
	catS2 "github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"math/rand"
	"time"
)

// S2IndexTracks indexes incoming CatTracks for one cat.
func (c *Cat) S2IndexTracks(ctx context.Context, in <-chan cattrack.CatTrack) {
	c.getOrInitState()

	c.State.Waiting.Add(1)
	defer c.State.Waiting.Done()

	c.logger.Info("S2 Indexing cat tracks")
	start := time.Now()
	defer func() {
		c.logger.Info("S2 Indexing complete", "elapsed", time.Since(start).Round(time.Second))
	}()

	cellIndexer, err := catS2.NewCellIndexer(c.CatID, params.DatadirRoot, params.S2DefaultCellLevels, params.DefaultBatchSize)
	if err != nil {
		c.logger.Error("Failed to initialize indexer", "error", err)
		return
	}
	defer func() {
		if err := cellIndexer.Close(); err != nil {
			c.logger.Error("Failed to close indexer", "error", err)
		}
	}()

	for _, level := range cellIndexer.Levels {
		// Running only on levels 13-16.
		// This is a good range for most use cases.
		// Lower levels (bigger polygons) start taking for-ev-er to tile with tippecanoe.
		// FIXME: These are better served as GeoJSON.
		// Tippecanoe sucks at large polygons.
		// Better to include Cell/BBox/Whatever else in properties
		// and have the client do the drawings.
		if level == catS2.CellLevel13 || level == catS2.CellLevel16 {

			// FIXME Shove me off to own function.
			// Beware routines. Must wait in.
			levelFeed, err := cellIndexer.FeedOfIndexedTracksForLevel(level)
			if err != nil {
				c.logger.Error("Failed to get S2 feed", "level", level, "error", err)
				return
			}

			uniqsLevelX := make(chan []cattrack.CatTrack)
			defer close(uniqsLevelX)
			sub := levelFeed.Subscribe(uniqsLevelX)
			defer sub.Unsubscribe()

			txed := stream.Transform(ctx, func(freshPowderTracks []cattrack.CatTrack) []cattrack.CatTrack {
				outs := make([]cattrack.CatTrack, 0, len(freshPowderTracks))
				for _, f := range freshPowderTracks {
					cp := f
					pt := cp.Point()
					leaf := s2.CellIDFromLatLng(s2.LatLngFromDegrees(pt.Lat(), pt.Lon()))
					leveledCellID := catS2.CellIDWithLevel(leaf, level)

					cell := s2.CellFromCellID(leveledCellID)

					vertices := []orb.Point{}
					for i := 0; i < 4; i++ {
						vpt := cell.Vertex(i)
						//pt := cell.Edge(i) // tippe halt catch fire
						ll := s2.LatLngFromPoint(vpt)
						vertices = append(vertices, orb.Point{ll.Lng.Degrees(), ll.Lat.Degrees()})
					}

					//cellUnion := cell.RectBound().CellUnionBound()
					//
					//vertices := []orb.Point{}
					//for i, c := range cellUnion {
					//	ccell := s2.CellFromCellID(c)
					//	v := ccell.Vertex(i)
					//
					//	ll := s2.LatLngFromPoint(v)
					//	vertices = append(vertices, orb.Point{ll.Lng.Degrees(), ll.Lat.Degrees()})
					//}

					//rect := cell.RectBound()
					//hi, lo := rect.Hi(), rect.Lo()
					//tl := orb.Point{lo.Lng.Degrees(), hi.Lat.Degrees()}
					//tr := orb.Point{hi.Lng.Degrees(), hi.Lat.Degrees()}
					//bl := orb.Point{lo.Lng.Degrees(), lo.Lat.Degrees()}
					//br := orb.Point{hi.Lng.Degrees(), lo.Lat.Degrees()}
					//vertices := []orb.Point{tl, tr, br, bl, tl}

					//rc := s2.NewRegionCoverer()
					//rc.MaxLevel = int(level)
					//rc.MinLevel = int(level)
					////rc.MaxCells = 8
					//
					//region := s2.Region(cell)
					////covering := rc.Covering(region)
					//
					//cellUnion := rc.FastCovering(region)
					//cellUnion.CellUnionBound()
					//cellUnion.RectBound()
					//for _, c := range cellUnion {
					//
					//}

					cp.Geometry = orb.Polygon{orb.Ring(vertices)}
					cp.ID = rand.Int63()
					outs = append(outs, cp)
				}
				return outs
			}, uniqsLevelX)

			sendBatchToCatRPCClient[cattrack.CatTrack](ctx, c, &tiled.PushFeaturesRequestArgs{
				SourceSchema: tiled.SourceSchema{
					CatID:      c.CatID,
					SourceName: "s2_cells",
					LayerName:  fmt.Sprintf("level-%02d-polygons", level),
				},
				TippeConfig: params.TippeConfigNameCells,
			}, stream.Unslice[[]cattrack.CatTrack, cattrack.CatTrack](ctx, txed))
		}
	}

	// Blocking.
	if err := cellIndexer.Index(ctx, in); err != nil {
		c.logger.Error("CellIndexer errored", "error", err)
	}
}
