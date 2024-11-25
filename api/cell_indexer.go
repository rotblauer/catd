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
)

// S2IndexTracks indexes incoming CatTracks for one cat.
func (c *Cat) S2IndexTracks(ctx context.Context, in <-chan cattrack.CatTrack) {
	c.getOrInitState()

	c.State.Waiting.Add(1)
	defer c.State.Waiting.Done()

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

	subscribeAndSendUniques := func(level catS2.CellLevel) {
		levelFeed, err := cellIndexer.FeedOfUniquesForLevel(level)
		if err != nil {
			c.logger.Error("Failed to get S2 feed", "level", level, "error", err)
			return
		}

		freshies := make(chan []*cattrack.CatTrack)
		defer close(freshies)
		sub := levelFeed.Subscribe(freshies)
		defer sub.Unsubscribe()

		sendBatchToCatRPCClient[[]*cattrack.CatTrack](ctx, c, &tiled.PushFeaturesRequestArgs{
			SourceSchema: tiled.SourceSchema{
				CatID:      c.CatID,
				SourceName: "s2_cells",
				LayerName:  fmt.Sprintf("level-%02d-polygons", level),
			},
			TippeConfig: params.TippeConfigNameTracks,
		}, stream.Transform(ctx, func(freshPowderTracks []*cattrack.CatTrack) []*cattrack.CatTrack {
			outs := make([]*cattrack.CatTrack, 0, len(freshPowderTracks))
			for _, f := range freshPowderTracks {
				cp := &cattrack.CatTrack{}
				*cp = *f
				pt := cp.Point()
				cell := s2.CellIDFromLatLng(s2.LatLngFromDegrees(pt.Lat(), pt.Lon()))
				leveledCell := catS2.CellIDWithLevel(cell, level)
				polygon := s2.PolygonFromCell(s2.CellFromCellID(leveledCell))
				rect := polygon.RectBound()
				hi, lo := rect.Hi(), rect.Lo()
				tl := orb.Point{lo.Lng.Degrees(), hi.Lat.Degrees()}
				tr := orb.Point{hi.Lng.Degrees(), hi.Lat.Degrees()}
				bl := orb.Point{lo.Lng.Degrees(), lo.Lat.Degrees()}
				br := orb.Point{hi.Lng.Degrees(), lo.Lat.Degrees()}
				cp.Geometry = orb.Polygon{orb.Ring{tl, tr, br, bl, tl}}
				//levelPt := leveledCell.Point()
				//latLng := s2.LatLngFromPoint(levelPt)
				//cp.Geometry = orb.Point{latLng.Lng.Degrees(), latLng.Lat.Degrees()}
				outs = append(outs, cp)
			}
			return outs
		}, freshies))
	}

	for _, level := range params.S2DefaultCellLevels {
		// Running only on levels 13-16.
		// This is a good range for most use cases.
		// Lower levels (bigger polygons) start taking for-ev-er to tile with tippecanoe.
		if level < catS2.CellLevel13 || level > catS2.CellLevel16 {
			continue
		}
		// These are better served as GeoJSON.
		// Tippecanoe sucks at large polygons.
		// Better to include Cell/BBox/Whatever else in properties
		// and have the client do the drawings.
		if false {
			subscribeAndSendUniques(level)
		}
	}

	// Blocking.
	if err := cellIndexer.Index(ctx, in); err != nil {
		c.logger.Error("CellIndexer errored", "error", err)
	}
}
