package params

import (
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/s2"
	"time"
)

var S2DefaultVisitThreshold = time.Hour

var S2DefaultIndexerT = &s2.TrackStackerV1{
	VisitThreshold: S2DefaultVisitThreshold,
}

// S2CellLevelTilingMinimum is the minimum cell level for tiling, inclusive.
var S2CellLevelTilingMinimum = s2.CellLevel6

// S2CellLevelTilingMaximum is the maximum cell level for tiling, inclusive.
var S2CellLevelTilingMaximum = s2.CellLevel18

// S2DefaultCellLevels are the default cell levels for indexing.
// These levels are not all necessarily tiled.
// See S2CellLevelTilingMinimum, S2CellLevelTilingMaximum for tiling bounds.
var S2DefaultCellLevels = []s2.CellLevel{
	// Not tiling, but still indexing
	s2.CellLevel0,
	s2.CellLevel3,
	s2.CellLevel4,
	s2.CellLevel5,

	// Tiling:
	s2.CellLevel6,  // Modest nation-state
	s2.CellLevel7,  //
	s2.CellLevel8,  // A day's ride (Twin Cities: ~4 cells)
	s2.CellLevel9,  //
	s2.CellLevel10, //
	s2.CellLevel11, //
	s2.CellLevel12, //
	s2.CellLevel13, // About a kilometer
	s2.CellLevel14, //
	s2.CellLevel15, //
	s2.CellLevel16, // Throwing distance
	s2.CellLevel17, //
	s2.CellLevel18, //
	//s2.CellLevel19, //
	//s2.CellLevel20, //
	//s2.CellLevel23, // Human body
}

// S2TilingDefaultCellZoomLevels are pairs of cell:zoom limits for use with tippecanoe --minimum-zoom and --maximum-zoom.
// Levels are inclusive.
// These are used as a lookup table for the zoom levels to use when tiling.
//
// http://s2geometry.io/resources/s2cell_statistics.html
// https://wiki.openstreetmap.org/wiki/Zoom_levels
var S2TilingDefaultCellZoomLevels = map[s2.CellLevel][2]common.SlippyZoomLevelT{
	s2.CellLevel5:  {2, 3},
	s2.CellLevel6:  {3, 4}, // 6 is big at z=5
	s2.CellLevel7:  {4, 5}, // ?
	s2.CellLevel8:  {4, 6},
	s2.CellLevel9:  {5, 7},
	s2.CellLevel10: {6, 8},
	s2.CellLevel11: {7, 9},
	s2.CellLevel12: {8, 10},
	s2.CellLevel13: {9, 11},
	s2.CellLevel14: {10, 12},
	s2.CellLevel15: {11, 13},
	s2.CellLevel16: {12, 14},
	s2.CellLevel17: {13, 15},
	s2.CellLevel18: {14, 16}, // could go to 17, but 16 is fine for cat map
	s2.CellLevel19: {15, 18},
	s2.CellLevel20: {17, 19},
}
