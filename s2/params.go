package s2

import (
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
)

const DBName = "s2.db"

var DefaultVisitThreshold = params.S2DefaultVisitThreshold

var DefaultIndexerT = &cattrack.StackerV1{
	VisitThreshold: DefaultVisitThreshold,
}

// CellLevelTilingMinimum is the minimum cell level for tiling, inclusive.
var CellLevelTilingMinimum = CellLevel6

// CellLevelTilingMaximum is the maximum cell level for tiling, inclusive.
var CellLevelTilingMaximum = CellLevel18

// DefaultCellLevels are the default cell levels for indexing.
// These levels are not all necessarily tiled.
// See CellLevelTilingMinimum, CellLevelTilingMaximum for tiling bounds.
var DefaultCellLevels = []CellLevel{
	// Not tiling, but still indexing
	CellLevel0,
	CellLevel3,
	CellLevel4,
	CellLevel5,

	// Tiling:
	CellLevel6,  // Modest nation-state
	CellLevel7,  //
	CellLevel8,  // A day's ride (Twin Cities: ~4 cells)
	CellLevel9,  //
	CellLevel10, //
	CellLevel11, //
	CellLevel12, //
	CellLevel13, // About a kilometer
	CellLevel14, //
	CellLevel15, //
	CellLevel16, // Throwing distance
	CellLevel17, //
	//CellLevel18, // Seeley Lake USPS Office
	//s2.CellLevel19, //
	//s2.CellLevel20, //
	//s2.CellLevel23, // Human body
}

// TilingDefaultCellZoomLevels are pairs of cell:zoom limits for use with tippecanoe --minimum-zoom and --maximum-zoom.
// Levels are inclusive.
// These are used as a lookup table for the zoom levels to use when tiling.
//
// http://s2geometry.io/resources/s2cell_statistics.html
// https://wiki.openstreetmap.org/wiki/Zoom_levels
var TilingDefaultCellZoomLevels = map[CellLevel][2]common.SlippyZoomLevelT{
	CellLevel5:  {2, 3},
	CellLevel6:  {3, 4}, // 6 is big at z=5
	CellLevel7:  {4, 5}, // ?
	CellLevel8:  {4, 6},
	CellLevel9:  {5, 7},
	CellLevel10: {6, 8},
	CellLevel11: {7, 9},
	CellLevel12: {8, 10},
	CellLevel13: {9, 11},
	CellLevel14: {10, 12},
	CellLevel15: {11, 13},
	CellLevel16: {12, 14},
	CellLevel17: {13, 15},
	CellLevel18: {14, 16}, // could go to 17, but 16 is fine for cat map
	CellLevel19: {15, 18},
	CellLevel20: {17, 19},
}
