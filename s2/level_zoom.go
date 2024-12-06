package s2

import "github.com/rotblauer/catd/common"

// SlippyCellZoomLevels are pairs of cell:zoom limits for use with tippecanoe --minimum-zoom and --maximum-zoom.
// Levels are inclusive.
var SlippyCellZoomLevels = map[CellLevel][2]common.SlippyZoomLevelT{
	CellLevel5:  {3, 5},
	CellLevel8:  {4, 6},
	CellLevel9:  {5, 8},
	CellLevel11: {6, 9},
	CellLevel12: {8, 10},
	CellLevel13: {9, 11},
	CellLevel14: {10, 13},
	CellLevel16: {11, 14},
	CellLevel17: {12, 15},
	CellLevel18: {14, 16},
	CellLevel19: {15, 18},
	CellLevel20: {16, 19},
}
