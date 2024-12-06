package s2

import "github.com/rotblauer/catd/common"

// SlippyZoomLevels are pairs of cell:zoom limits for use with tippecanoe --minimum-zoom and --maximum-zoom.
// Levels are inclusive.
var SlippyZoomLevels = map[CellLevel][2]common.SlippyZoomLevelT{
	CellLevel8:  {3, 6},
	CellLevel9:  {5, 8},
	CellLevel11: {6, 9},
	CellLevel12: {8, 10},
	CellLevel13: {9, 11},
	CellLevel14: {10, 12},
	CellLevel16: {11, 12},
	CellLevel17: {12, 15},
	CellLevel18: {14, 16},
	CellLevel19: {15, 18},
	CellLevel20: {16, 19},
}
