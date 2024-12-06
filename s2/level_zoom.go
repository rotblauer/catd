package s2

import "github.com/rotblauer/catd/common"

// SlippyCellZoomLevels are pairs of cell:zoom limits for use with tippecanoe --minimum-zoom and --maximum-zoom.
// Levels are inclusive.
var SlippyCellZoomLevels = map[CellLevel][2]common.SlippyZoomLevelT{
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
	CellLevel20: {16, 19},
}
