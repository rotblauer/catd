package s2

import "github.com/rotblauer/catd/common"

// SlippyZoomLevels are pairs of cell:zoom limits for use with tippecanoe --minimum-zoom and --maximum-zoom.
// Levels are inclusive.
var SlippyZoomLevels = map[CellLevel][2]common.SlippyZoomLevelT{
	CellLevel8:  {3, 5},
	CellLevel11: {6, 8},
	CellLevel13: {9, 10},
	CellLevel16: {11, 13},
	CellLevel18: {14, 15},
	CellLevel20: {16, 18},
}
