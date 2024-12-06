package params

import "github.com/rotblauer/catd/s2"

var S2DefaultCellLevels = []s2.CellLevel{
	//s2.CellLevel5,  // Modest nation-state
	s2.CellLevel8,  // A day's ride (Twin Cities: ~4 cells)
	s2.CellLevel9,  // TODO
	s2.CellLevel11, // TODO
	s2.CellLevel12, // TODO
	s2.CellLevel13, // About a kilometer
	s2.CellLevel14, // TODO
	s2.CellLevel16, // Throwing distance
	s2.CellLevel17, // TODO
	s2.CellLevel18, // TODO
	s2.CellLevel19, // TODO
	s2.CellLevel20, // TODO
	//s2.CellLevel23, // Human body
}
