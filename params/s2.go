package params

import "github.com/rotblauer/catd/s2"

var S2DefaultCellLevels = []s2.CellLevel{
	//s2.CellLevel5,  // Modest nation-state
	s2.CellLevel8,  // A day's ride (Twin Cities: ~4 cells)
	s2.CellLevel11, // TODO I'm new here.
	s2.CellLevel13, // About a kilometer
	s2.CellLevel16, // Throwing distance
	s2.CellLevel18, // TODO I'm new here.
	s2.CellLevel20, // TODO I'm new here.
	//s2.CellLevel23, // Human body
}
