package conceptual

type CatID string

func (c CatID) String() string {
	return string(c)
}

func (c CatID) IsEmpty() bool {
	return c == ""
}

func CatIDIn(id CatID, cats []CatID) bool {
	for _, c := range cats {
		if c == id {
			return true
		}
	}
	return false
}
