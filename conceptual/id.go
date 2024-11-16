package conceptual

type CatID string

func (c CatID) String() string {
	return string(c)
}

func (c CatID) Empty() bool {
	return c == ""
}
