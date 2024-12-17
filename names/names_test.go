package names

import "testing"

func TestAliasOrSanitizedName(t *testing.T) {
	troublesome := `QP1A_191005_007_A3_Pixel_XL`
	want := "ric"
	got := AliasOrName(troublesome)
	if want != got {
		t.Errorf("got %s, want %s", got, want)
	}
}
