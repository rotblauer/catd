package clean

import "testing"

func TestBufferedSliceAppend(t *testing.T) {
	buffer := make([]int, 0, 10)
	for i := 0; i < 100; i++ {
		buffer = append(buffer, i)
	}
	t.Log(buffer)
}
