package stream

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestRingBuffer_Scan(t *testing.T) {
	ringBuffer := NewRingBuffer[int](3)
	ringBuffer.Add(1)
	ringBuffer.Add(2)
	ringBuffer.Add(3)

	expected := []int{1, 2, 3}
	actual := make([]int, 3)
	i := 0
	ringBuffer.Scan(func(in int) bool {
		actual[i] = in
		i++
		return true
	})
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}

	ringBuffer.Add(4)
	expected = []int{2, 3, 4}
	actual = make([]int, 3)
	i = 0
	ringBuffer.Scan(func(in int) bool {
		actual[i] = in
		i++
		return true
	})
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}

func TestRingBuffer_Last(t *testing.T) {
	ringBuffer := NewRingBuffer[int](3)
	ringBuffer.Add(1)
	ringBuffer.Add(2)
	ringBuffer.Add(3)

	expected := 3
	actual := ringBuffer.Last()
	if actual != expected {
		t.Errorf("Expected %d, but got %d", expected, actual)
	}

	ringBuffer.Add(4)
	ringBuffer.Add(5)
	ringBuffer.Add(6)

	expected = 6
	actual = ringBuffer.Last()
	if actual != expected {
		t.Errorf("Expected %d, but got %d", expected, actual)
	}

	ringBuffer.Add(7)
	ringBuffer.Add(8)

	expected = 8
	actual = ringBuffer.Last()
	if actual != expected {
		t.Errorf("Expected %d, but got %d", expected, actual)
	}
}

func TestRingBuffer_First(t *testing.T) {
	ringBuffer := NewRingBuffer[int](3)
	ringBuffer.Add(1)
	ringBuffer.Add(2)
	ringBuffer.Add(3)

	expected := 1
	actual := ringBuffer.First()
	if actual != expected {
		t.Errorf("Expected %d, but got %d", expected, actual)
	}

	ringBuffer.Add(4)
	ringBuffer.Add(5)
	ringBuffer.Add(6)

	expected = 4
	actual = ringBuffer.First()
	if actual != expected {
		t.Errorf("Expected %d, but got %d", expected, actual)
	}

	ringBuffer.Add(7)
	ringBuffer.Add(8)

	expected = 6
	actual = ringBuffer.First()
	if actual != expected {
		t.Errorf("Expected %d, but got %d", expected, actual)
	}
}

func TestRingBuffer_AddAndGet(t *testing.T) {
	ringBuffer := NewRingBuffer[int](5)
	ringBuffer.Add(1)
	ringBuffer.Add(2)
	ringBuffer.Add(3)

	expected := []int{1, 2, 3}
	actual := ringBuffer.Get()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}

	ringBuffer.Add(4)
	ringBuffer.Add(5)
	ringBuffer.Add(6)

	expected = []int{2, 3, 4, 5, 6}
	actual = ringBuffer.Get()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}

	ringBuffer.Add(7)
	ringBuffer.Add(8)

	expected = []int{4, 5, 6, 7, 8}
	actual = ringBuffer.Get()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}

func TestRingBufferConcurrent(t *testing.T) {
	ringBuffer := NewRingBuffer[int](3)
	var wg sync.WaitGroup

	addValues := func(values []int) {
		for _, value := range values {
			ringBuffer.Add(value)
			// Simulate delay
			time.Sleep(10 * time.Millisecond)
		}
		wg.Done()
	}

	readValues := func() {
		prices := ringBuffer.Get()
		if len(prices) > 0 && len(prices) != ringBuffer.size {
			t.Errorf("Buffer length inconsistency: expected size %d but got %d", ringBuffer.size, len(prices))
		}
		wg.Done()
	}

	wg.Add(3)
	go addValues([]int{1, 2, 3})
	go addValues([]int{4, 5})
	go addValues([]int{6, 7, 8})

	wg.Add(2)
	go readValues()
	go readValues()

	wg.Wait()

	finalValues := ringBuffer.Get()

	for _, value := range finalValues {
		if value < 1 || value > 8 {
			t.Errorf("Unexpected value in buffer: %d", value)
		}
	}

	// Ensure the buffer size is consistent with expectations
	if len(finalValues) != ringBuffer.size {
		t.Errorf("Expected buffer size %d, but got %d", ringBuffer.size, len(finalValues))
	}
}

func TestSortingRingBuffer_Add(t *testing.T) {
	ringBuffer := NewSortingRingBuffer[int](5, func(a, b int) bool {
		return a < b
	})

	ringBuffer.Add(2)
	ringBuffer.Add(1)

	expected := []int{1, 2}
	actual := ringBuffer.Get()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}

	ringBuffer.Add(3)
	ringBuffer.Add(4)
	ringBuffer.Add(5)
	ringBuffer.Add(2)
	ringBuffer.Add(1)

	expected = []int{1, 2, 3, 4, 5}
	actual = ringBuffer.Get()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}

	ringBuffer.Add(7)
	ringBuffer.Add(6)
	ringBuffer.Add(8)

	expected = []int{4, 5, 6, 7, 8}
	actual = ringBuffer.Get()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}

func TestSortingRingBuffer_Add2(t *testing.T) {
	ringBuffer := NewSortingRingBuffer[int](5, func(a, b int) bool {
		return a < b
	})

	ringBuffer.Add(5)
	ringBuffer.Add(4)
	ringBuffer.Add(3)
	ringBuffer.Add(2)
	ringBuffer.Add(1)

	expected := []int{1, 2, 3, 4, 5}
	actual := ringBuffer.Get()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}

	ringBuffer.Add(7)
	ringBuffer.Add(6)
	ringBuffer.Add(8)

	expected = []int{4, 5, 6, 7, 8}
	actual = ringBuffer.Get()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}

	ringBuffer.Add(1)
	expected = []int{1, 5, 6, 7, 8}
	actual = ringBuffer.Get()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}

	ringBuffer.Add(2)
	expected = []int{2, 5, 6, 7, 8}
	actual = ringBuffer.Get()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}

	ringBuffer.Add(3)
	expected = []int{3, 5, 6, 7, 8}
	actual = ringBuffer.Get()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}

	ringBuffer.Add(5)
	expected = []int{5, 5, 6, 7, 8}
	actual = ringBuffer.Get()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}

func TestSortingRingBuffer_Get(t *testing.T) {
	ringBuffer := NewSortingRingBuffer[int](5, func(a, b int) bool {
		return a < b
	})

	ringBuffer.Add(3)
	ringBuffer.Add(1)
	ringBuffer.Add(5)
	ringBuffer.Add(4)
	ringBuffer.Add(2)

	expected := []int{1, 2, 3, 4, 5}
	actual := ringBuffer.Get()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}

	ringBuffer.Add(6)
	ringBuffer.Add(8)
	ringBuffer.Add(7)

	expected = []int{4, 5, 6, 7, 8}
	actual = ringBuffer.Get()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}

func TestSortingRingBuffer_First(t *testing.T) {
	ringBuffer := NewSortingRingBuffer[int](5, func(a, b int) bool {
		return a < b
	})

	ringBuffer.Add(3)
	ringBuffer.Add(1)
	ringBuffer.Add(5)
	ringBuffer.Add(4)
	ringBuffer.Add(2)

	expected := 1
	actual := ringBuffer.First()
	if actual != expected {
		t.Errorf("Expected %d, but got %d", expected, actual)
	}

	ringBuffer.Add(6)
	ringBuffer.Add(8)
	ringBuffer.Add(7)

	expected = 4
	actual = ringBuffer.First()
	if actual != expected {
		t.Errorf("Expected %d, but got %d", expected, actual)
	}
}

func TestSortingRingBuffer_Last(t *testing.T) {
	ringBuffer := NewSortingRingBuffer[int](5, func(a, b int) bool {
		return a < b
	})

	ringBuffer.Add(3)
	ringBuffer.Add(1)
	ringBuffer.Add(5)
	ringBuffer.Add(4)
	ringBuffer.Add(2)

	expected := 5
	actual := ringBuffer.Last()
	if actual != expected {
		t.Errorf("Expected %d, but got %d", expected, actual)
	}

	ringBuffer.Add(6)
	ringBuffer.Add(8)
	ringBuffer.Add(7)

	expected = 8
	actual = ringBuffer.Last()
	if actual != expected {
		t.Errorf("Expected %d, but got %d", expected, actual)
	}
}

func TestSortingRingBuffer_Scan(t *testing.T) {
	ringBuffer := NewSortingRingBuffer[int](5, func(a, b int) bool {
		return a < b
	})

	ringBuffer.Add(3)
	ringBuffer.Add(1)
	ringBuffer.Add(5)
	ringBuffer.Add(4)
	ringBuffer.Add(2)

	expected := []int{1, 2, 3, 4, 5}
	actual := make([]int, 5)
	i := 0
	ringBuffer.Scan(func(in int) bool {
		actual[i] = in
		i++
		return true
	})
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}

	ringBuffer.Add(6)
	ringBuffer.Add(8)
	ringBuffer.Add(7)

	expected = []int{4, 5, 6, 7, 8}
	actual = make([]int, 5)
	i = 0
	ringBuffer.Scan(func(in int) bool {
		actual[i] = in
		i++
		return true
	})
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}