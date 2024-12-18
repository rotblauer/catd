package stream

import (
	"context"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/rotblauer/catd/params"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"
)

var localRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func divideByTwo(n int) int {
	return n / 2
}

func multiplyByTwo(n int) int {
	return n * 2
}

func isNonZero(n int) bool {
	return n != 0
}

func TestStream1(t *testing.T) {
	data := []int{0, 2, 4, 6, 8}
	ctx := context.Background()
	myStream := Slice(ctx, data)
	result := Collect(ctx,
		Transform(ctx, divideByTwo,
			Filter(ctx, isNonZero,
				myStream)))

	if !slices.Equal([]int{1, 2, 3, 4}, result) {
		t.Errorf("Expected [1, 2, 3, 4], got %v", result)
	}
}

func TestStream2(t *testing.T) {
	data := []int{0, 2, 4, 6, 8}
	ctx := context.Background()
	s := Slice(ctx, data)
	tf := Transform(ctx, divideByTwo, s)
	f := Filter(ctx, isNonZero, tf)
	result := Collect(ctx, f)

	if !slices.Equal([]int{1, 2, 3, 4}, result) {
		t.Errorf("Expected [1, 2, 3, 4], got %v", result)
	}
}

func TestCatchSize(t *testing.T) {
	data := []int{0, 2, 4, 6, 8}
	ctx := context.Background()
	s := Slice(ctx, data)
	b := BatchSort(ctx, 2, nil, s)
	result := Collect(ctx, b)

	if !slices.Equal([]int{0, 2, 4, 6, 8}, result) {
		t.Errorf("Expected [0, 2, 4, 6, 8], got %v", result)
	}
}

func TestCatchSize2(t *testing.T) {
	reverse := func(a, b int) int {
		return b - a
	}

	data := []int{0, 2, 4, 6, 8}
	ctx := context.Background()
	s := Slice(ctx, data)
	b := BatchSort(ctx, 2, reverse, s)
	result := Collect(ctx, b)

	if !slices.Equal([]int{2, 0, 6, 4, 8}, result) {
		t.Errorf("Expected [2, 0, 6, 4, 8], got %v", result)
	}
}

func TestCatchSize3(t *testing.T) {
	reverse := func(a, b int) int {
		return b - a
	}

	data := []int{0, 2, 4, 6, 8}
	ctx := context.Background()
	s := Slice(ctx, data)
	b := BatchSort(ctx, 10, reverse, s)
	result := Collect(ctx, b)

	if !slices.Equal([]int{8, 6, 4, 2, 0}, result) {
		t.Errorf("Expected [8, 6, 4, 2, 0], got %v", result)
	}
}

func TestTee(t *testing.T) {
	data := []int{0, 2, 4, 6, 8}
	ctx := context.Background()
	s := Slice(ctx, data)

	out1, out2 := Tee(ctx, s)

	t1 := Transform(ctx, divideByTwo, out1)
	t2 := Transform(ctx, func(i int) int {
		time.Sleep(100 * time.Millisecond)
		return multiplyByTwo(i)
	}, out2)

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		r1 := Collect(ctx, t1)
		if !slices.Equal([]int{0, 1, 2, 3, 4}, r1) {
			t.Errorf("Expected [0, 1, 2, 3, 4], got %v", r1)
		}
	}()
	go func() {
		defer wg.Done()
		r2 := Collect(ctx, t2)
		t.Log(r2)
		if !slices.Equal([]int{0, 4, 8, 12, 16}, r2) {
			t.Errorf("Expected [0, 4, 8, 12, 16], got %v", r2)
		}
	}()

	wg.Wait()
}

func TestMeter(t *testing.T) {
	old := params.MetricsEnabled
	params.MetricsEnabled = true
	defer func() {
		params.MetricsEnabled = old
	}()
	m := metrics.NewMeter()
	m.Mark(47)
	if v := m.Snapshot().Count(); v != 47 {
		t.Fatalf("have %d want %d", v, 47)
	}
}

func myOrdering(a, b int) int {
	return a - b
}

type BatchSorterInt func(ctx context.Context, size int, cmp func(a, b int) int, s <-chan int) <-chan int

func TestBatchSorting(t *testing.T) {
	t.Run("BatchSort", func(t *testing.T) {
		testBatchSort(t, BatchSort, myOrdering)
	})
	t.Run("BatchSortaBetter", func(t *testing.T) {
		testBatchSort(t, BatchSortaBetter, myOrdering)
	})
}

func testBatchSort(t *testing.T, mySort BatchSorterInt, comparator func(a, b int) int) {
	cases := []struct {
		name string
		fn   func(tt *testing.T)
	}{
		{
			name: "Does not unsort",
			fn: func(tt *testing.T) {
				data := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
				cp := make([]int, len(data))
				copy(cp, data)
				ctx := context.Background()
				s := Slice(ctx, data)
				b := mySort(ctx, 5, comparator, s)
				result := Collect(ctx, b)
				if !slices.Equal(cp, result) {
					tt.Errorf("Expected %v, got %v", cp, result)
				}
			},
		},
		{
			name: "Sorts in batches",
			fn: func(tt *testing.T) {
				data := []int{20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
				expected := []int{16, 17, 18, 19, 20, 11, 12, 13, 14, 15, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 0}
				ctx := context.Background()
				s := Slice(ctx, data)
				b := mySort(ctx, 5, comparator, s)
				result := Collect(ctx, b)
				if !slices.Equal(expected, result) {
					tt.Errorf("Expected %v, got %v", expected, result)
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, c.fn)
	}
}

func TestRingSort(t *testing.T) {
	t.Run("RingSort", func(t *testing.T) {
		testRingSorting(t, RingSort, myOrdering)
	})
}

func testRingSorting(t *testing.T, mySort BatchSorterInt, comparator func(a, b int) int) {
	cases := []struct {
		name     string
		data     []int
		expected []int
		size     int
	}{
		{
			name:     "Does not unsort",
			data:     []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			size:     5,
		},
		{
			name:     "Sorts below size",
			data:     []int{3, 2, 1},
			expected: []int{1, 2, 3},
			size:     5,
		},
		{
			name:     "Sorts completely at size",
			data:     []int{4, 3, 2, 1, 0},
			expected: []int{0, 1, 2, 3, 4},
			size:     5,
		},
		{
			name:     "Sorts completely at size actually almost random",
			data:     []int{4, 2, 0, 3, 1},
			expected: []int{0, 1, 2, 3, 4},
			size:     5,
		},
		{
			name:     "Sorts best effort beyond size",
			data:     []int{4, 2, 0, 3, 1},
			expected: []int{0, 2, 1, 3, 4},
			size:     3,
		},
		{
			name:     "Sorts slightly shuffled simulated cats",
			data:     []int{0, 1, 3, 2, 5, 4, 6, 8, 7, 9, 10, 12, 11, 14, 13, 16, 15, 18, 20, 17, 19},
			expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			size:     5,
		},
		{
			name:     "Sorts unintuitively but as expected",
			data:     []int{20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			expected: []int{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 17, 18, 19, 20},
			size:     5,
		},
		{
			name:     "Sorts large data",
			data:     genIntsShuffled(100_00),
			expected: genInts(100_00),
			size:     100_000,
		},
		{
			name:     "Sorts partially shuffled data",
			data:     append(genInts(5), append(genIntsShuffledOffset(5, 5), genIntsOffset(5, 10)...)...),
			expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
			size:     5,
		},
		// Fails probabilistically, because batch size too low. Just a demo.
		//{
		//	name:     "Fails to sort partially shuffled data with too small buffer",
		//	data:     append(genInts(5), append(genIntsShuffledOffset(5, 5), genIntsOffset(5, 10)...)...),
		//	expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
		//	size:     2,
		//},
	}
	for _, c := range cases {
		t.Run(c.name, func(tt *testing.T) {
			ctx := context.Background()
			s := Slice(ctx, c.data)
			b := mySort(ctx, c.size, comparator, s)
			result := Collect(ctx, b)
			if !slices.Equal(c.expected, result) {
				tt.Errorf("Expected/Got\n%v\n%v", c.expected, result)
			}
		})
	}
}

func genInts(n int) []int {
	data := make([]int, n)
	for i := 0; i < n; i++ {
		data[i] = i
	}
	return data
}

func genIntsOffset(n, offset int) []int {
	data := make([]int, n)
	for i := 0; i < n; i++ {
		data[i] = i + offset
	}
	return data
}

func shuffleInts(data []int) {
	r := localRand.Int()
	for i := len(data) - 1; i > 0; i-- {
		j := r % (i + 1)
		data[i], data[j] = data[j], data[i]
	}
}

func genIntsShuffled(n int) []int {
	data := genInts(n)
	shuffleInts(data)
	return data
}

func genIntsShuffledOffset(n, offset int) []int {
	data := genIntsOffset(n, offset)
	shuffleInts(data)
	return data
}

var benchmarkBatchSize = 1_00

func BenchmarkSorts(b *testing.B) {
	b.Run("BatchSort", func(bb *testing.B) {
		benchmarkSort(bb, BatchSort, benchmarkBatchSize)
	})
	b.Run("BatchSortaBetter", func(bb *testing.B) {
		benchmarkSort(bb, BatchSortaBetter, benchmarkBatchSize)
	})
	b.Run("RingSort", func(bb *testing.B) {
		benchmarkSort(bb, RingSort, benchmarkBatchSize)
	})
}

func benchmarkSort(bb *testing.B, sorter BatchSorterInt, size int) {
	bb.Run("Ordered", func(b *testing.B) {
		b.ReportAllocs()
		data := genInts(size)
		ctx := context.Background()
		s := Slice(ctx, data)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b := sorter(ctx, size, myOrdering, s)
			_ = Collect(ctx, b)
		}
	})
	bb.Run("Shuffled", func(b *testing.B) {
		b.ReportAllocs()
		data := genIntsShuffled(size)
		ctx := context.Background()
		s := Slice(ctx, data)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b := sorter(ctx, size, myOrdering, s)
			_ = Collect(ctx, b)
		}
	})
}
