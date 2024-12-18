package stream

import (
	"context"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/rotblauer/catd/params"
	"slices"
	"sync"
	"testing"
	"time"
)

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

type BatchSorterInt func(ctx context.Context, size int, cmp func(a, b int) int, s <-chan int) <-chan int

func TestBatchSorting(t *testing.T) {
	t.Run("BatchSort", func(t *testing.T) {
		testBatchSort(t, BatchSort, reverse)
	})
	t.Run("SortRing1", func(t *testing.T) {
		testBatchSort(t, SortRing1, reverse)
	})
	t.Run("BatchSortBetterSorta", func(t *testing.T) {
		testBatchSort(t, BatchSortBetterSorta, reverse)
	})
	t.Run("BatchSortBetter", func(t *testing.T) {
		//t.Skip("failure to comprehend")
		testBatchSort(t, BatchSortBetter, reverse)
	})
}

func reverse(a, b int) int {
	return b - a
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
				b := mySort(ctx, 5, nil, s)
				result := Collect(ctx, b)
				if !slices.Equal(cp, result) {
					tt.Errorf("Expected %v, got %v", cp, result)
				}
			},
		},
		{
			name: "Sorts",
			fn: func(tt *testing.T) {
				data := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
				expected := []int{20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
				ctx := context.Background()
				s := Slice(ctx, data)
				b := mySort(ctx, 5, comparator, s)
				result := Collect(ctx, b)
				if !slices.Equal(expected, result) {
					tt.Errorf("Expected %v, got %v", expected, result)
				}
			},
		},
		//{
		//	name: "Sorts all",
		//	fn: func(tt *testing.T) {
		//		data := []int{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}
		//		ctx := context.Background()
		//		s := Slice(ctx, data)
		//		b := mySort(ctx, 10, comparator, s)
		//		result := Collect(ctx, b)
		//		if !slices.Equal([]int{8, 6, 4, 2, 0}, result) {
		//			tt.Errorf("Expected [8, 6, 4, 2, 0], got %v", result)
		//		}
		//	},
		//},
	}
	for _, c := range cases {
		t.Run(c.name, c.fn)
	}
}
