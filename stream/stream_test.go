package stream

import (
	"context"
	"slices"
	"testing"
)

func divideByTwo(n int) int {
	return n / 2
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
	b := CatchSizeSorting(ctx, 2, nil, s)
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
	b := CatchSizeSorting(ctx, 2, reverse, s)
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
	b := CatchSizeSorting(ctx, 10, reverse, s)
	result := Collect(ctx, b)

	if !slices.Equal([]int{8, 6, 4, 2, 0}, result) {
		t.Errorf("Expected [8, 6, 4, 2, 0], got %v", result)
	}
}
