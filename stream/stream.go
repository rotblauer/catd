package stream

import (
	"context"
	"encoding/json"
	"io"
	"slices"
	"sync"
)

// Slice, et al., taken originally from:
// https://betterprogramming.pub/writing-a-stream-api-in-go-afbc3c4350e2

// Slice is a non-blocking function that converts a slice into a channel of elements.
func Slice[T any](ctx context.Context, in []T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for _, element := range in {
			el := element
			select {
			case <-ctx.Done():
				return
			case out <- el:
			}
		}
	}()
	return out
}

// Filter is a non-blocking function that filters elements from the input channel.
func Filter[T any](ctx context.Context, predicate func(T) bool, in <-chan T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for element := range in {
			el := element
			if predicate(el) {
				select {
				case <-ctx.Done():
					return
				case out <- el:
				}
			}
		}
	}()
	return out
}

// Transform is a non-blocking function that applies a transformation function to each element in the input channel.
func Transform[I any, O any](ctx context.Context, transformer func(I) O, in <-chan I) <-chan O {
	out := make(chan O)
	go func() {
		defer close(out)
		for element := range in {
			el := element
			select {
			case <-ctx.Done():
				return
			case out <- transformer(el):
			}
		}
	}()
	return out
}

// Collect is a blocking function that collects all elements from the input channel in a slice.
func Collect[T any](ctx context.Context, in <-chan T) []T {
	out := make([]T, 0)
	for element := range in {
		el := element
		select {
		case <-ctx.Done():
			return out
		default:
			out = append(out, el)
		}
	}
	return out
}

// BatchSort is a batching function that can sort batches of elements
// before forwarding them on the channel. The 'sorter' function is optional.
func BatchSort[T any](ctx context.Context, batchSize int, sorter func(a, b T) int, in <-chan T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)

		var batch []T
		flush := func() {
			if sorter != nil {
				if !slices.IsSortedFunc(batch, sorter) {
					slices.SortFunc(batch, sorter)
				}
			}
			Sink(ctx, func(t T) {
				out <- t
			}, Slice(ctx, batch))
			batch = []T{}
		}
		defer flush()

		// Range, blocking until 'in' is closed.
		for element := range in {
			batch = append(batch, element)
			if len(batch) == batchSize {
				flush()
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
	return out
}

// Tee is a non-blocking function that sends copies of each element from the input channel
// to two output channels.
func Tee[T any](ctx context.Context, in <-chan T) (a, b chan T) {
	a, b = make(chan T), make(chan T)
	go func() {
		defer close(a)
		defer close(b)
		for element := range in {
			var el1, el2 = element, element
			var out1, out2 = a, b
			for i := 0; i < 2; i++ {
				select {
				case <-ctx.Done():
					return
				case out1 <- el1:
					out1 = nil
				case out2 <- el2:
					out2 = nil
				}
			}
		}
	}()
	return
}

// Batch is a non-blocking function that batches elements from the input channel.
func Batch[T any](ctx context.Context, predicate func(T) bool, posticate func([]T) bool, in <-chan T) <-chan []T {
	out := make(chan []T)
	go func() {
		defer close(out)

		var batch []T
		flush := func() {
			out <- batch
			batch = []T{}
		}
		defer flush()

		// Range, blocking until 'in' is closed.
		for element := range in {
			el := element
			if predicate != nil && predicate(el) {
				flush()
			}
			batch = append(batch, el)
			if posticate != nil && posticate(batch) {
				flush()
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
	return out
}

// Unbatch is a non-blocking function that sends slice elements from a slice channel to a single-element output channel.
// Slices in, items out.
func Unbatch[S []T, T any](ctx context.Context, in <-chan S) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for sl := range in {
			sl := sl
			for _, e := range sl {
				select {
				case <-ctx.Done():
					return
				case out <- e:
				}
			}
		}
	}()
	return out
}

// Sink is a blocking function that executes the sink function (if any) for each element in the input channel.
func Sink[T any](ctx context.Context, sink func(T), in <-chan T) {
	for element := range in {
		el := element
		if sink != nil {
			sink(el)
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// Merge is a non-blocking function that merges multiple input channels into a single output channel.
func Merge[T any](ctx context.Context, ins ...<-chan T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		wg := sync.WaitGroup{}
		wg.Add(len(ins))
		for _, inn := range ins {
			in := inn
			go func() {
				defer wg.Done()
				for element := range in {
					el := element
					select {
					case <-ctx.Done():
						return
					case out <- el:
					}
				}
			}()
		}
		wg.Wait()
	}()
	return out
}

// TeeMany is a non-blocking function that sends elements from the input channel to multiple output channels.
func TeeMany[T any](ctx context.Context, in <-chan T, outs ...chan T) {
	go func() {
		defer func() {
			for _, out := range outs {
				close(out)
			}
		}()
		for element := range in {
			el := element
			for _, out := range outs {
				o := out
				select {
				case <-ctx.Done():
					return
				case o <- el:
				}
			}
		}
	}()
}

func TeeFilter[T any](ctx context.Context, filter func(T) bool, in <-chan T) (hit, miss chan T) {
	hit = make(chan T)
	miss = make(chan T)
	go func() {
		defer close(hit)
		defer close(miss)
		for element := range in {
			el := element
			if filter(el) {
				select {
				case <-ctx.Done():
					return
				case hit <- el:
				}
			} else {
				select {
				case <-ctx.Done():
					return
				case miss <- el:
				}
			}
		}
	}()
	return
}

// TeeManyN is like TeeMany, but returns a slice of n output channels.
func TeeManyN[T any](ctx context.Context, in <-chan T, n int) []<-chan T {
	outs := make([]chan T, n)
	for i := range outs {
		outs[i] = make(chan T)
	}
	go func() {
		defer func() {
			for _, out := range outs {
				close(out)
			}
		}()
		for element := range in {
			element := element
			for _, out := range outs {
				out := out
				select {
				case <-ctx.Done():
					return
				case out <- element:
				}
			}
		}
	}()
	var out []<-chan T
	for _, ch := range outs {
		out = append(out, ch)
	}
	return out
}

func NDJSON[T any](ctx context.Context, in io.Reader) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		dec := json.NewDecoder(in)
		for {
			var element T
			if err := dec.Decode(&element); err != nil {
				if err == io.EOF {
					return
				}
				continue
				// return?
			}
			select {
			case <-ctx.Done():
				return
			case out <- element:
			}
		}
	}()
	return out
}
