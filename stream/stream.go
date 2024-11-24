package stream

import (
	"context"
	"encoding/json"
	"io"
	"slices"
)

// Slice, et al., taken from:
// https://betterprogramming.pub/writing-a-stream-api-in-go-afbc3c4350e2

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

// CatchSizeSorting is a caching/batching function that can sort batches of elements
// before forwarding them on the channel. The 'sorter' function is optional.
func CatchSizeSorting[T any](ctx context.Context, batchSize int, sorter func(a, b T) int, in <-chan T) <-chan T {
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

func Tee[T any](ctx context.Context, in <-chan T) (a, b chan T) {
	//a, b = make(chan T), make(chan T)
	//go func() {
	//	defer close(a)
	//	defer close(b)
	//	feed := event.FeedOf[T]{}
	//	subA := feed.Subscribe(a)
	//	subB := feed.Subscribe(b)
	//	defer subA.Unsubscribe()
	//	defer subB.Unsubscribe()
	//	for element := range in {
	//		feed.Send(element)
	//	}
	//}()
	//return

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

func Merge[T any](ctx context.Context, ins ...<-chan T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for _, inn := range ins {
			in := inn
			go func() {
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
	}()
	return out
}

func Broadcast[T any](ctx context.Context, in <-chan T, n int) []<-chan T {
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

func BroadcastTo[T any](ctx context.Context, streams ...chan<- T) chan<- T {
	out := make(chan T)
	go func() {
		defer close(out)
		for element := range out {
			element := element
			for _, stream := range streams {
				stream := stream
				select {
				case <-ctx.Done():
					return
				case stream <- element:
				}
			}
		}
	}()
	return out
}
