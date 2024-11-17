package stream

import (
	"context"
	"encoding/json"
	"io"
)

// Slice, et al., taken from:
// https://betterprogramming.pub/writing-a-stream-api-in-go-afbc3c4350e2

func Slice[T any](ctx context.Context, in []T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for _, element := range in {
			select {
			case <-ctx.Done():
				return
			case out <- element:
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
			if predicate(element) {
				select {
				case <-ctx.Done():
					return
				case out <- element:
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
			select {
			case <-ctx.Done():
				return
			case out <- transformer(element):
			}
		}
	}()
	return out
}

func Collect[T any](ctx context.Context, in <-chan T) []T {
	out := make([]T, 0)
	for element := range in {
		select {
		case <-ctx.Done():
			return out
		default:
			out = append(out, element)
		}
	}
	return out
}

func BatchSize[T any](ctx context.Context, in <-chan T, batchSize int) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)

		var batch []T
		flush := func() {
			Sink(ctx, Slice(ctx, batch), func(t T) {
				out <- t
			})
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

func Sink[T any](ctx context.Context, in <-chan T, sink func(T)) {
	for element := range in {
		select {
		case <-ctx.Done():
			return
		default:
			if sink != nil {
				sink(element)
			}
		}
	}
}

func Drain[T any](ctx context.Context, in <-chan T) {
	for range in {
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
