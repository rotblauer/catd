package stream

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rotblauer/catd/common"
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"
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

//
//func BatchCh[T any](ctx context.Context, predicate func(T) bool, in <-chan T) chan<- chan T {
//	out := make(chan <-chan T)
//	go func() {
//		defer close(out)
//		var batch []T
//		flush := func() {
//			if len(batch) > 0 {
//				out <- Slice(ctx, batch)
//				batch = []T{}
//			}
//		}
//		defer flush()
//		for element := range in {
//			el := element
//			if predicate(el) {
//				flush()
//			}
//			batch = append(batch, el)
//			select {
//			case <-ctx.Done():
//				return
//			default:
//			}
//		}
//	}()
//	return out
//}

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

// BatchSort is a batching function that can sort batches of elements
// before forwarding them on the channel. The 'sorter' function is optional.
func BatchSort[T any](ctx context.Context, batchSize int, sorter func(a, b T) int, in <-chan T) <-chan T {
	out := make(chan T, batchSize)
	go func() {
		defer close(out)

		// Could declare size and make, but that's a lot of i's. Append is easy and safe.
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

// BatchSort is a batching function that can sort batches of elements
// before forwarding them on the channel. The 'sorter' function is optional.
func BatchSortBetterSorta[T any](ctx context.Context, batchSize int, sorter func(a, b T) int, in <-chan T) <-chan T {
	out := make(chan T, batchSize)
	go func() {
		defer close(out)

		// Could declare size and make, but that's a lot of i's. Append is easy and safe.
		var batch = make([]T, batchSize)
		sorted := true
		flush := func() {
			if !sorted && sorter != nil {
				slices.SortFunc(batch, sorter)
			}
			Sink(ctx, func(t T) {
				out <- t
			}, Slice(ctx, batch))
			sorted = true
			batch = make([]T, batchSize)
		}
		defer flush()

		// Range, blocking until 'in' is closed.
		n := 0
		for element := range in {
			batch[n%batchSize] = element
			n++
			if n > 1 {
				i := n % batchSize
				// a truth can become a lie, but a lie never the truth
				sorted = sorted || slices.IsSortedFunc([]T{batch[i-2], batch[i-1]}, sorter)
			}
			if n > batchSize {
				flush()
				n = 0
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

// BatchSortBetter is a batching function that can sort batches of elements
// before forwarding them on the channel. The 'sorter' function is optional.
func BatchSortBetter[T any](ctx context.Context, maxSize int, sorter func(a, b T) int, in <-chan T) <-chan T {
	out := make(chan T, maxSize)
	go func() {
		defer close(out)

		// Range, blocking until 'in' is closed.
		var batch = make([]T, maxSize)
		count := 0
		cursor := func(count int) int {
			return count % maxSize
		}

		flush := func() {
			for j := cursor(count); j < maxSize; j++ {
				out <- batch[j]
			}
			for j := 0; j < cursor(count); j++ {
				out <- batch[j]
			}
		}
		defer flush()

		for element := range in {
			next := cursor(count)
			count++
			if count >= maxSize {
				// about to overwrite the oldest element
				out <- batch[next]

				last := []T{batch[cursor(count-1)], batch[cursor(count)]}
				if sorter != nil && !slices.IsSortedFunc(last, sorter) {
					if count >= maxSize {
						head := batch[cursor(count):]
						tail := batch[:cursor(count)]
						slices.SortFunc(tail, sorter)
						slices.SortFunc(head, sorter)
					}
				}
			}
			batch[next] = element
		}
	}()
	return out
}

//// batch[0] will always be sent
//// but first sort (all) if any of this batch were unsorted
//// ... and batch[0] isn't batch[0], its the fifo element i from the ring buffer
//if sorter != nil {
//	if !slices.IsSortedFunc(batch, sorter) {
//		slices.SortFunc(batch, sorter)
//	}
//}

func SortRing1[T any](ctx context.Context, size int, sorter func(a, b T) int, in <-chan T) <-chan T {
	out := make(chan T, size)
	go func() {
		defer close(out)
		var ring = make([]T, size)
		var write int
		var count int
		defer func() {
			//slices.SortFunc(ring, sorter)
			for i := 0; i < count; i++ {
				idx := (write + size - count + i) % size
				select {
				case <-ctx.Done():
					return
				case out <- ring[idx]:
				}
			}
		}()
		for element := range in {
			ring[write] = element
			write = (write + 1) % size
			if count < size {
				count++
				continue
			}
			if write > 1 {
				if !slices.IsSortedFunc([]T{ring[write-1], ring[write]}, sorter) {
					slices.SortFunc(ring, sorter)
				}

			}
			select {
			case <-ctx.Done():
				return
			case out <- ring[write]:
			}
		}
	}()
	return out
}

//
//func SortRingMod[T any](ctx context.Context, sorter func(a, b T) int, size int, in <-chan T) <-chan T {
//	out := make(chan T)
//	go func() {
//		defer close(out)
//		var n atomic.Uint64
//		var ring = make([]T, size, size)
//		defer func() {
//			for _, r := range ring {
//				select {
//				case <-ctx.Done():
//					return
//				case out <- r:
//				}
//			}
//		}()
//		for element := range in {
//			nn := n.Load()
//			i := nn % uint64(size)
//			ring[int(i)] = element
//
//			if len(ring) > 2 {
//				slices.SortFunc(ring, sorter)
//			}
//			n.Add(1)
//			select {
//			case <-ctx.Done():
//				return
//			case out <- ring[int(i)]:
//				n.Add(1)
//			}
//		}
//	}()
//	return out
//}

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

func Blackhole[T any](in <-chan T) {
	go func() {
		for range in {
		}
	}()
}

// Merge is a non-blocking function that merges multiple input channels into a single output channel.
// It blocks until all input channels are closed.
func Merge[T any](ctx context.Context, ins ...chan T) <-chan T {
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
// It is responsible for closing the outputs.
func TeeMany[T any](ctx context.Context, in <-chan T, outs ...chan T) {
	go func() {
		defer func() {
			for _, out := range outs {
				close(out)
				out = nil
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

func MeterTicker[T any](ctx context.Context, slogger *slog.Logger, label string, tick time.Duration, in <-chan T) <-chan T {
	out := make(chan T)
	ticker := time.NewTicker(tick)
	tlogger := slogger.With("MeterTicker", label)
	go func() {
		defer close(out)
		var count int
		var start time.Time
		var first time.Time
		var last time.Time
		defer func() {
			ticker.Stop()
			stop := time.Now()
			rangeElapsed := last.Sub(first)
			tps := float64(0)
			if count > 0 {
				tps = float64(count) / rangeElapsed.Seconds()
			}
			tlogger.Info("Done",
				"line", count,
				"range.elapsed", rangeElapsed.Round(time.Second),
				"start.elapsed", stop.Sub(start).Round(time.Second),
				"t/s", common.DecimalToFixed(tps, 0))
		}()
		go func() {
			for range ticker.C {
				stop := time.Now()
				rangeElapsed := last.Sub(first)
				tps := float64(0)
				if count > 0 && rangeElapsed.Seconds() > 0 {
					tps = float64(count) / rangeElapsed.Seconds()
				}
				tlogger.Info("tick",
					"line", count,
					"range.elapsed", rangeElapsed.Round(time.Second),
					"start.elapsed", stop.Sub(start).Round(time.Second),
					"t/s", common.DecimalToFixed(tps, 0))
			}
		}()
		start = time.Now()
		for el := range in {
			if count == 0 {
				first = time.Now()
			}
			last = time.Now()
			select {
			case <-ctx.Done():
				return
			case out <- el:
				count++
			}
		}
	}()
	return out
}

func Buffered[T any](ctx context.Context, in <-chan T, size int) <-chan T {
	out := make(chan T, size)
	go func() {
		defer close(out)
		for element := range in {
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

func NDJSON[T any](ctx context.Context, in io.Reader) (<-chan T, chan error) {
	out := make(chan T)
	errs := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errs)
		dec := json.NewDecoder(in)
		for {
			var element T
			if err := dec.Decode(&element); err != nil {
				if errors.Is(err, io.EOF) {
					//log.Println(err)
					return
				}
				errs <- err
				return
			}
			select {
			case <-ctx.Done():
				return
			case out <- element:
			}
		}
	}()
	return out, errs
}
