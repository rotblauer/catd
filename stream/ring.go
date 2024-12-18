package stream

import (
	"sync"
)

// https://logdy.dev/blog/post/ring-buffer-in-golang
// https://www.sergetoro.com/golang-round-robin-queue-from-scratch/

// RingBuffer from https://medium.com/@nathanbcrocker/a-practical-guide-to-implementing-a-generic-ring-buffer-in-go-866d27ec1a05.
// Have added a few things.
// I'm to blame for SortingRingBuffer.
type RingBuffer[T any] struct {
	buffer []T
	size   int
	mu     sync.Mutex
	write  int
	count  int
}

// NewRingBuffer creates a new ring buffer with a fixed size.
func NewRingBuffer[T any](size int) *RingBuffer[T] {
	return &RingBuffer[T]{
		buffer: make([]T, size),
		size:   size,
	}
}

// Add inserts a new element into the buffer, overwriting the oldest if full.
func (rb *RingBuffer[T]) Add(value T) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.buffer[rb.write] = value
	rb.write = (rb.write + 1) % rb.size

	if rb.count < rb.size {
		rb.count++
	}
}

// Get returns the contents of the buffer in FIFO order.
func (rb *RingBuffer[T]) Get() []T {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	result := make([]T, 0, rb.count)

	for i := 0; i < rb.count; i++ {
		index := (rb.write + rb.size - rb.count + i) % rb.size
		result = append(result, rb.buffer[index])
	}

	return result
}

// Len returns the current number of elements in the buffer.
func (rb *RingBuffer[T]) Len() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.count
}

func (rb *RingBuffer[T]) Last() T {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.buffer[(rb.write+rb.size-1)%rb.size]
}

func (rb *RingBuffer[T]) First() T {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.buffer[(rb.write+rb.size-rb.count)%rb.size]
}

func (rb *RingBuffer[T]) Scan(fn func(T) bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for i := 0; i < rb.count; i++ {
		index := (rb.write + rb.size - rb.count + i) % rb.size
		if !fn(rb.buffer[index]) {
			break
		}
	}
}

type SortingRingBuffer[T any] struct {
	*RingBuffer[T]
	less func(T, T) bool
	les  func(a T, b T) int
}

func NewSortingRingBuffer[T any](size int, less func(T, T) bool) *SortingRingBuffer[T] {
	//genericLess := func(a T, b T) int
	// cmp func(a E, b E) int
	return &SortingRingBuffer[T]{
		RingBuffer: NewRingBuffer[T](size),
		less:       less,
		les: func(a T, b T) int {
			if less(a, b) {
				return -1
			}
			if less(b, a) {
				return 1
			}
			return 0
		},
	}
}

func (rb *SortingRingBuffer[T]) Add(value T) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	prev := (rb.write + rb.size - 1) % rb.size
	wrote := rb.write
	rb.buffer[rb.write] = value
	rb.write = (rb.write + 1) % rb.size

	if rb.count < rb.size {
		rb.count++
	}

	if rb.count > 1 {
		sorted := rb.less(rb.buffer[prev], rb.buffer[wrote])
		// I can walk forwards but not backwards.
		for i := 0; !sorted && i < rb.count-1; i++ {
			i1 := (rb.write + rb.size - rb.count + i) % rb.size
			i2 := (rb.write + rb.size - 1) % rb.size // prev
			if !rb.less(rb.buffer[i1], rb.buffer[i2]) {
				rb.buffer[i1], rb.buffer[i2] = rb.buffer[i2], rb.buffer[i1]
			}
		}
		//for i := wrote; !sorted && i >= 0; i-- {
		//	pr := (wrote + rb.size - 1) % rb.size // prev
		//	wr := (wrote + rb.size - rb.count + i) % rb.size
		//	if !rb.less(rb.buffer[pr], rb.buffer[wr]) {
		//		rb.buffer[pr], rb.buffer[wr] = rb.buffer[wr], rb.buffer[pr]
		//	}
		//}
		//for i := rb.count - 1; !sorted && i > 0; i-- {
		//	pr := (rb.write + rb.size - 1) % rb.size // prev
		//	wr := (rb.write + rb.size - rb.count + i) % rb.size
		//	if !rb.less(rb.buffer[pr], rb.buffer[wr]) {
		//		rb.buffer[pr], rb.buffer[wr] = rb.buffer[wr], rb.buffer[pr]
		//	}
		//}

		//if sorted {
		//sort.Slice(rb.buffer, func(i, j int) bool {
		//	ii := (rb.write + rb.size - rb.count + i) % rb.size
		//	jj := (rb.write + rb.size - 1) % rb.size
		//	return rb.less(rb.buffer[ii], rb.buffer[jj])
		//})
		//}

		//if !sorted {
		//	slices.SortStableFunc(rb.buffer, rb.les)
		//}
	}
}

func (rb *SortingRingBuffer[T]) Get() []T {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	result := make([]T, 0, rb.count)

	for i := 0; i < rb.count; i++ {
		index := (rb.write + rb.size - rb.count + i) % rb.size
		result = append(result, rb.buffer[index])
	}

	return result
}

func (rb *SortingRingBuffer[T]) Scan(fn func(T) bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for i := 0; i < rb.count; i++ {
		index := (rb.write + rb.size - rb.count + i) % rb.size
		if !fn(rb.buffer[index]) {
			break
		}
	}
}

func (rb *SortingRingBuffer[T]) Last() T {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.buffer[(rb.write+rb.size-1)%rb.size]
}

func (rb *SortingRingBuffer[T]) First() T {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.buffer[(rb.write+rb.size-rb.count)%rb.size]
}

func (rb *SortingRingBuffer[T]) Len() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.count
}
