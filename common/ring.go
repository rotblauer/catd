package common

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

// Head returns the first (first in) n elements in the buffer.
func (rb *RingBuffer[T]) Head(n int) []T {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if n > rb.count {
		n = rb.count
	}

	result := make([]T, 0, n)

	for i := 0; i < n; i++ {
		index := (rb.write + rb.size - rb.count + i) % rb.size
		result = append(result, rb.buffer[index])
	}

	return result
}

// Tail returns the last (last in) n elements in the buffer.
func (rb *RingBuffer[T]) Tail(n int) []T {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if n > rb.count {
		n = rb.count
	}
	start := rb.count - n

	result := make([]T, 0, n)

	for i := start; i < rb.count; i++ {
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
	less  func(T, T) bool
	les   func(a T, b T) int
	iters int
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
	rb.RingBuffer.Add(value)
	if rb.count > 1 {
		if !rb.less(rb.buffer[(rb.write+rb.size-2)%rb.size], rb.Last()) {
			rb.Sort()
		}
	}
}

func (rb *SortingRingBuffer[T]) sort() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.count < 2 {
		return
	}
	back := func(i int) int {
		return ((rb.write - 1 - i) + rb.size) % rb.size
	}
	i := 0
	rb.iters = 0
	for i < rb.count-1 {
		rb.iters++
		b1, b0 := back(i+1), back(i)
		sorted := rb.less(rb.buffer[b1], rb.buffer[b0])
		if sorted {
			break
		}
		i++
		rb.buffer[b1], rb.buffer[b0] = rb.buffer[b0], rb.buffer[b1]
	}
}

func (rb *SortingRingBuffer[T]) Sort() {
	rb.sort()
	// Giving up.
	//index := func(i int) int {
	//	// (rb.write + rb.size - rb.count + i) % rb.size
	//	return (rb.write + rb.size - rb.count + i) % rb.size
	//}
	//sort.SliceStable(rb.buffer[:rb.count], func(i, j int) bool {
	//	return rb.less(rb.buffer[index(i)], rb.buffer[index(j)])
	//})
	//buf := make([]T, rb.count)
	//for i := 0; i < rb.count; i++ {
	//	buf[i] = rb.buffer[i]
	//}
	//sort.SliceStable(buf, func(i, j int) bool {
	//	return rb.less(buf[index(i)], buf[index(i)])
	//})
	//for i := 0; i < rb.count; i++ {
	//	rb.buffer[i] = buf[i]
	//}
}

func (rb *SortingRingBuffer[T]) IsSorted() bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.count < 2 {
		return true
	}
	wrote := (rb.write - 1 + rb.size) % rb.size
	for ii := 0; ii < rb.count-1; ii++ {
		rb.iters++
		target := (rb.write + rb.size - rb.count + ii) % rb.size
		if !rb.less(rb.buffer[target], rb.buffer[wrote]) {
			return false
		}
	}
	return true
}

func (rb *SortingRingBuffer[T]) Get() []T {
	return rb.RingBuffer.Get()
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
