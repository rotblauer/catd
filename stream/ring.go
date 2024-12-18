package stream

import (
	"sync"
)

// https://logdy.dev/blog/post/ring-buffer-in-golang
// https://www.sergetoro.com/golang-round-robin-queue-from-scratch/

// RingBuffer from https://medium.com/@nathanbcrocker/a-practical-guide-to-implementing-a-generic-ring-buffer-in-go-866d27ec1a05.
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
}

func NewSortingRingBuffer[T any](size int, less func(T, T) bool) *SortingRingBuffer[T] {
	return &SortingRingBuffer[T]{
		RingBuffer: NewRingBuffer[T](size),
		less:       less,
	}
}

func (rb *SortingRingBuffer[T]) Add(value T) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.buffer[rb.write] = value
	rb.write = (rb.write + 1) % rb.size

	if rb.count < rb.size {
		rb.count++
	}

	if rb.count > 1 {
		for i := 0; i < rb.count-1; i++ {
			index := (rb.write + rb.size - rb.count + i) % rb.size
			if !rb.less(rb.buffer[index], rb.buffer[(rb.write+rb.size-1)%rb.size]) {
				rb.buffer[index], rb.buffer[(rb.write+rb.size-1)%rb.size] = rb.buffer[(rb.write+rb.size-1)%rb.size], rb.buffer[index]
			}
		}
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
