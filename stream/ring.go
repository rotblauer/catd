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

func (rb *SortingRingBuffer[T]) Sort() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.count > 1 {
		wrote := (rb.write - 1 + rb.size) % rb.size
		//offset := rb.size - rb.count
		for ii := 0; ii < rb.count-1; ii++ {
			rb.iters++
			//back := (rb.write + offset + ii) % rb.size
			back := ((rb.write - 2 - ii) + rb.size) % rb.size
			// eg. rb.write = 3 (next write index), rb.size = 5, rb.count = 3, offset = 2 => 3 + 2 + 0 % 5 = 0
			// eg. rb.write = 4 (next write index), rb.size = 5, rb.count = 4, offset = 1 => 4 + 1 + 0 % 5 = 0
			// eg. rb.write = 5 (next write index), rb.size = 5, rb.count = 5, offset = 0 => 5 + 0 + 0 % 5 = 0
			if !rb.less(rb.buffer[back], rb.buffer[wrote]) {
				rb.buffer[back], rb.buffer[wrote] = rb.buffer[wrote], rb.buffer[back]
				break
			} else {
				//break
			}
		}
	}
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

/*
	2024/12/18 16:39:12 INFO SortRing sorting... size=9000 count=9000 wr=5997 pr=5996 wrote=5996
	2024/12/18 16:39:12 INFO SortRing sorted iters=8999
	2024/12/18 16:39:12 INFO SortRing sorting... size=9000 count=9000 wr=7689 pr=7688 wrote=7688
	2024/12/18 16:39:12 INFO SortRing sorted iters=8999
	2024/12/18 16:39:12 INFO SortRing sorting... size=9000 count=9000 wr=5998 pr=5997 wrote=5997
	2024/12/18 16:39:12 INFO SortRing sorted iters=8999
	2024/12/18 16:39:12 INFO SortRing sorting... size=9000 count=9000 wr=7709 pr=7708 wrote=7708
	2024/12/18 16:39:12 INFO SortRing sorted iters=8999
	2024/12/18 16:39:12 INFO SortRing sorted iters=8999
	2024/12/18 16:39:12 INFO SortRing sorting... size=9000 count=9000 wr=7714 pr=7713 wrote=7713
	2024/12/18 16:39:12 INFO SortRing sorting... size=9000 count=9000 wr=6149 pr=6148 wrote=6148
	2024/12/18 16:39:12 INFO SortRing sorted iters=8999
	2024/12/18 16:39:12 INFO SortRing sorting... size=9000 count=9000 wr=6154 pr=6153 wrote=6153
	2024/12/18 16:39:12 INFO SortRing sorted iters=8999
	^C2024/12/18 16:39:12 INFO SortRing sorting... size=9000 count=9000 wr=7719 pr=7718 wrote=7718

	This is running backwards.
*/
/*
	/*


		I guess you can acheive that by doing:

		   targetIndex = (arryLength + (index- x)% arryLength ) % arryLength

		where:

		    index: is the location from where you want to look back

		    x: is the number of items you want to look back

		    explanation:
		     in Modulo arithmetic adding arryLength any number of times to an index and doing a mod % arryLength will not change the position of the index within the array
		     -(index- x)% arryLength could result in a negative value
		     -this value would lie between -arryLength and +arryLength (non inclusive)
		     -now adding arryLength to the resulting value and taking the mod again we get a value between 0 and arryLength

		https://stackoverflow.com/a/66701348
*/
