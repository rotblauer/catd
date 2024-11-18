package cmd

import (
	"testing"
	"time"
)

func TestBufferedChannel(t *testing.T) {
	working := make(chan struct{}, 8)
	go func() {
		for range working {
			time.Sleep(1000 * time.Millisecond)
		}
	}()
	defer close(working)
	for i := 0; i < 64; i++ {
		working <- struct{}{}
		t.Log("Running", i)
		<-working
	}
}
