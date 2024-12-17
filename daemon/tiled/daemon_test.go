package tiled

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestFileLocking shows that a syscall.EX lock on a file
// will NOT block once the file has been closed, I guess because of file descriptor change.
// There are no syscall.UN locks in this test.
func TestFileLocking(t *testing.T) {
	target := filepath.Join(os.TempDir(), "mytestfile.xyz")
	defer os.Remove(target)

	// Create some file and lock EX it.
	f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}

	// Then _slowly_ write to the file.
	wait := sync.WaitGroup{}
	wait.Add(1)
	go func(f io.WriteCloser) {
		defer wait.Done()
		defer f.Close()
		time.Sleep(1 * time.Second)
		if err := json.NewEncoder(f).Encode("test1"); err != nil {
			t.Fatal(err)
		}
	}(f)

	// Open it again in another instance, locking too.
	ff, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	// If this syscall is NOT made, the write will succeed and the test will fail.
	if err := syscall.Flock(int(ff.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(ff).Encode("test2"); err != nil {
		t.Fatal(err)
	}
	ff.Close()

	wait.Wait()

	read, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(read) != "\"test1\"\n\"test2\"\n" {
		t.Fatalf("unexpected file content:\n%s", read)
	}
}
