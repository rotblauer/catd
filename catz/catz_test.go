package catz

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestFileLocking shows that a syscall.EX tryLock on a file
// will NOT block once the file has been closed, I guess because of file descriptor change.
// This means that the tryLock is not on the file, but on the file descriptor,
// and that the tryLock is invalidated once the file is closed,
// and that syscall.LOCK_UN is unnecessary.
// There are no syscall.UN locks in this test.
func TestFileLocking(t *testing.T) {
	target := filepath.Join(os.TempDir(), "mytestfile.xyz")
	defer os.Remove(target)

	// Create some file and tryLock EX it.
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
		// But do no LOCK_UN the file. Just close it.
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
	// If this syscall tryLock is NOT held, the write will succeed and the test will fail.
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

func TestGZFileWriter_Write(t *testing.T) {
	target := filepath.Join(os.TempDir(), "mytestfile.xyz.gz")
	os.Truncate(target, 0)
	defer os.Remove(target)

	// Create some file and write to it, locking, twice, concurrently.
	w1, err := NewGZFileWriter(target, DefaultGZFileWriterConfig())
	if err != nil {
		t.Fatal(err)
	}

	w2, err := NewGZFileWriter(target, DefaultGZFileWriterConfig())
	if err != nil {
		t.Fatal(err)
	}

	wait := sync.WaitGroup{}
	writeFile := func(w *GZFileWriter, name string, delay time.Duration) {
		defer wait.Done()
		defer func() {
			err := w.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()
		for i := 0; i < 10; i++ {
			if _, err := w.Write([]byte(fmt.Sprintf("%s testing... %d\n", name, i))); err != nil {
				t.Fatal(err)
			}
			t.Logf("%s wrote %d", name, i)
			time.Sleep(delay)
		}
	}

	wait.Add(2)
	go writeFile(w1, "w1", 50*time.Millisecond)
	time.Sleep(10 * time.Millisecond) // wait for w1 to tryLock the file.
	writeFile(w2, "w2", 10*time.Millisecond)
	wait.Wait()

	// Read in two ways: with stdlib packages only, then with our own gz reader.

	// Stdlibs only.
	// (Will throw an error if compression is screwy.)
	f, err := os.Open(target)
	if err != nil {
		t.Fatal(err)
	}
	gr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	read, err := io.ReadAll(gr)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("read: %s", string(read))
	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = gr.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Our own gz reader.
	// (Will also throw an error if compression is screwy.)
	r, err := NewGZFileReader(target)
	if err != nil {
		t.Fatal(err)
	}

	scanner := bufio.NewScanner(r.Reader())
	first, last := "", ""
	for scanner.Scan() {
		if first == "" {
			first = scanner.Text()
		}
		last = scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if first != "w1 testing... 0" {
		t.Fatalf("unexpected first: %s", first)
	}
	if last != "w2 testing... 9" {
		t.Fatalf("unexpected last: %s", last)
	}
	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}
