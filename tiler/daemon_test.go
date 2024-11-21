package tiler

import (
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestFileLocking(t *testing.T) {
	target := filepath.Join(os.TempDir(), "mytestfile.xyz")
	defer os.Remove(target)

	// Create a file

	f, err := os.Create(target)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}

	ff, err := os.OpenFile(target, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}

	if err := syscall.Flock(int(ff.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}

	if err := json.NewEncoder(ff).Encode(map[string]string{"test": "test2"}); err != nil {
		t.Fatal(err)
	} else {
		t.Log("Wrote to file (2)")
	}

	if err := json.NewEncoder(f).Encode(map[string]string{"test": "test1"}); err != nil {
		t.Fatal(err)
	}
	t.Log("Wrote to file (1)")
}
