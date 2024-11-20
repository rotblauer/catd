package state

import (
	"go.etcd.io/bbolt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBBoltOpenNX(t *testing.T) {
	target := filepath.Join(os.TempDir(), "bbolt-test.db")
	defer os.Remove(target)
	db, err := bbolt.Open(target, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		t.Log("Sleeping 1...")
		time.Sleep(time.Second)
		db.Close()
	}()
	db2, err := bbolt.Open(target, 0600, nil)
	if err == nil {
		t.Log("next db conn! open 2x is blocking, not erroring")
	} else {
		t.Errorf("next db conn: %v", err)
	}
	db2.Close()
}

func TestBBoltOpenNXReadOnly(t *testing.T) {
	target := filepath.Join(os.TempDir(), "bbolt-test.db")
	defer os.Remove(target)
	db, err := bbolt.Open(target, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		t.Log("Sleeping 1...")
		time.Sleep(time.Second)
		db.Close()
	}()
	db2, err := bbolt.Open(target, 0600, &bbolt.Options{ReadOnly: true})
	if err == nil {
		t.Log("next db conn! open 2x is blocking, not erroring")
	} else {
		t.Errorf("next db conn: %v", err)
	}
	db2.Close()
}

// TestBBoltOpenNXReadOnlyBoth shows that a bbolt db conn will
// block (blocking read-only conn tries too) if opening with writing.
// However, a db conn with only a read-only conn open will allow another read-only conn.
func TestBBoltOpenNXReadOnlyBoth(t *testing.T) {
	target := filepath.Join(os.TempDir(), "bbolt-test.db")
	defer os.Remove(target)

	// Init an existing db.
	db0, err := bbolt.Open(target, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	db0.Close()

	db, err := bbolt.Open(target, 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		t.Log("Sleeping 1...")
		time.Sleep(time.Second)
		db.Close()
	}()
	db2, err := bbolt.Open(target, 0600, &bbolt.Options{ReadOnly: true})
	if err == nil {
		t.Log("next db conn! open 2x is non-blocking, not erroring")
	} else {
		t.Errorf("next db conn: %v", err)
	}
	db2.Close()
}
