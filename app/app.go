package app

import (
	"os"
	"path/filepath"
)

var DatadirRoot = func() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".catd")
}()
