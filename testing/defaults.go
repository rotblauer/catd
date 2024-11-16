package testing

import (
	"os"
	"path/filepath"
)

const DefaultTestDirRoot = "catd-test"

func DefaultTestDir() string {
	return filepath.Join(os.TempDir(), DefaultTestDirRoot)
}
