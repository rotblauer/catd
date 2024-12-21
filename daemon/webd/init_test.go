package webd

import (
	"github.com/rotblauer/catd/params"
	"os"
)

/// newTestWebDaemon creates a new WebDaemon for testing purposes.
// If datadir is empty, one will provided for you (
func newTestWebDaemon(datadir string) (daemon *WebDaemon, teardown func() error) {
	config := params.DefaultTestWebDaemonConfig()
	if datadir != "" {
		config.DataDir = datadir
	} else {
		tmpd, err := os.MkdirTemp(os.TempDir(), "catd-webd-test")
		if err != nil {
			panic(err)
		}
		config.DataDir = tmpd
	}
	daemon, err := NewWebDaemon(config)
	if err != nil {
		panic(err)
	}
	teardown = func() error {
		return os.RemoveAll(config.DataDir)
	}
	return daemon, teardown
}
