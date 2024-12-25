package api

import (
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/params"
	"io"
	"path/filepath"
	"sync"
)

// Master stores the incoming CatTracks in their original form in a file <datadir>/master.json.gz.
// Each call to Master appends to the file.
// The 'body' value is written in its entirety in a single newline.
// Users of the master file should note that this results in lines which may be longer than bufio.MaxScanTokenSize,
// and should be prepared to handle this.
func Master(datadir string, body io.Reader) (written int64, err error) {
	target := filepath.Join(datadir, params.MasterGZFileName)
	wr, err := catz.NewGZFileWriter(target, catz.DefaultGZFileWriterConfig())
	if err != nil {
		return 0, err
	}
	once := sync.Once{}
	shut := func() error {
		once.Do(func() {
			wr.Write([]byte("\n"))
		})
		return wr.Close()
	}
	defer shut()
	written, err = io.Copy(wr, body)
	if err != nil {
		return
	}
	return written, shut()
}
