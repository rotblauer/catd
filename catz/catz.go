package catz

import (
	"bufio"
	"compress/gzip"
	"github.com/rotblauer/catd/params"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
)

type GZFileWriter struct {
	path   string
	f      *os.File
	gzw    *gzip.Writer
	locked atomic.Bool
	closed atomic.Bool

	GZFileWriterConfig
}

type GZFileWriterConfig struct {
	CompressionLevel int
	Flag             int
	FilePerm         os.FileMode
	DirPerm          os.FileMode
}

func DefaultGZFileWriterConfig() *GZFileWriterConfig {
	return &GZFileWriterConfig{
		CompressionLevel: params.DefaultGZipCompressionLevel,
		Flag:             os.O_WRONLY | os.O_APPEND | os.O_CREATE,
		FilePerm:         0660,
		DirPerm:          0770,
	}
}

func NewGZFileWriter(path string, config *GZFileWriterConfig) (*GZFileWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), config.DirPerm); err != nil {
		return nil, err
	}
	fi, err := os.OpenFile(path, config.Flag, config.FilePerm)
	if err != nil {
		return nil, err
	}
	gzw, err := gzip.NewWriterLevel(fi, config.CompressionLevel)
	if err != nil {
		return nil, err
	}
	g := &GZFileWriter{
		path: path,
		f:    fi,
		gzw:  gzw,
	}
	return g, nil
}

func (g *GZFileWriter) Write(p []byte) (int, error) {
	g.tryLock()
	return g.gzw.Write(p)
}

func (g *GZFileWriter) Writer() *gzip.Writer {
	return g.gzw
}

// lock locks the file for exclusive access.
// The lock will be invalidated if and when the file is closed.
func (g *GZFileWriter) tryLock() {
	if g.locked.Load() || g.closed.Load() || g.f == nil {
		return
	}
	fd := g.f.Fd()
	_ = syscall.Flock(int(fd), syscall.LOCK_EX)
	g.locked.Store(true)
}

// unlock unlocks the file. It is a no-op if the file is not locked.
// It is not required if the file is closed.
func (g *GZFileWriter) unlock() {
	if !g.locked.Load() || g.closed.Load() || g.f == nil {
		return
	}
	fd := g.f.Fd()
	_ = syscall.Flock(int(fd), syscall.LOCK_UN)
	g.locked.Store(false)
}

func (g *GZFileWriter) Close() error {
	defer g.closed.Store(true)
	defer g.unlock()
	err := g.gzw.Flush()
	if err != nil {
		return err
	}
	err = g.gzw.Close()
	if err != nil {
		return err
	}
	g.unlock()
	err = g.f.Close()
	if err != nil {
		return err
	}
	return nil
}

func (g *GZFileWriter) TryClose() error {
	defer g.closed.Store(true)
	_ = g.gzw.Flush()
	_ = g.gzw.Close()
	_ = g.f.Sync()
	g.unlock()
	return g.f.Close()
}

func (g *GZFileWriter) CloseNoReply() {
	defer g.closed.Store(true)
	_ = g.gzw.Flush()
	_ = g.gzw.Close()
	_ = g.f.Sync()
	g.unlock()
	_ = g.f.Close()
}

func (g *GZFileWriter) Path() string {
	return g.path
}

type GZFileReader struct {
	path   string
	closed atomic.Bool
	f      *os.File
	gzr    *gzip.Reader
}

func NewGZFileReader(path string) (*GZFileReader, error) {
	// If file/path does not exist, return error.
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	fi, err := os.OpenFile(path, os.O_RDONLY, 0660)
	if err != nil {
		return nil, err
	}
	gzr, err := gzip.NewReader(fi)
	if err != nil {
		return nil, err
	}
	return &GZFileReader{path: path, f: fi, gzr: gzr}, nil
}

func (g *GZFileReader) Path() string {
	return g.path
}

// lockEX locks a file for exclusive access.
func (g *GZFileReader) lockEX() error {
	//if g.closed {
	//	panic("closed")
	//}
	return nil // syscall.Flock(int(g.f.Fd()), syscall.LOCK_EX)
}

// lockSH locks a file for shared access.
func (g *GZFileReader) lockSH() error {
	//if g.closed {
	//	panic("closed")
	//}
	return nil // syscall.Flock(int(g.f.Fd()), syscall.LOCK_SH)
}

// unlock unlocks a file.
func (g *GZFileReader) unlock() error {
	//if g.closed {
	//	panic("closed")
	//}
	return nil // syscall.Flock(int(g.f.Fd()), syscall.LOCK_UN)
}

// Read satisfies the io.Reader interface.
func (g *GZFileReader) Read(p []byte) (int, error) {
	//if g.lockSH() != nil {
	//	return 0, nil
	//}
	return g.gzr.Read(p)
}

// Reader returns the gzip reader for the file.
func (g *GZFileReader) Reader() *gzip.Reader {
	return g.gzr
}

// Close satisfies the io.Closer interface.
// It closes the gzip reader and the file.
func (g *GZFileReader) Close() error {
	if g.closed.Load() {
		return nil
	}
	defer g.closed.Store(true)
	if err := g.unlock(); err != nil {
		return err
	}
	if err := g.gzr.Close(); err != nil {
		return err
	}
	if err := g.f.Close(); err != nil {
		return err
	}
	return nil
}

func (g *GZFileReader) MustClose() error {
	defer g.closed.Store(true)
	_ = g.unlock()
	_ = g.gzr.Close()
	return g.f.Close()
}

func (g *GZFileReader) MaybeClose() {
	if g.closed.Load() {
		return
	}
	_ = g.Close()
}

func (g *GZFileReader) LineCount() (int, error) {
	if err := g.lockSH(); err != nil {
		return 0, err
	}
	defer g.unlock()
	count := 0
	scanner := bufio.NewScanner(g.Reader())
	for scanner.Scan() {
		count++
	}
	return count, scanner.Err()
}
