package catz

import (
	"compress/gzip"
	"github.com/rotblauer/catd/params"
	"os"
	"path/filepath"
)

type GZFileWriter struct {
	f      *os.File
	gzw    *gzip.Writer
	locked bool
	closed bool

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
		f:   fi,
		gzw: gzw,
	}
	return g, nil
}

func (g *GZFileWriter) Write(p []byte) (int, error) {
	f := g.f
	if !g.locked && f != nil {
		//if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		//	return 0, err
		//}
		g.locked = true
	}
	return g.gzw.Write(p)
}

func (g *GZFileWriter) Unlock() {
	//f := g.f
	//if f == nil {
	//	return
	//}
	//rc, err := f.SyscallConn()
	//_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	g.locked = false
}

func (g *GZFileWriter) Close() error {
	g.Unlock()
	//if err := g.gzw.Flush(); err != nil {
	//	return err
	//}
	if err := g.gzw.Close(); err != nil {
		return err
	}
	//if err := g.f.Sync(); err != nil {
	//	return err
	//}
	//if err := g.f.Close(); err != nil {
	//	return err
	//}
	return g.f.Close()
}

func (g *GZFileWriter) MustClose() error {
	g.Unlock()
	_ = g.gzw.Flush()
	_ = g.gzw.Close()
	_ = g.f.Sync()
	return g.f.Close()
}

func (g *GZFileWriter) MaybeClose() {
	g.Unlock()
	_ = g.gzw.Flush()
	_ = g.gzw.Close()
	_ = g.f.Sync()
	_ = g.f.Close()
}

func (g *GZFileWriter) Path() string {
	return g.f.Name()
}

type GZFileReader struct {
	f      *os.File
	gzr    *gzip.Reader
	closed bool
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
	return &GZFileReader{f: fi, gzr: gzr}, nil
}

// LockEX locks a file for exclusive access.
func (g *GZFileReader) LockEX() error {
	//if g.closed {
	//	panic("closed")
	//}
	return nil // syscall.Flock(int(g.f.Fd()), syscall.LOCK_EX)
}

// LockSH locks a file for shared access.
func (g *GZFileReader) LockSH() error {
	//if g.closed {
	//	panic("closed")
	//}
	return nil // syscall.Flock(int(g.f.Fd()), syscall.LOCK_SH)
}

// Unlock unlocks a file.
func (g *GZFileReader) Unlock() error {
	//if g.closed {
	//	panic("closed")
	//}
	return nil // syscall.Flock(int(g.f.Fd()), syscall.LOCK_UN)
}

func (g *GZFileReader) Read(p []byte) (int, error) {
	//if g.LockSH() != nil {
	//	return 0, nil
	//}
	return g.gzr.Read(p)
}

// Reader returns a gzip reader for the file.
// While the reader is not closed, a shared lock is held on the file.
func (g *GZFileReader) Reader() *gzip.Reader {
	//if g.closed {
	//	panic("closed")
	//}
	//if err := syscall.Flock(int(g.f.Fd()), syscall.LOCK_SH); err != nil {
	//	panic(err)
	//}
	return g.gzr
}

func (g *GZFileReader) Close() error {
	if g.closed {
		return nil
	}
	defer func() {
		g.closed = true
	}()
	if err := g.Unlock(); err != nil {
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
	defer func() {
		g.closed = true
	}()
	_ = g.Unlock()
	_ = g.gzr.Close()
	return g.f.Close()
}

func (g *GZFileReader) MaybeClose() {
	if g.closed {
		return
	}
	_ = g.Close()
}
