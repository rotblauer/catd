package flat

import (
	"compress/gzip"
	"github.com/rotblauer/catd/conceptual"
	"os"
	"path/filepath"
	"syscall"
)

const (
	CatsDir        = "cats"
	TracksFileName = "tracks.geojson.gz"
	SnapsFileName  = "snaps.geojson.gz"
	LapsFileName   = "laps.geojson.gz"
	NapsFileName   = "naps.geojson.gz"
)

type Flat struct {
	// path is the cat-subdirectory for flat file storage.
	// It includes the root directory.
	path string
}

func NewFlatWithRoot(root string) *Flat {
	root = filepath.Clean(root)
	// If root is not absolute, make it absolute.
	if !filepath.IsAbs(root) {
		root, _ = filepath.Abs(root)
	}
	return &Flat{path: root}
}

func (f *Flat) ForCat(catID conceptual.CatID) *Flat {
	return f.Joining(CatsDir, catID.String())
}

func (f *Flat) Joining(paths ...string) *Flat {
	f.path = filepath.Join(append([]string{f.path}, paths...)...)
	return f
}

// Exists returns true if the directory exists.
func (f *Flat) Exists() bool {
	_, err := os.Stat(f.path)
	return err == nil
}

func (f *Flat) MkdirAll() error {
	return os.MkdirAll(f.path, 0770)
}

func (f *Flat) Path() string {
	return f.path
}

func (f *Flat) NamedGZWriter(name string, config *GZFileWriterConfig) (*GZFileWriter, error) {
	if config == nil {
		config = DefaultGZFileWriterConfig()
	}
	return NewFlatGZWriter(filepath.Join(f.path, name), config)
}

func (f *Flat) NamedGZReader(name string) (*GZFileReader, error) {
	return NewFlatGZReader(filepath.Join(f.path, name))
}

type GZFileWriter struct {
	f      *os.File
	gzw    *gzip.Writer
	locked bool

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
		CompressionLevel: gzip.BestCompression,
		Flag:             os.O_WRONLY | os.O_APPEND | os.O_CREATE,
		FilePerm:         0660,
		DirPerm:          0770,
	}
}

func NewFlatGZWriter(path string, config *GZFileWriterConfig) (*GZFileWriter, error) {
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
	return &GZFileWriter{
		f:   fi,
		gzw: gzw,
	}, nil
}

// Writer returns a gzip writer for the file.
// While the writer is not closed, an exclusive lock is held on the file.
func (g *GZFileWriter) Writer() *gzip.Writer {
	if !g.locked && g.f != nil {
		if err := syscall.Flock(int(g.f.Fd()), syscall.LOCK_EX); err != nil {
			panic(err)
		}
		g.locked = true
	}
	return g.gzw
}

func (g *GZFileWriter) Close() error {
	if err := g.gzw.Flush(); err != nil {
		return err
	}
	if err := g.gzw.Close(); err != nil {
		return err
	}
	if err := g.f.Sync(); err != nil {
		return err
	}
	if err := syscall.Flock(int(g.f.Fd()), syscall.LOCK_UN); err != nil {
		panic(err)
	}
	if err := g.f.Close(); err != nil {
		return err
	}
	return nil
}

func (g *GZFileWriter) Path() string {
	return g.f.Name()
}

type GZFileReader struct {
	f      *os.File
	gzr    *gzip.Reader
	closed bool
}

func NewFlatGZReader(path string) (*GZFileReader, error) {
	// If file/path does not exist, return error.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, err
	} else if err != nil {
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

// Reader returns a gzip reader for the file.
// While the reader is not closed, a shared lock is held on the file.
func (g *GZFileReader) Reader() *gzip.Reader {
	if g.closed {
		panic("closed")
	}
	if err := syscall.Flock(int(g.f.Fd()), syscall.LOCK_SH); err != nil {
		panic(err)
	}
	return g.gzr
}

func (g *GZFileReader) Close() error {
	if g.closed {
		return nil
	}
	defer func() {
		g.closed = true
	}()
	if err := g.gzr.Close(); err != nil {
		return err
	}
	if err := syscall.Flock(int(g.f.Fd()), syscall.LOCK_UN); err != nil {
		panic(err)
	}
	if err := g.f.Close(); err != nil {
		return err
	}
	return nil
}
