package flat

import (
	"compress/gzip"
	"github.com/rotblauer/catd/conceptual"
	"os"
	"path/filepath"
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
	f.path = filepath.Join(f.path, CatsDir, catID.String())
	return f
}

// Exists returns true if the directory exists.
func (f *Flat) Exists() bool {
	_, err := os.Stat(f.path)
	return err == nil
}

func (f *Flat) Ensure() error {
	return os.MkdirAll(f.path, 0770)
}

func (f *Flat) Path() string {
	return f.path
}

func (f *Flat) NamedGZ(name string) (*GZFile, error) {
	return NewFlatGZ(filepath.Join(f.path, name))
}

func (f *Flat) TracksGZ() (*GZFile, error) {
	return NewFlatGZ(filepath.Join(f.path, TracksFileName))
}

func (f *Flat) SnapsGZ() (*GZFile, error) {
	return NewFlatGZ(filepath.Join(f.path, SnapsFileName))
}

func (f *Flat) LapsGZ() (*GZFile, error) {
	return NewFlatGZ(filepath.Join(f.path, LapsFileName))
}

func (f *Flat) NapsGZ() (*GZFile, error) {
	return NewFlatGZ(filepath.Join(f.path, NapsFileName))
}

type GZFile struct {
	f   *os.File
	gzw *gzip.Writer
}

func NewFlatGZ(path string) (*GZFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0770); err != nil {
		return nil, err
	}
	fi, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return nil, err
	}
	gzw, err := gzip.NewWriterLevel(fi, gzip.BestCompression)
	if err != nil {
		return nil, err
	}
	return &GZFile{f: fi, gzw: gzw}, nil
}

func (g *GZFile) Writer() *gzip.Writer {
	return g.gzw
}

func (g *GZFile) Close() error {
	if err := g.gzw.Close(); err != nil {
		return err
	}
	if err := g.f.Close(); err != nil {
		return err
	}
	return nil
}

func (g *GZFile) Path() string {
	return g.f.Name()
}

type TextFile struct {
	f *os.File
}

func NewFlatText(path string) (*TextFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0770); err != nil {
		return nil, err
	}
	fi, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		return nil, err
	}
	return &TextFile{f: fi}, nil
}

func (t *TextFile) Writer() *os.File {
	return t.f
}

func (t *TextFile) Close() error {
	return t.f.Close()
}

func (t *TextFile) Path() string {
	return t.f.Name()
}
