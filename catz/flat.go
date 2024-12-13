package catz

import (
	"os"
	"path/filepath"
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

func (f *Flat) Joins(paths ...string) *Flat {
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

func (f *Flat) NewGZFileWriter(name string, config *GZFileWriterConfig) (*GZFileWriter, error) {
	if config == nil {
		config = DefaultGZFileWriterConfig()
	}
	return NewGZFileWriter(filepath.Join(f.path, name), config)
}

func (f *Flat) NamedGZReader(name string) (*GZFileReader, error) {
	return NewGZFileReader(filepath.Join(f.path, name))
}
