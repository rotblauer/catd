package testdata

import (
	"context"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/stream"
	"os"
	"path/filepath"
	"runtime"
)

// basepath is the root directory of this package.
var basepath string

func init() {
	_, currentFile, _, _ := runtime.Caller(0)
	basepath = filepath.Dir(currentFile)
}

// Path returns the absolute path the given relative file or directory path,
// relative to this testdata/ directory in the user's GOPATH.
// If rel is already absolute, it is returned unmodified.
// Taken from https://github.com/grpc/grpc-go/blob/master/testdata/testdata.go.
func Path(rel string) string {
	if filepath.IsAbs(rel) {
		return rel
	}

	return filepath.Join(basepath, rel)
}

// Source_RYE202412
//   zcat testing/testdata/private/rye_2024-12.geojson.gz | wc -l
//   444358
var Source_RYE202412 = "./private/rye_2024-12.geojson.gz"
var Source_EDGE1000 = "./private/edge_1000.json.gz"
var Source_EDGE20241217 = "./private/edge_20241217.json.gz"
var Source_LastPush_ia_20241221 = "./private/last_push_ia_20241221.json"
var Source_LastPush_rye_20241221 = "./private/last_push_rye_20241221.json"

func ReadSourceJSONGZ[T any](ctx context.Context, path string) (<-chan T, chan error) {
	errs := make(chan error, 1)
	defer close(errs)

	gzr, err := catz.NewGZFileReader(path)
	if err != nil {
		errs <- err
		return nil, errs
	}
	itemsCh, errCh := stream.NDJSON[T](ctx, gzr)
	items := stream.Collect(ctx, itemsCh)
	err = <-errCh
	if err != nil {
		errs <- err
		return nil, errs
	}
	err = gzr.Close()
	if err != nil {
		errs <- err
	}

	return stream.Slice(ctx, items), errs
}

func ReadSourceJSONGZs[T any](ctx context.Context, paths ...string) (<-chan T, chan error) {
	out := make(chan T, 1)
	errs := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errs)
		for _, path := range paths {
			gzr, err := catz.NewGZFileReader(path)
			defer gzr.Close() // throwaway error
			if err != nil {
				errs <- err
				return
			}
			itemsCh, errCh := stream.NDJSON[T](ctx, gzr)
			for item := range itemsCh {
				out <- item
			}
			err = <-errCh
			if err != nil {
				errs <- err
				return
			}
			err = gzr.Close()
			if err != nil {
				errs <- err
				return
			}
		}
	}()
	return out, errs
}

func ReadSourceJSON[T any](ctx context.Context, path string) (<-chan T, chan error) {
	errs := make(chan error, 1)
	defer close(errs)
	f, err := os.Open(path)
	itemsCh, errCh := stream.NDJSON[T](ctx, f)
	items := stream.Collect(ctx, itemsCh)
	err = <-errCh
	if err != nil {
		errs <- err
		return nil, errs
	}
	err = f.Close()
	if err != nil {
		errs <- err
	}
	return stream.Slice(ctx, items), errs
}
