package testdata

import (
	"context"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/stream"
	"path/filepath"
	"runtime"
)

var GeoJSONTrack = `{"heading":-1,"speed":-1,"uuid":"5D37B5DA-6E0B-41FE-8A72-2BB681D661DA","version":"V.customizableCatTrackHat","long":-93.255317687988281,"time":"2024-11-14T22:51:36.252Z","elevation":324.51593017578125,"notes":"{\"floorsAscended\":26,\"customNote\":\"\",\"heartRateS\":\"79 count\\\/min\",\"currentTripStart\":\"2024-11-12T13:15:26.996Z\",\"floorsDescended\":30,\"averageActivePace\":0.39090444504871674,\"networkInfo\":\"{\\\"ssidData\\\":\\\"{length = 12, bytes = 0x42616e616e6120486f74656c}\\\",\\\"bssid\\\":\\\"6c:70:9f:de:59:89\\\",\\\"ssid\\\":\\\"Banana Hotel\\\"}\",\"numberOfSteps\":30088,\"visit\":\"{\\\"validVisit\\\":false}\",\"relativeAltitude\":88.192718505859375,\"currentCadence\":1.8234708309173584,\"heartRateRawS\":\"277ECF13-9C43-4DBA-ADD8-B39BECC4303C 79 count\\\/min 277ECF13-9C43-4DBA-ADD8-B39BECC4303C, (2), \\\"iPhone17,1\\\" (18.0.1) (2024-11-14 10:48:00 -0600 - 2024-11-14 10:48:00 -0600)\",\"batteryStatus\":\"{\\\"level\\\":0.64999997615814209,\\\"status\\\":\\\"unplugged\\\"}\",\"activity\":\"Stationary\",\"currentPace\":0.66939723491668701,\"imgb64\":\"\",\"pressure\":97.587936401367188,\"distance\":35989.653745806601}","lat":44.989009857177734,"pushToken":"b1874b7923da4dbded73e3097c0de4d154b462feacf5eee22b7e6fef2ecf38f3","accuracy":5.1245980262756348,"name":"Rye16"}
`

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
