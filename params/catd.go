package params

import (
	"compress/gzip"
	"github.com/ethereum/go-ethereum/metrics"
	"os"
	"path/filepath"
	"time"
)

func init() {
	metrics.Enabled = true
}

const (
	CatsDir        = "cats"
	CatSnapsSubdir = "snaps"

	MasterTracksGZFileName = "master.geojson.gz"
	TracksGZFileName       = "tracks.geojson.gz"
	TracksLastGZFileName   = "last_tracks.geojson.gz"
	SnapsGZFileName        = "snaps.geojson.gz"
	LapsGZFileName         = "laps.geojson.gz"
	NapsGZFileName         = "naps.geojson.gz"
)

var DatadirRoot = func() string {
	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	return filepath.Join(home, ".catd")
}()

var CatStateDBName = "state.db"
var CatStateBucket = []byte("state")
var CatSnapBucket = []byte("snaps")

var DefaultBatchSize = 10_000
var DefaultBufferSize = 100_000

var DefaultGZipCompressionLevel = gzip.BestCompression

// AWS_BUCKETNAME is the fallbak AWS_BUCKETNAME value for cat snaps
// for the purpose of running catd _without_ an S3 config.
// Example catsnap:
// {"id":0,"type":"Feature","bbox":[-114.0877518,46.9292804,-114.0877518,46.9292804],"geometry":{"type":"Point","coordinates":[-114.0877518,46.9292804]},"properties":{"AccelerometerX":null,"AccelerometerY":null,"AccelerometerZ":null,"Accuracy":3,"Activity":"Walking","ActivityConfidence":100,"AmbientTemp":null,"BatteryLevel":0.95,"BatteryStatus":"unplugged","CurrentTripStart":null,"Distance":0,"Elevation":965.6,"GyroscopeX":null,"GyroscopeY":null,"GyroscopeZ":null,"Heading":-1,"Lightmeter":null,"Name":"ranga-moto-act3","NumberOfSteps":97647,"Pressure":null,"Speed":0.08,"Time":"2024-11-18T17:54:27.293Z","UUID":"76170e959f967f40","UnixTime":1731952467,"UserAccelerometerX":null,"UserAccelerometerY":null,"UserAccelerometerZ":null,"Version":"gcps/v0.0.0+4","heading_accuracy":-1,"imgS3":"rotblauercatsnaps/ia_76170e959f967f40_1731952467","speed_accuracy":0.1,"vAccuracy":1}}
var AWS_BUCKETNAME = os.Getenv("AWS_BUCKETNAME")

var (
	CacheLastPushTTL  = 1 * 24 * time.Hour
	CacheLastKnownTTL = 7 * 24 * time.Hour
)
