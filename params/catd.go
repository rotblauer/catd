package params

import (
	"compress/gzip"
	"github.com/ethereum/go-ethereum/metrics"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

var MetricsEnabled = true

func init() {
	metrics.Enabled = MetricsEnabled
}

const (
	RgeoDBName  = "rgeo.db"
	S2DBName    = "s2.db"
	TiledDBName = "tile.db"

	CatsDir        = "cats"
	CatSnapsSubdir = "snaps"

	MasterTracksGZFileName = "master.geojson.gz"
	TracksGZFileName       = "tracks.geojson.gz"
	LastTracksGZFileName   = "last_tracks.geojson.gz"
	SnapsGZFileName        = "snaps.geojson.gz"
	LapsGZFileName         = "laps.geojson.gz"
	NapsGZFileName         = "naps.geojson.gz"
)

var DefaultDatadirRoot = func() string {
	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	return filepath.Join(home, ".catd")
}()

var CatStateDBName = "state.db"
var CatStateBucket = []byte("state")
var CatSnapBucket = []byte("snaps")

// DefaultBatchSize is the default batch size for cat push/populate-batches.
// It is used in several important places.
// Counterintuitively, maybe, bigger is not always better. But neither is smaller.
// TODO More work with batch vs. buffer sizes.
// Have a feeling buffers need to tbe small(er) and batches need to be big(ger).
// What are they anyways, really? Buffers are the channel size, batches are the
// number of tracks per batch... (so, what's a "batch"?)...
// TODO: Make this a flag lol

const BestNumberForEverything = 9_000

// DefaultBatchSize is now the default batch size primarily for doing db io.
var DefaultBatchSize = runtime.NumCPU() * 1_000

// DefaultSortSize is the default size for sorting tracks.
var DefaultSortSize = runtime.NumCPU() * 1_000

// DefaultChannelCap is the default channel capacity for channels.
var DefaultChannelCap = runtime.NumCPU() * 1_000

// DedupeCacheSize is the default size for the dedupe cache.
var DedupeCacheSize = int((1 * time.Hour).Seconds())

// Disused since gone gz.
//var RPCTrackBatchSize = 111_111 //  9_000 is about 8.3MB max. Give me 100MB max: 111_000

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
