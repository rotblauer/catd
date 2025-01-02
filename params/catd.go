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
	CatStateDBName = "state.db"
	RgeoDBName     = "rgeo.db"
	S2DBName       = "s2.db"
	TiledDBName    = "tile.db"

	// CatsDir is the cats/ subdirectory name, nested directly under the datadir root.
	CatsDir = "cats"
	// CatTracksDir is the cats/<catID>/"tracks"/ subdirectory name, nested under the catID.
	// This is used only with YYYY-MM storage.
	CatTracksDir = "tracks"
	// CatSnapsDir is the cats/<catID>/"snaps"/ subdirectory name, nested under the catID.
	CatSnapsSubdir = "snaps"

	MasterGZFileName     = "master.json.gz"
	TracksGZFileName     = "tracks.geojson.gz"
	LastTracksGZFileName = "last_tracks.geojson.gz"
	SnapsGZFileName      = "snaps.geojson.gz"
	LapsGZFileName       = "laps.geojson.gz"
	NapsGZFileName       = "naps.geojson.gz"
)

var DefaultDatadirRoot = func() string {
	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	return filepath.Join(home, ".catd") // aka ~/tdata
}()

// Optimal is the number that works best for everything.
const Optimal = 9_000

// NearOptimal is nearly optimal.
var NearOptimal = runtime.NumCPU() * 1_000

// DefaultBatchSize is now the default batch size primarily for doing db io.
// Used by indexers s2 and rgeo for their persistent kv indices.
var DefaultBatchSize = Optimal

// DefaultSortSize is the default size for sorting tracks.
// Its used by the sorter in Populate.
var DefaultSortSize = Optimal

// DefaultChannelCap is the default channel capacity for channels.
// Used widely for channels caps.
var DefaultChannelCap = Optimal

// DedupeCacheSize is the default size for the dedupe cache.
var DedupeCacheSize = int((1 * time.Hour).Seconds())

// Disused since RPC doesn't push in batches anymore.
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

var INFLUXDB_URL = os.Getenv("CATD_INFLUXDB_URL")
var INFLUXDB_TOKEN = os.Getenv("CATD_INFLUXDB_TOKEN")
var INFLUXDB_ORG = os.Getenv("CATD_INFLUXDB_ORG")
var INFLUXDB_BUCKET = os.Getenv("CATD_INFLUXDB_BUCKET")
