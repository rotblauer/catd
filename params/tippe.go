package params

import "io"

var TippecanoeCommand = "/usr/local/bin/tippecanoe"

type CLIFlagsT []string

type TippeConfig struct {
	LayerName     string
	TilesetName   string
	InputGZ       io.Reader
	OutputMBTiles string
	Args          CLIFlagsT
}

func DefaultTippeLapsArgs() CLIFlagsT {
	return TippeLapsArgs.Copy()
}

func DefaultTippeNapsArgs() CLIFlagsT {
	return TippeNapsArgs.Copy()
}

var (
	TippeLapsArgs = CLIFlagsT{
		"--maximum-tile-bytes", "750000",
		"--drop-smallest-as-needed",
		"--minimum-zoom", "3",
		"--maximum-zoom", "18",
		"--include", "Name",
		"--include", "UUID",
		"--include", "Time",
		"--include", "StartTime",
		"--include", "UnixTime",
		"--include", "Activity",
		"--include", "PointCount",
		"--include", "Duration",
		"--include", "AverageAccuracy",
		"--include", "DistanceTraversed",
		"--include", "DistanceAbsolute",
		"--include", "AverageReportedSpeed",
		"--include", "AverageCalculatedSpeed",
		"--include", "ElevationGain",
		"--include", "ElevationLoss",
		"--include", "MotionStateReasonStart",
		"--include", "MotionStateReasonEnd",
		"--order-by", "UnixTime",
		"--single-precision",
		"--generate-ids",
		"--read-parallel",
		"--temporary-directory", "/tmp",
		"-l", "${LAYER_NAME}",
		"-n", "${TILESET_NAME}",
		"-o", "${OUTPUT_FILE}",
		"--force",
	}
	TippeNapsArgs = &CLIFlagsT{
		"--maximum-tile-bytes", "5000000",
		"--cluster-densest-as-needed",
		"--cluster-distance", "1",
		"--calculate-feature-density",
		"--drop-rate", "1",
		"--minimum-zoom", "3",
		"--maximum-zoom", "18",
		"--include", "Name",
		"--include", "Time",
		"--include", "StartTime",
		"--include", "UnixTime",
		"--include", "Activity",
		"--include", "Accuracy",
		"--include", "Speed",
		"--include", "Duration",
		"-EDuration:sum",
		"--include", "Count",
		"-ECount:sum",
		"--include", "P50Dist",
		"--include", "P99Dist",
		"--include", "Area",
		"--include", "IsTrip",
		"--include", "MotionStateReason",
		"--single-precision",
		"--generate-ids",
		"--temporary-directory", "/tmp",
		"--read-parallel",
		"-l", "${LAYER_NAME}",
		"-n", "${TILESET_NAME}",
		"-o", "${OUTPUT_FILE}",
		"--force",
	}
)

func (c CLIFlagsT) Add(flag ...string) CLIFlagsT {
	return append(c, flag...)
}

func (c CLIFlagsT) SetPair(key, value string) (next CLIFlagsT, ok bool) {
	for i, f := range c {
		if f == key {
			(c)[i+1] = value
			return c, true
		}
	}
	return c, false
}

func (c CLIFlagsT) Remove(key string, vN int) (next CLIFlagsT, ok bool) {
	for i, f := range c {
		if f == key {
			return append((c)[:i], (c)[i+1+vN:]...), true
		}
	}
	return c, false
}

func (c CLIFlagsT) Copy() CLIFlagsT {
	return append(CLIFlagsT{}, c...)
}

/*
Usage: tippecanoe [options] [file.json ...]
  Output tileset
         --output=output.mbtiles [--output-to-directory=...] [--force]
         [--allow-existing]
  Tileset description and attribution
         [--name=...] [--attribution=...] [--description=...]
  Input files and layer names
         [--layer=...] [--named-layer=...]
  Parallel processing of input
         [--read-parallel]
  Projection of input
         [--projection=...]
  Zoom levels
         [--maximum-zoom=...] [--minimum-zoom=...]
         [--smallest-maximum-zoom-guess=...]
         [--extend-zooms-if-still-dropping]
         [--extend-zooms-if-still-dropping-maximum=...] [--one-tile=...]
  Tile resolution
         [--full-detail=...] [--low-detail=...] [--minimum-detail=...]
         [--extra-detail=...]
  Filtering feature attributes
         [--exclude=...] [--include=...] [--exclude-all]
  Modifying feature attributes
         [--attribute-type=...] [--attribute-description=...]
         [--accumulate-attribute=...] [--empty-csv-columns-are-null]
         [--convert-stringified-ids-to-numbers]
         [--use-attribute-for-id=...] [--single-precision]
         [--set-attribute=...]
  Filtering features by attributes
         [--feature-filter-file=...] [--feature-filter=...]
         [--unidecode-data=...]
  Dropping a fixed fraction of features by zoom level
         [--drop-rate=...] [--retain-points-multiplier=...] [--base-zoom=...]
         [--drop-denser=...] [--limit-base-zoom-to-maximum-zoom]
         [--drop-lines] [--drop-polygons] [--cluster-distance=...]
         [--cluster-maxzoom=...] [--preserve-point-density-threshold=...]
  Dropping or merging a fraction of features to keep under tile size limits
         [--drop-densest-as-needed] [--drop-fraction-as-needed]
         [--drop-smallest-as-needed] [--coalesce-densest-as-needed]
         [--coalesce-fraction-as-needed]
         [--coalesce-smallest-as-needed] [--force-feature-limit]
         [--cluster-densest-as-needed]
  Dropping tightly overlapping features
         [--gamma=...] [--increase-gamma-as-needed]
  Line and polygon simplification
         [--simplification=...] [--no-line-simplification]
         [--simplify-only-low-zooms] [--simplification-at-maximum-zoom=...]
         [--no-tiny-polygon-reduction]
         [--no-tiny-polygon-reduction-at-maximum-zoom]
         [--tiny-polygon-size=...] [--no-simplification-of-shared-nodes]
         [--visvalingam]
  Attempts to improve shared polygon boundaries
         [--detect-shared-borders] [--grid-low-zooms]
  Controlling clipping to tile boundaries
         [--buffer=...] [--no-clipping] [--no-duplication]
  Reordering features within each tile
         [--preserve-input-order] [--reorder] [--coalesce]
         [--reverse] [--hilbert] [--order-by=...]
         [--order-descending-by=...] [--order-smallest-first]
         [--order-largest-first]
  Adding calculated attributes
         [--calculate-feature-density] [--generate-ids]
  Trying to correct bad source geometry
         [--detect-longitude-wraparound] [--use-source-polygon-winding]
         [--reverse-source-polygon-winding] [--clip-bounding-box=...]
         [--convert-polygons-to-label-points]
  Filtering tile contents
         [--prefilter=...] [--postfilter=...]
  Setting or disabling tile size limits
         [--maximum-tile-bytes=...] [--maximum-tile-features=...]
         [--limit-tile-feature-count=...]
         [--limit-tile-feature-count-at-maximum-zoom=...]
         [--no-feature-limit] [--no-tile-size-limit]
         [--no-tile-compression] [--no-tile-stats]
         [--tile-stats-attributes-limit=...]
         [--tile-stats-sample-values-limit=...] [--tile-stats-values-limit=...]
  Temporary storage
         [--temporary-directory=...]
  Progress indicator
         [--quiet] [--no-progress-indicator] [--progress-interval=...]
         [--json-progress] [--version]

*/
