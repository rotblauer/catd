package params

var TippecanoeCommand = "/usr/local/bin/tippecanoe"

type TippeConfigName string

const (
	TippeConfigNameTracks       TippeConfigName = "tracks"
	TippeConfigNameSnaps        TippeConfigName = "snaps"
	TippeConfigNameLaps         TippeConfigName = "laps"
	TippeConfigNameNaps         TippeConfigName = "naps"
	TippeConfigNameTripDetected TippeConfigName = "tripdetected"
	TippeConfigNameCells        TippeConfigName = "cells"
)

func LookupTippeConfig(name TippeConfigName) (config CLIFlagsT, ok bool) {
	switch name {
	case TippeConfigNameTracks:
		return DefaultTippeConfigs.Tracks(), true
	case TippeConfigNameSnaps:
		return DefaultTippeConfigs.Snaps(), true
	case TippeConfigNameLaps:
		return DefaultTippeConfigs.Laps(), true
	case TippeConfigNameNaps:
		return DefaultTippeConfigs.Naps(), true
	case TippeConfigNameTripDetected:
		return DefaultTippeConfigs.Tracks().Add("--include", "IsTrip"), true
	case TippeConfigNameCells:
		return TippeCellsArgs.Copy(), true
	}
	return nil, false
}

type CLIFlagsT []string

var DefaultTippeConfigs = &struct {
	Tracks func() CLIFlagsT
	Snaps  func() CLIFlagsT
	Laps   func() CLIFlagsT
	Naps   func() CLIFlagsT
}{
	Tracks: func() CLIFlagsT {
		return TippeTracksArgs.Copy()
	},
	Snaps: func() CLIFlagsT {
		return TippeSnapsArgs.Copy()
	},
	Laps: func() CLIFlagsT {
		return TippeLapsArgs.Copy()
	},
	Naps: func() CLIFlagsT {
		return TippeNapsArgs.Copy()
	},
}

var (
	commonArgs = CLIFlagsT{
		"--single-precision",
		"--generate-ids",
		"--read-parallel",
		"--json-progress",
		"--progress-interval", "5",
		"--temporary-directory", "/tmp",
		"--layer", "${LAYER_NAME}",
		"--name", "${TILESET_NAME}",
		"--output", "${OUTPUT_FILE}",
		"--force",
	}
	TippeLapsArgs = CLIFlagsT{
		"--maximum-tile-bytes", "750000",
		"--drop-smallest-as-needed",
		"--minimum-zoom", "3",
		"--maximum-zoom", "18",
		"--include", "Name",
		"--include", "UUID",
		"--include", "Activity",
		"--include", "RawPointCount",

		"--include", "Accuracy_Mean",

		"--include", "Time_Start_Unix",
		"--include", "Duration",

		"--include", "Speed_Reported_Mean",
		"--include", "Speed_Calculated_Mean",

		"--include", "Distance_Traversed",
		"--include", "Distance_Absolute",

		"--include", "Elevation_Mean",
		"--include", "Elevation_Gain",
		"--include", "Elevation_Loss",

		"--order-by", "Time_Start_Unix",
		"--single-precision",
		"--generate-ids",
		"--read-parallel",
		"--json-progress",
		"--progress-interval", "5",
		"--temporary-directory", "/tmp",
		"--layer", "${LAYER_NAME}",
		"--name", "${TILESET_NAME}",
		"--output", "${OUTPUT_FILE}",
		"--force",
	}
	TippeNapsArgs = CLIFlagsT{
		"--maximum-tile-bytes", "5000000",
		"--cluster-densest-as-needed",
		"--cluster-distance", "1",
		"--calculate-feature-density",
		"--drop-rate", "1",
		"--minimum-zoom", "3",
		"--maximum-zoom", "18",
		"--include", "Name",
		"--include", "UUID",
		"--include", "Time_Start_Unix",
		"--include", "Duration",
		"-EDuration:sum",
		"--include", "Accuracy_Mean",
		"--include", "Elevation_Mean",
		"--include", "Area",
		"--include", "RawPointCount",
		"-ERawPointCount:sum",
		"--single-precision",
		"--generate-ids",
		"--temporary-directory", "/tmp",
		"--read-parallel",
		"--json-progress",
		"--progress-interval", "5",
		"--layer", "${LAYER_NAME}",
		"--name", "${TILESET_NAME}",
		"--output", "${OUTPUT_FILE}",
		"--force",
	}
	// TippeTracksArgs taken from V1 CatTracks procedge, procmaster.
	TippeTracksArgs = CLIFlagsT{
		"--maximum-tile-bytes", "500000", // num bytes/tile,default: 500kb=500000
		"--cluster-densest-as-needed",
		"--cluster-distance=1",
		"--calculate-feature-density",
		"--include", "Alias",
		"--include", "UUID",
		"--include", "Name",
		"--include", "Activity",
		"--include", "Elevation",
		"--include", "Speed",
		"--include", "Accuracy",
		"--include", "Heading",
		"--include", "UnixTime",
		"-EUnixTime:max",
		"-EElevation:max",
		"-ESpeed:max", // mean",
		"-EAccuracy:mean",
		"--single-precision",
		"--drop-rate", "1", // == --drop-rate
		"--minimum-zoom", "3",
		"--maximum-zoom", "18",
		"--json-progress", "--progress-interval", "30",
		"--read-parallel",
		"--json-progress",
		"--progress-interval", "5",
		"--layer", "${LAYER_NAME}", // TODO: what's difference layer vs name?
		"--name", "${TILESET_NAME}",
		"--output", "${OUTPUT_FILE}",
		"--force",
	}
	TippeSnapsArgs = CLIFlagsT{
		"--maximum-tile-bytes", "330000", // num bytes/tile,default: 500kb=500000
		"--cluster-densest-as-needed",
		"--cluster-distance=1",
		"--calculate-feature-density",
		"--include", "Alias",
		"--include", "UUID",
		"--include", "Name",
		"--include", "Activity",
		"--include", "Elevation",
		"--include", "Speed",
		"--include", "Accuracy",
		"--include", "S3URL",
		// "--include", "Heading",
		"--include", "UnixTime",
		"-EUnixTime:max",
		"-EElevation:max",
		"-ESpeed:max", // mean",
		"-EAccuracy:mean",
		"--single-precision",
		"--drop-rate", "1", // == --drop-rate
		"--minimum-zoom", "3",
		"--maximum-zoom", "18",
		"--json-progress", "--progress-interval", "5",
		"--read-parallel",
		"--json-progress",
		"--progress-interval", "5",
		"--layer", "${LAYER_NAME}", // TODO: what's difference layer vs name?
		"--name", "${TILESET_NAME}",
		"--output", "${OUTPUT_FILE}",
		"--force",
	}
	// TippeCellsArgs are for S2 cell polygons.
	TippeCellsArgs = CLIFlagsT{
		"--maximum-tile-bytes", "500000", // num bytes/tile,default: 500kb=500000

		// -zg: Automatically choose a maxzoom that should be sufficient to clearly distinguish the features and the detail within each feature
		"--maximum-zoom", "g", // guess
		//"--minimum-zoom", "3",
		//"--maximum-zoom", "18",

		// --coalesce-densest-as-needed: If the tiles are too big at low or medium zoom levels,
		// merge as many features together as are necessary to allow tiles to be created with those features that are still distinguished
		"--coalesce-densest-as-needed",

		//--extend-zooms-if-still-dropping: If even the tiles at high zoom levels are too big,
		// keep adding zoom levels until one is reached that can represent all the features.
		"--extend-zooms-if-still-dropping",

		// Don't simplify away nodes at which LineStrings or Polygon rings converge, diverge, or cross.
		// (This will not be effective if you also use --coalesce.)
		// In between intersection nodes, LineString segments or polygon edges will be simplified identically in each feature if possible.
		// Use this instead of --detect-shared-borders.
		// https://felt.com/blog/tippecanoe-polygons-shard-gaps
		"--no-simplification-of-shared-nodes",

		// Multiply the tolerance for line and polygon simplification by _scale_ (the value).
		// The standard tolerance tries to keep the line or polygon within one tile unit of its proper location.
		// You can probably go up to about 10 without too much visible difference.
		"--simplification", "10",

		"--include", "Alias",
		"--include", "UUID",
		"--include", "Name",
		"--include", "Activity",
		"--include", "Elevation",
		"--include", "Speed",
		"--include", "Accuracy",
		// "--include", "Heading",
		"--include", "UnixTime",
		"--single-precision",
		//"--minimum-zoom", "3",
		//"--maximum-zoom", "18",
		"--json-progress", "--progress-interval", "5",
		"--read-parallel",
		"--json-progress",
		"--progress-interval", "5",
		"--layer", "${LAYER_NAME}", // TODO: what's difference layer vs name?
		"--name", "${TILESET_NAME}",
		"--output", "${OUTPUT_FILE}",
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

func (c CLIFlagsT) MustSetPair(key, value string) CLIFlagsT {
	next, _ := c.SetPair(key, value)
	return next
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
