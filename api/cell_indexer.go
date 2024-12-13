package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/event"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/params"
	catS2 "github.com/rotblauer/catd/s2"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"sync/atomic"
	"time"
)

func (c *Cat) GetDefaultCellIndexer() (*catS2.CellIndexer, error) {
	return catS2.NewCellIndexer(&catS2.CellIndexerConfig{
		CatID:           c.CatID,
		Flat:            c.State.Flat,
		Levels:          catS2.DefaultCellLevels,
		BatchSize:       params.DefaultBatchSize, // 10% of default batch size? Why? Reduce batch-y-ness.
		DefaultIndexerT: catS2.DefaultIndexerT,
		LevelIndexerT:   nil,
	})
}

// S2IndexTracks indexes incoming CatTracks for one cat.
func (c *Cat) S2IndexTracks(ctx context.Context, in <-chan cattrack.CatTrack) error {
	c.getOrInitState(false)

	c.logger.Info("S2 Indexing cat tracks")
	start := time.Now()
	defer func() {
		c.logger.Info("S2 Indexing complete", "elapsed", time.Since(start).Round(time.Second))
	}()

	cellIndexer, err := c.GetDefaultCellIndexer()
	if err != nil {
		c.logger.Error("Failed to initialize indexer", "error", err)
		return err
	}
	defer func() {
		if err := cellIndexer.Close(); err != nil {
			c.logger.Error("Failed to close indexer", "error", err)
		}
	}()

	subs := []event.Subscription{}
	chans := []chan []cattrack.CatTrack{}
	sendErrs := make(chan error, 30)
	for _, level := range cellIndexer.Config.Levels {
		if level < catS2.CellLevelTilingMinimum ||
			level > catS2.CellLevelTilingMaximum {
			continue
		}
		if !c.IsRPCEnabled() {
			c.logger.Warn("No tiled configuration, skipping S2 indexing", "level", level)
			continue
		}
		uniqLevelFeed, err := cellIndexer.FeedOfUniqueTracksForLevel(level)
		if err != nil {
			c.logger.Error("Failed to get S2 feed", "level", level, "error", err)
			return err
		}

		// First paradigm: send unique tracks to tiled, with source mode appending.
		// This builds maps with unique tracks, but where each track is the FIRST "track" seen
		// (this FIRST track can be a "small" Indexed track, though, if multiples were cached).
		// This pattern was used by CatTracksV1 to build point-based maps of unique cells for level 23.
		// The problem with this is that the first track is not as useful as the last track,
		// nor as useful a latest-state index value.
		//u1 := make(chan []cattrack.CatTrack)
		//chans = append(chans, u1)
		//u1Sub := uniqLevelFeed.Subscribe(u1)
		//subs = append(subs, u1Sub)
		//go c.sendUniqueTracksLevelAppending(ctx, level, u1, u1Sub.Err())

		// Second paradigm: in the event of a (any) unique cell for the level,
		// send ALL indexed tracks/index-values from that level to tiled, mode truncate.
		//
		// This might be crazy because when a cat goes wandering in fresh powder,
		// every time they push (a batch of unique tracks) there'll be a lot of redundant data
		// being constantly (or, with each batch) sent to tiled.
		// Big asymmetry between some fresh powder and the avalanche.
		// This is less a concern for low cell levels (e.g. 6) than high ones (e.g. 18).
		//
		// One way to improve this might be to...
		// - give tiled an indexing database option (opposed to only flat gz files),
		//   and then establish a convention for pushing indexed data (i.e. index on this level's cell id).
		//   Tiled would also need to be able to dump all indexed data for some tiled-request config.
		//   This would fix the amount of data going "over the wire" to tiled,
		//   but that's not really the major concern since, for now at least,
		//   since I expect the services to be co-located.
		// ...
		//
		// The major constraint here is that in order to produce an updated tileset (.mbtiles),
		// tippecanoe needs to read ALL the data for that tileset; it doesn't do "updates".
		u2 := make(chan []cattrack.CatTrack)
		chans = append(chans, u2)
		u2Sub := uniqLevelFeed.Subscribe(u2)
		subs = append(subs, u2Sub)
		go func() {
			sendErrs <- c.tiledDumpLevelIfUnique(ctx, cellIndexer, level, u2)
		}()
	}

	// Blocking.
	c.logger.Info("S2 Indexing blocking")
	if err := cellIndexer.Index(ctx, in); err != nil {
		c.logger.Error("CellIndexer errored", "error", err)
	}
	for i, sub := range subs {
		sub.Unsubscribe()
		close(chans[i])
		rpcErr := <-sendErrs
		if rpcErr != nil {
			c.logger.Error("Failed to send unique tracks level", "error", rpcErr)
			return err
		}
	}
	close(sendErrs)
	return nil
}

// tiledDumpLevelIfUnique sends all unique tracks at a given level to tiled, with mode truncate.
func (c *Cat) tiledDumpLevelIfUnique(ctx context.Context, cellIndexer *catS2.CellIndexer, level catS2.CellLevel, in <-chan []cattrack.CatTrack) error {
	unsliced := stream.Unbatch[[]cattrack.CatTrack, cattrack.CatTrack](ctx, in)

	// Block, waiting to see if any unique tracks are sent.
	uniqs := 0
	stream.Sink(ctx, func(track cattrack.CatTrack) { uniqs++ }, unsliced)
	if uniqs == 0 {
		c.logger.Debug("No unique tracks for level", "level", level)
		return nil
	}

	// There were some unique tracks at this level.
	batchSize := params.RPCTrackBatchSize
	pushBatchN := int32(0)
	c.logger.Info("S2 Unique tracks dumping", "level", level, "count", uniqs, "push.batch_size", batchSize)
	defer func() {
		c.logger.Info("S2 Unique tracks dumped", "level", level, "count", uniqs,
			"push.batch_size", batchSize, "pushed.batches", pushBatchN)
	}()

	// So now we need to send ALL unique tracks at this level to tiled,
	// and tell tiled to use mode truncate.
	// This will replace the existing map/level with the newest version of unique tracks.
	dump, errs := cellIndexer.DumpLevel(level)

	levelZoomMin := catS2.TilingDefaultCellZoomLevels[level][0]
	levelZoomMax := catS2.TilingDefaultCellZoomLevels[level][1]

	levelTippeConfig, _ := params.LookupTippeConfig(params.TippeConfigNameCells, nil)
	levelTippeConfig = levelTippeConfig.Copy()
	levelTippeConfig.MustSetPair("--maximum-zoom", fmt.Sprintf("%d", levelZoomMax))
	levelTippeConfig.MustSetPair("--minimum-zoom", fmt.Sprintf("%d", levelZoomMin))

	/*
				2024/12/11 21:13:43 INFO RequestTiling d=tile args=iPhone/s2_cells/level-18-polygons/canonical config="" version=canonical                         [29/9497]
				2024/12/11 21:13:43 INFO PushFeatures d=tile cat=iPhone source=s2_cells layer=level-18-polygons size="8.1 MB"
				2024/12/11 21:13:43 INFO RequestTiling d=tile args=iPhone/s2_cells/level-18-polygons/canonical config="" version=canonical
				2024/12/11 21:13:44 INFO PushFeatures d=tile cat=iPhone source=s2_cells layer=level-18-polygons size="8.1 MB"
				2024/12/11 21:13:44 INFO RequestTiling d=tile args=iPhone/s2_cells/level-18-polygons/canonical config="" version=canonical
				2024/12/11 21:13:44 INFO PushFeatures d=tile cat=iPhone source=s2_cells layer=level-18-polygons size="8.1 MB"
				2024/12/11 21:13:44 INFO RequestTiling d=tile args=iPhone/s2_cells/level-18-polygons/canonical config="" version=canonical
				2024/12/11 21:13:45 INFO PushFeatures d=tile cat=iPhone source=s2_cells layer=level-18-polygons size="8.1 MB"
				2024/12/11 21:13:45 INFO RequestTiling d=tile args=iPhone/s2_cells/level-18-polygons/canonical config="" version=canonical
				2024/12/11 21:13:45 INFO PushFeatures d=tile cat=iPhone source=s2_cells layer=level-18-polygons size="8.1 MB"
				2024/12/11 21:13:45 INFO RequestTiling d=tile args=iPhone/s2_cells/level-18-polygons/canonical config="" version=canonical
				2024/12/11 21:13:45 INFO Read tracks n=20,847,931 read.last=2019-06-16T03:19:17.003Z tps=1577 bps="667 kB" total.bytes="8.8 GB" running=1h4m10s
				2024/12/11 21:13:45 INFO PushFeatures d=tile cat=iPhone source=s2_cells layer=level-18-polygons size="8.1 MB"
				2024/12/11 21:13:46 INFO RequestTiling d=tile args=iPhone/s2_cells/level-18-polygons/canonical config="" version=canonical
				2024/12/11 21:13:46 INFO PushFeatures d=tile cat=iPhone source=s2_cells layer=level-18-polygons size="8.1 MB"
				2024/12/11 21:13:46 INFO RequestTiling d=tile args=iPhone/s2_cells/level-18-polygons/canonical config="" version=canonical
				2024/12/11 21:13:46 INFO PushFeatures d=tile cat=iPhone source=s2_cells layer=level-18-polygons size="8.1 MB"
				2024/12/11 21:13:47 INFO RequestTiling d=tile args=iPhone/s2_cells/level-18-polygons/canonical config="" version=canonical


				should describe batch a limit of memory, not track count
				# batchsizein...MB?

				make all rpc requests in order. do not send all at once.
				these layers are not that big. they should not take that long; i think they're competing.
				batsch size ein MB? 1MB? 2MB? 4MB? 8MB? 16MB? 32MB? 64MB? 128MB? 256MB? 512MB? 1024MB? 2048MB? 4096MB? 8192MB? 16384MB? 32768MB? 65536MB? 131072MB? 262144MB? 524288MB? 1048576MB? 2097152MB? 4194304MB? 8388608MB? 16777216MB? 33554432MB? 67108864MB? 134217728MB? 268435456MB? 536870912MB? 1073741824MB? 2147483648MB? 4294967296MB? 8589934592MB? 17179869184MB? 34359738368MB? 68719476736MB? 137438953472MB? 274877906944MB? 549755813888MB? 1099511627776MB? 2199023255552MB? 4398046511104MB? 8796093022208MB? 17592186044416MB? 35184372088832MB? 70368744177664MB? 140737488355328MB? 281474976710656MB? 562949953421312MB? 1125899906842624MB? 2251799813685248MB? 4503599627370496MB? 9007199254740992MB? 18014398509481984MB? 36028797018963968MB? 72057594037927936MB? 144115188075855872MB? 288230376151711744MB? 576460752303423488MB? 1152921504606846976MB? 2305843009213693952MB? 4611686018427387904MB? 9223372036854775808MB? 18446744073709551616MB? 36893488147419103232MB? 73786976294838206464MB? 147573952589676412928MB? 295147905179352825856MB? 590295810358705651712MB? 1180591620717411303424MB
				ps halt this thing is gonna catch fire

				8.1 MB ~= 9000 tracks batch size

			2024/12/12 13:51:27 ERROR tip failed to pipe source gz file d=tile source=/tmp/catd/tiled/source/ia/s2_cells/level-11-polygons.geojson.gz error="unexpected EOF"                                      [182/9509]
			2024/12/12 13:51:27 INFO Piped gz data to tippecanoe d=tile source=ia/s2_cells/level-11-polygons/canonical size="621 kB"
			2024/12/12 13:51:27 ERROR Failed to tip d=tile error="exit status 110"
			2024/12/12 13:51:27 ++ standard input:20: Reached EOF without all containers being closed: in JSON object {"id":1582930342,"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[-80.09818693656482,36.
			198654568532329],[-80.09818693656483,36.02647124849523]]]}} ia/s2_cells/level-09-polygons/canonical
			2024/12/12 13:51:27 ++ Did not read any valid geometries ia/s2_cells/level-07-polygons/canonical
			2024/12/12 13:51:27 ++ standard input:421: Reached EOF without all containers being closed: in JSON object {"id":1581910177,"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[-80.49938574651256,35
			.626830951349777],[-80.49938574651256,35.615994161089407],[-80.48826719616963,35.615112744177917],[-80.48826719616963,35.625949421199077]]]},"properties":{"Accuracy":10,"Activity":"Automotive","ActivityMode":
			"Automotive","ActivityMode.Automotive":3,"ActivityMode.Bike":0,"ActivityMode.Fly":0,"ActivityMode.Running":0,"ActivityMode.Stationary":0,"ActivityMode.Unknown":0,"ActivityMode.Walking":0,"... ia/s2_cells/leve
			l-13-polygons/canonical
			2024/12/12 13:51:27 INFO Piped gz data to tippecanoe d=tile source=rj/naps/naps/canonical size="71 kB"
			2024/12/12 13:51:27 INFO Piped gz data to tippecanoe d=tile source=ia/naps/naps/canonical size="682 kB"
			2024/12/12 13:51:27 ERROR tip failed to pipe source gz file d=tile source=/tmp/catd/tiled/source/ia/s2_cells/level-12-polygons.geojson.gz error="unexpected EOF"
			2024/12/12 13:51:27 INFO Piped gz data to tippecanoe d=tile source=ia/s2_cells/level-12-polygons/canonical size="1.5 MB"
			2024/12/12 13:51:27 ERROR Failed to tip d=tile error="exit status 110"
			2024/12/12 13:51:27 ++ standard input:19: Reached EOF without all containers being closed: in JSON object {"id":1589746539,"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[-91.52086243796509,41.
			27084919617728],[-91.52086243796509,40.9486948794372],[-91.21213217531624,40.95234044951101],[-91.21213217531624,41.27450036944918]]]},"properties":{"Accuracy":16.002416610717775,"Activity":"Automotive","Acti
			vityMode":"Automotive","ActivityMode.Automotive":233,"ActivityMode.Bike":0,"ActivityMode.Fly":0,"ActivityMode.Running":0,"ActivityMode.Stationary":0,"ActivityMode.Unknown":0,"ActivityMode... rye/s2_cells/leve
			l-08-polygons/canonical
			2024/12/12 13:51:27 ++ Did not read any valid geometries rye/s2_cells/level-07-polygons/canonical
			2024/12/12 13:51:27 ERROR tip failed to pipe source gz file d=tile source=/tmp/catd/tiled/source/rye/s2_cells/level-15-polygons.geojson.gz error="unexpected EOF"


		zcat source/rye/s2_cells/level-18-polygons.geojson.gz | tail -1 | jj -p

		gzip: source/rye/s2_cells/level-18-polygons.geojson.gz: invalid compressed data--format violated
		{
		  "id": 1599423124,
		  "type": "Feature",
		  "geometry": {
		    "type": "Polygon",
		    "coordinates": [
		      [
		        [-104.86297461544879, 38.66596839789597],
		        [-104.86297461544879, 38.66564788873041],
		        [-104.86260974786234, 38.665695125090956],
		        [-104.86260974786234, 38.66601563437531]
		      ]
		    ]
		  },
		  "properties": {
		    "Accuracy": 8.001208305358887,
		    "Activity": "Automotive",
		    "ActivityMode": "Automotive",
		    "ActivityMode.Automotive": 1,
		    "ActivityMode.Bike": 0,
		    "ActivityMode.Fly": 0,
		    "ActivityMode.Running": 0,
		    "ActivityMode.Stationary": 0,
		    "ActivityMode.Unknown": 0,
		    "ActivityMode.Walking": 0,
		    "Alias": "rye",
		    "Count": 1,
		    "Elevation": 1943.

		gzip: source/rj/s2_cells/level-18-polygons.geojson.gz: unexpected end of file
		{
		  "id": 1595169478,
		  "type": "Feature",
		  "geometry": {
		    "type": "Polygon",
		    "coordinates": [[[-92.84333852437592, 46.314022327109
		_bird 12-12_16:18:43 /tmp/catd/tiled
		zcat source/rj/s2_cells/level-09-polygons.geojson.gz | tail -1 | jj -p

		gzip: source/rj/s2_cells/level-09-polygons.geojson.gz: unexpected end of file

		_whim 12-12_16:18:48 /tmp/catd/tiled
		zcat source/rj/s2_cells/level-12-polygons.geojson.gz | tail -1 | jj -p

		gzip: source/rj/s2_cells/level-12-polygons.geojson.gz: unexpected end of file
		{
		  "id": 1602099444,
		  "type": "Feature",
		  "geometry": {
		    "type": "Polygon",
		    "coordinates": [
		      [
		        [-92.04385597109959, 46.92346950977309],
		        [-92.06476847327605, 46.92309529016898],
		        [-92.06336769450594, 46.903705154614094],
		        [-92.04246935559617, 46.90407888376931]



	*/

	sendErrCh := make(chan error, 1)
	go func() {
		defer close(sendErrCh)

		// Batch dumped tracks to avoid sending too many at once.
		batched := stream.Batch(ctx, nil, func(s []cattrack.CatTrack) bool {
			return len(s) == batchSize
		}, dump)

		sourceMode := tiled.SourceModeTrunc
		for s := range batched {
			if atomic.LoadInt32(&pushBatchN) > 0 {
				sourceMode = tiled.SourceModeAppend
			}
			atomic.AddInt32(&pushBatchN, 1)
			err := sendToCatRPCClient(ctx, c, &tiled.PushFeaturesRequestArgs{
				SourceSchema: tiled.SourceSchema{
					CatID:      c.CatID,
					SourceName: "s2_cells",
					LayerName:  fmt.Sprintf("level-%02d-polygons", level),
				},
				TippeConfigName: "",
				TippeConfigRaw:  levelTippeConfig,
				Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical},
				SourceModes:     []tiled.SourceMode{sourceMode},
			}, stream.Transform[cattrack.CatTrack, cattrack.CatTrack](ctx, func(track cattrack.CatTrack) cattrack.CatTrack {
				cp := track
				cp.ID = track.MustTime().Unix()
				cp.Geometry = catS2.CellPolygonForPointAtLevel(cp.Point(), level)
				return cp
			}, stream.Slice(ctx, s)))
			if err != nil {
				sendErrCh <- err
				return
			}
		}
	}()

	// Block on dump errors.
	for err := range errs {
		if err != nil {
			c.logger.Error("Failed to dump level if unique", "error", err)
			return err
		}
	}
	for err := range sendErrCh {
		if err != nil {
			c.logger.Error("Failed to send unique tracks level", "error", err)
			return err
		}
	}
	return nil
}

func (c *Cat) sendUniqueTracksLevelAppending(ctx context.Context, level catS2.CellLevel, in <-chan []cattrack.CatTrack, awaitErr <-chan error) {
	txed := stream.Transform(ctx, func(track cattrack.CatTrack) cattrack.CatTrack {
		cp := track

		cp.ID = track.MustTime().Unix()
		cp.Geometry = catS2.CellPolygonForPointAtLevel(cp.Point(), level)

		return cp
	}, stream.Unbatch[[]cattrack.CatTrack, cattrack.CatTrack](ctx, in))

	levelZoomMin := catS2.TilingDefaultCellZoomLevels[level][0]
	levelZoomMax := catS2.TilingDefaultCellZoomLevels[level][1]

	levelTippeConfig, _ := params.LookupTippeConfig(params.TippeConfigNameCells, nil)
	levelTippeConfig = levelTippeConfig.Copy()
	levelTippeConfig.MustSetPair("--maximum-zoom", fmt.Sprintf("%d", levelZoomMax))
	levelTippeConfig.MustSetPair("--minimum-zoom", fmt.Sprintf("%d", levelZoomMin))

	sendToCatRPCClient[cattrack.CatTrack](ctx, c, &tiled.PushFeaturesRequestArgs{
		SourceSchema: tiled.SourceSchema{
			CatID:      c.CatID,
			SourceName: "s2_cells_first",
			LayerName:  fmt.Sprintf("level-%02d-polygons", level),
		},
		TippeConfigName: "",
		TippeConfigRaw:  levelTippeConfig,
		Versions:        []tiled.TileSourceVersion{tiled.SourceVersionCanonical, tiled.SourceVersionEdge},
		SourceModes:     []tiled.SourceMode{tiled.SourceModeAppend, tiled.SourceModeAppend},
	}, stream.Filter(ctx, func(track cattrack.CatTrack) bool {
		return !track.IsEmpty()
	}, txed))

	for err := range awaitErr {
		if err != nil {
			c.logger.Error("Failed to send unique tracks level", "error", err)
		}
	}
}

// S2CollectLevel returns all indexed tracks for a given S2 cell level.
func (c *Cat) S2CollectLevel(ctx context.Context, level catS2.CellLevel) ([]cattrack.CatTrack, error) {
	c.getOrInitState(true)

	cellIndexer, err := c.GetDefaultCellIndexer()
	if err != nil {
		return nil, err
	}
	defer cellIndexer.Close()

	out := []cattrack.CatTrack{}
	dump, errs := cellIndexer.DumpLevel(level)
	out = stream.Collect(ctx, dump)
	return out, <-errs
}

// S2CollectLevel returns all indexed tracks for a given S2 cell level.
func (c *Cat) S2DumpLevel(wr io.Writer, level catS2.CellLevel) error {
	c.getOrInitState(true)

	cellIndexer, err := c.GetDefaultCellIndexer()
	if err != nil {
		return err
	}
	defer cellIndexer.Close()

	enc := json.NewEncoder(wr)
	dump, errs := cellIndexer.DumpLevel(level)
	go func() {
		for track := range dump {
			if err := enc.Encode(track); err != nil {
				select {
				case errs <- err:
					return
				default:
				}
			}
		}
	}()
	return <-errs
}
