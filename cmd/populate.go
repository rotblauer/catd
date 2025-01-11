/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/paulmach/orb/geojson"
	"github.com/rotblauer/catd/api"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/daemon/rgeod"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/names"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"github.com/spf13/cobra"
	"github.com/tidwall/gjson"
	"io"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"
)

var optSortTrackBatches bool
var optCatWorkersN int = runtime.NumCPU()
var optPushLimitN int = 0 // 0 to disable.
var optAutoTilingOff bool
var optAutoRgeoDOff bool
var optWhitelistCats []string

// populateCmd represents the import command
var populateCmd = &cobra.Command{
	Use:   "populate",
	Short: "Populate cat tracks from stdin stream",
	Long: `

Tracks from mixed cats ARE supported, e.g. master.json.gz.

Tracks are decoded as JSON lines from stdin and grouped by cat before decoding into CatTracks.
Graceful shutdown is MANDATORY for data integrity. Be patient.

This command can run idempotently, or incrementally, given the same source.
Cats will refuse to import "backtracks", where those are tracks which do not extend
their first:last track time range.

Examples:

  zcat master.json.gz | catd populate --workers 12 --batch-size 9_000 --sort true

Notes:

Ordering and sorting:

MOST tracks are ordered defacto -- they were created, listed, pushed, and appended cat-chronologically, 
and generally close to globally-chronologically (albeit with many riffles at the per-cat edges due to async client recording and pushing).
But some tracks aren't; either because of client bugs or adhoc retrospective cat tracking.
For example, about a year in to the CatTrack life I pushed some old GPS records I had from biking in Boston years earlier.
While tracks within a batch can/will get sorted, there is no guarantee that ALL tracks
for any cat are ordered. This isn't a huge deal given that 
a) again, MOST tracks ARE already sorted.
b) operations that depend on chronology, like TripDetector, are stateful 
   and can handle out-of-order tracks by breaking/resetting on temporal discontinuity.

Since we want to process each cat's tracks chronologically (according to the order they were saved),
the best (at optimum) we can do as far as parallelization is, basically, to run one thread for each cat.
Implementation uses the atomic package to ensure incremental worker order, where the workers block until
their increment is matched, but once matched, different-cat workers will not block (on state).
This permits different-cat batches to run in parallel, but ensures same-cat batches wait politely in order.

But we are still limited by the source input order. For example, if rye cat has posted 1_000_000 tracks at once,
then master.json.gz will have 10 batches (of 100_000) to consume before any other cat's tracks can be processed -
and these will all be serially blocking. This is an inherent limitation of the input format and the way we process it.

ThinkPad P52. 
Missoula, Montana
20241120
`,
	PreRun: func(cmd *cobra.Command, args []string) {
		/*
			Bummer, so far.
			Can't background/disown this command, which means populate would need
			a way to WAIT for these children before quitting with them.
			Gonna be easier to just use them in proc.
			Could use exec.Command to fire one off, but that seems extra hacky.
		*/
		setDefaultSlog(cmd, args)
		slog.Info("populate.PreRun")
		//// Automagically start the tiling daemon.
		////var d *tiled.TileDaemon
		//if !optAutoTilingOff {
		//	slog.Info("Checking auto-start on tiled")
		//	tryGetOrInitTileD(cmd, args)
		//} else {
		//	slog.Warn("Tiling daemon disabled")
		//}
		if !optAutoRgeoDOff {
			slog.Info("Checking auto-start on rgeod")
			tryGetOrInitRgeoD(cmd, args)
		} else {
			slog.Warn("Reverse Geocode daemon disabled")
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		setDefaultSlog(cmd, args)
		slog.Info("populate.Run")

		// we need a webserver to get the pprof webserver
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
		//defer profile.Start(profile.MemProfile).Stop()

		ctx, ctxCanceler := context.WithCancel(context.Background())
		interrupt := common.Interrupted()

		// Configure cat connections.
		catBackendC := params.DefaultCatBackendConfig()
		catBackendC.TileD.Network = optTilingListenNetwork
		catBackendC.TileD.Address = optTilingListenAddress
		catBackendC.RgeoD.Network = params.InProcRgeoDaemonConfig.Network
		catBackendC.RgeoD.Address = params.InProcRgeoDaemonConfig.Address
		catBackendC.RgeoD.ServiceName = params.InProcRgeoDaemonConfig.ServiceName

		// An in-proc TileDaemon.
		// Would rather have this as a backgrounded/disowned process, but can't do that yet without a waiter.
		var d *tiled.TileDaemon
		if !optAutoTilingOff {
			dConfig := params.DefaultTileDaemonConfig()
			dConfig.Address = optTilingListenAddress
			dConfig.Network = optTilingListenNetwork
			dConfig.AwaitPendingOnShutdown = optTilingAwaitPending
			dConfig.TilingPendingExpiry = optTilingPendingExpiry
			dConfig.SkipEdge = optTilingSkipEdge
			var err error
			d, err = tiled.NewDaemon(dConfig)
			if err != nil {
				log.Fatalln(err)
			}
			if err := d.Start(); err != nil {
				log.Fatalln(err)
			}
		} else {
			slog.Warn("Tiling daemon disabled")
		}

		// The reader will finish before the workers do. Wait for them.
		catsWorking := new(sync.WaitGroup)

		whiteCats := make([]conceptual.CatID, len(optWhitelistCats))
		for i, cat := range optWhitelistCats {
			whiteCats[i] = conceptual.CatID(cat)
		}
		if len(optWhitelistCats) == 0 {
			whiteCats = nil
		}

		quitScanner := make(chan struct{}, 4)
		catChCh, scanErrCh := stream.ScanLinesUnbatchedCats(
			os.Stdin, quitScanner,
			// Small buffer to keep scanner running while workers catch up.
			// A small buffer is faster than a large one,
			// but too small is slower. These numbers are magic. Around 5MB/s.
			optCatWorkersN, 1_111, 111_111, optPushLimitN, whiteCats)

		go func() {
			for i := 0; i < 2; i++ {
				select {
				case sig := <-interrupt:
					slog.Warn("Received signal", "signal", sig, "i", i)
					if i == 0 {
						quitScanner <- struct{}{}

					} else {
						slog.Warn("Force exit")
						os.Exit(1)
					}
				}
			}
		}()

		populateErrs := make(chan error, optCatWorkersN)
		catWorkers := make(map[conceptual.CatID]*catWorker)

		catN := 0
	readLoop:
		for {
			select {
			case catCh, open := <-catChCh:
				if !open {
					slog.Info("Channel of cat channels closed")
					break readLoop
				}
				slog.Info("Received cat chan", "cat", catN)
				worker, ok := catWorkers[conceptual.CatID(catCh.ID)]
				if !ok {
					catN++
					catsWorking.Add(1)
					worker = &catWorker{
						ctx:     ctx,
						backend: catBackendC,
						catN:    catN,
						wg:      catsWorking,
						errCh:   populateErrs,
						jobs:    make(chan chan []byte),
					}
					go worker.run()
					catWorkers[conceptual.CatID(catCh.ID)] = worker
				}
				worker.jobs <- catCh.Ch

			case err, open := <-scanErrCh:
				// out of tracks
				if errors.Is(err, io.EOF) {
					slog.Info("CatScanner EOF")
					break readLoop
				}
				// user interrupt
				if errors.Is(err, io.ErrUnexpectedEOF) {
					slog.Warn("CatScanner unexpected EOF")
					break readLoop
				}
				if errors.Is(err, stream.ErrMissingAttribute) {
					slog.Warn("CatScanner found bad track", "error", err)
					continue
				}
				if err != nil {
					slog.Error("CatScanner errored", "error", err)
					os.Exit(1)
				}
				if !open {
					slog.Warn("CatScanner closed err channel")
					break readLoop
				}
			case err := <-populateErrs:
				if err != nil {
					slog.Error("Received populate error", "error", err)
					quitScanner <- struct{}{}
					break readLoop
				}
			}
		}

		for _, v := range catWorkers {
			close(v.jobs)
		}

		slog.Info("Waiting on workers")
		stopTicker := make(chan struct{})
		go func() {
			defer close(stopTicker)
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					slog.Info("Still waiting on workers...")
				case <-stopTicker:
					return
				}
			}
		}()
		catsWorking.Wait()
		stopTicker <- struct{}{}

		if !optAutoTilingOff {
			slog.Info("Interrupting tiled")
			d.Interrupt()
			slog.Info("Waiting on tiled")
			d.Wait()
		}

		slog.Info("Canceling context")
		ctxCanceler()
		slog.Info("Populate graceful dismount")
	},
}

type catWorker struct {
	ctx     context.Context
	backend *params.CatRPCServices
	catN    int
	wg      *sync.WaitGroup
	errCh   chan error
	jobs    chan chan []byte
}

func (c *catWorker) run() {
	defer c.wg.Done()
	for job := range c.jobs {
		c.catPopulate(c.ctx, c.catN, job, c.errCh, c.backend)
	}
}

// catPopulate runs Populate for some cat, non-blocking (re waitgroup).
// CatID is determined by the first track read from the channel.
// The call to Populate will block on DB lock for each cat, which
// makes calling this function for the same cat concurrently harmless, but useless.
func (c *catWorker) catPopulate(ctx context.Context, catN int, catCh chan []byte, errCh chan error, backend *params.CatRPCServices) {
	var err error
	var cat *api.Cat

	first, open := <-catCh
	if !open {
		slog.Warn("CatWorker channel closed before any tracks received", "cat", catN)
		return
	}

	slog.Debug("CatWorker first track", "cat", catN, "track", string(first))
	catID := names.AliasOrSanitizedName(gjson.GetBytes(first, "properties.Name").String())
	id := conceptual.CatID(catID)
	catD := params.DefaultCatDataDir(id.String())
	cat, err = api.NewCat(id, catD, backend)
	if err != nil {
		panic(err)
	}
	slog.Info("Populating",
		"cat-worker", fmt.Sprintf("%d/%d", catN, optCatWorkersN),
		"cat", cat.CatID)

	defer func() {
		slog.Info("Cat worker populator done", "cat", cat.CatID)
	}()

	recat := make(chan []byte, params.DefaultChannelCap)
	recat <- first
	go func() {
		defer close(recat)
		for line := range catCh {
			recat <- line
		}
	}()

	decoded := stream.Transform(ctx, func(data []byte) cattrack.CatTrack {
		feat, err := geojson.UnmarshalFeature(data)
		if err != nil {
			slog.Error("cmd/populate : Failed to unmarshal track", "error", err)
			slog.Error(string(data))
			return cattrack.CatTrack{}
		}
		return cattrack.CatTrack(*feat)
	}, recat)

	// Populate is blocking. It holds a lock on the cat state.
	err = cat.Populate(ctx, optSortTrackBatches, decoded)
	if err != nil {
		errCh <- err
		slog.Error("Failed to populate CatTracks", "error", err)
	} else {
		slog.Info("Populator worker done", "cat", cat.CatID)
	}
}

func init() {
	rootCmd.AddCommand(populateCmd)
	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// populateCmd.PersistentFlags().String("foo", "", "A help for foo")
	flags := populateCmd.Flags()

	// Cat Populate likes to have RPC hookups to be able to
	// - index cats on Reverse Geocode lookups (and export associated geometry)
	// - make maps
	// This command will auto-start those services if not found.
	//flags.AddFlagSet(rgeodListenerFlags)
	//flags.AddFlagSet(tiledCmd.Flags())
	// ...actually can't add them here, must be shared from original cmd

	// Here you will define your flags and configuration settings.
	flags.BoolVar(&optAutoTilingOff, "auto-tiled.off", false,
		`Disable automatic starting tiling daemon.
Attempts to start 'catd tiled' (with shared --tiled.listen opts) 
if configured RPC client ping request fails.`)
	flags.BoolVar(&optAutoRgeoDOff, "auto-rgeod.off", false,
		`"Disable automatic starting reverse geocode daemon.
Attempts to start 'catd rgeod' (with shared --rgeod.listen opts) 
if configured RPC client ping request fails.`)

	flags.BoolVar(&optSortTrackBatches, "sort", true,
		`Sort cat tracks chronologically, in batches. 
This is time a little resource consuming and makes populate work in batches (vs pure stream).
Also relatively important (critical) for cat tracking.`)

	flags.IntVar(&optCatWorkersN, "workers", runtime.NumCPU(),
		`Number of cat/workers to run in parallel.
Cat.Populate calls are blocking PER CAT.
For optimal results, use the number of cats tracked.
For superoptimal results, use 0, to unlimit the number of cats tracked concurrently.
`)
	flags.IntVar(&optPushLimitN, "push-limit", 0,
		`Max number of track lines to process per call to Cat.Populate. Use 0 to disable.
Using a non-zero value (eg. 100) simulates push-batch sizing, generally causing Populate calls
to behave equivalently to real-world batched (HTTP) calls to Populate. 
This can be useful for testing store/restore of state machines.
Using a zero value causes Populate to read the entire input stream, unless interrupted by a stale interval. 
`)

	flags.StringSliceVar(&optWhitelistCats, "whitelist", nil,
		`Only these cats will be populated.`)

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// populateCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// workT is passed to concurrent workers.
// It represents a batch of cat tracks for some (one) cat.
type workT struct {
	name      string
	n         int32
	indexedAt int
	lines     [][]byte
}

var rpcToleratedErrors = []syscall.Errno{syscall.ECONNREFUSED, syscall.ENOENT}

func tryGetOrInitTileD(cmd *cobra.Command, args []string) {
	once := sync.Once{}
	call := func() {
		slog.Warn(" 🚧 TileD RPC client connection refused, attempting auto-start")
		// FIXME: Can these be backgrounded/disowned to live beyond cmdPopulate?
		// Otherwise, have to listen RPC for ReadyToQuit, or there's no time for tiling.
		cmd := &cobra.Command{}
		*cmd = *tiledCmd
		cmd.SetContext(context.Background())
		go cmd.Run(cmd, args)
	}
	start := time.Now()
trying:
	for ; time.Since(start) < 1*time.Minute; time.Sleep(1 * time.Second) {
		rpcClient, err := common.DialRPC(optTilingListenNetwork, optTilingListenAddress)
		if err == nil {
			err = rpcClient.Call("TileD.Ping", common.ArgNone, nil)
			_ = rpcClient.Close()
		}
		if err == nil {
			// Cool, we're outta here.
			slog.Info(" 🚀 TileD responded pong, running")
			return
		}
		// Gotcha: errors from RPC client isn't _the actual_ error,
		// because it's coming from the RPC client (not inproc). Leaving noop errors.Is only for fair warning.
		if err.Error() == tiled.ErrNotReady.Error() || errors.Is(err, tiled.ErrNotReady) {
			slog.Warn("TileD not ready, retrying in 1s...", "error", err)
			continue
		}
		for _, tolerated := range rpcToleratedErrors {
			if errors.Is(err, tolerated) {
				once.Do(call)
				continue trying
			}
		}
		log.Fatalln("unhandled error", err)
	}
}

func tryGetOrInitRgeoD(cmd *cobra.Command, args []string) {
	once := sync.Once{}
	call := func() {
		slog.Warn(" 🚧 RgeoD RPC client connection refused, attempting auto-start")
		// FIXME: Can these be backgrounded/disowned to live beyond cmdPopulate?
		// Otherwise, have to listen RPC for ReadyToQuit, or there's no time for tiling.
		// The nice thing, however, about this is that the rgeod daemon will be running
		// inproc.
		cmd := &cobra.Command{}
		*cmd = *rgeodCmd
		cmd.SetContext(context.Background())
		go cmd.Run(cmd, args)
	}
	start := time.Now()
trying:
	for ; time.Since(start) < 1*time.Minute; time.Sleep(2 * time.Second) {
		rpcClient, err := common.DialRPC(
			params.InProcRgeoDaemonConfig.Network,
			params.InProcRgeoDaemonConfig.Address)

		if err == nil {
			err = rpcClient.Call("ReverseGeocode.Ping", common.ArgNone, nil)
			_ = rpcClient.Close()
		}
		if err == nil {
			// Cool, we're outta here.
			slog.Info(" 🚀 RgeoD responded pong, running")
			return
		}
		// Gotcha: errors from RPC client isn't _the actual_ error,
		// because it's coming from the RPC client (not inproc). Leaving noop errors.Is only for fair warning.
		if err.Error() == rgeod.ErrNotReady.Error() || errors.Is(err, rgeod.ErrNotReady) {
			slog.Warn("RgeoD not ready, retrying in 2s...")
			continue
		}
		for _, tolerated := range rpcToleratedErrors {
			if errors.Is(err, tolerated) {
				once.Do(call)
				continue trying
			}
		}
		log.Fatalln("unhandled error", err)
	}
}
