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
	"fmt"
	"github.com/paulmach/orb/geojson"
	"github.com/rotblauer/catd/api"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/conceptual"
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
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var optSortTrackBatches bool
var optWorkersN int = runtime.NumCPU()
var optTilingPendingExpiry time.Duration
var optTilingAwaitPending bool
var optSkipOverrideN int64
var optTilingOff bool

// populateCmd represents the import command
var populateCmd = &cobra.Command{
	Use:   "populate",
	Short: "Populate cat tracks from stdin stream",
	Long: `

Tracks from mixed cats ARE supported, e.g. master.json.gz.

Tracks are decoded as JSON lines from stdin and grouped by cat before decoding into CatTracks.
Graceful shutdown is MANDATORY for data integrity. Be patient.

This command can run idempotently, and incrementally on the same source,
at least so far as a line-scan count will take it.
The cat app, as it is, though, has no strong way of persistent de-duping, yet.

Examples:

  zcat master.json.gz | catd populate --workers 12 --batch-size 100_000 --sort true

Notes:

Same-source, re-runs:

A record is stored in /tmp of the number of lines read (and processed), and if that cache is found
on restart, that number of lines will be skipped (fast!) before processing begins.
This assumes that the input is consistent between runs through that line number - 
so interim append-only operations on the source may be OK, but changing the source entirely will 
cause unexpected and unreasonable outcomes.  

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

Processing goes at the rate of about 7000-8000 tracks/cat/second, or about 15s/100_000 cat batch.
A master.json.gz with a majority of 2 cats represented flows at around 15000 tracks/second.

ThinkPad P52. 
Missoula, Montana
20241120
`,
	Run: func(cmd *cobra.Command, args []string) {
		setDefaultSlog(cmd, args)

		ctx, ctxCanceler := context.WithCancel(context.Background())
		interrupt := common.Interrupted()

		defer func() {
			slog.Info("Import done")
		}()

		var d *tiled.TileDaemon
		if !optTilingOff {
			dConfig := params.DefaultTileDaemonConfig()
			dConfig.AwaitPendingOnShutdown = optTilingAwaitPending
			dConfig.TilingPendingExpiry = optTilingPendingExpiry
			d = tiled.NewDaemon(dConfig)
			if err := d.Start(); err != nil {
				log.Fatal(err)
			}
		} else {
			slog.Warn("Tiling daemon disabled")
		}

		// workersWG is used for clean up processing after the reader has finished.
		workersWG := new(sync.WaitGroup)
		workCh := make(chan workT, optWorkersN)

		// workingWorkN is used to ensure cat chronology.
		// It permits 2 cats to populate simultaneously,
		// but does not permit the same cat to Populate out of order (cat.Populate's state lock ensure serial-cats).
		// It is incremented for each work package received.
		// Workers then block until their increment matches the latest package number.
		// Since cat Populators do not block each other, the worker
		// will only block if it is the same cat as the previous package.
		//var workingWorkN int32 = 0

		workerFn := func(workerI int, w workT) {
			defer workersWG.Done()
			if len(w.lines) == 0 {
				return
			}

			var tiledConfig *params.TileDaemonConfig
			if d != nil {
				tiledConfig = d.Config
			}
			cat := api.NewCat(conceptual.CatID(w.name), tiledConfig)

			// Use a temporary cat flat base path (dir).
			// This will be non-blocking, but will init empty state for each batch;
			// tripdetector, s2indexer will have tabula rasas.
			// There will be spuriously-broken laps and naps at every batch edge.
			// Then need to collapse tmp cat-dirs into a/the canonical one;
			// Depend on lexical ordering for chrono.
			// tracks.geojson.gzs append.
			// cat/state.dbs use last one... (but will break/incomplete snaps KV!).
			// Snapper needs to mv .json and .jpeg files. Which it can do; no conflicts in path naming.
			//firstTrackTime := gjson.GetBytes(w.lines[0], "properties.Time").Time()
			//cat.State.Flat = flat.NewFlatWithRoot(cat.State.Flat.Path() + fmt.Sprintf(".%011d", firstTrackTime.Unix()))

			slog.Info("Populating",
				"worker", fmt.Sprintf("%d/%d", workerI, optWorkersN),
				"work-n", w.n,
				"cat", cat.CatID, "lines", len(w.lines))

			pipe := stream.Transform(ctx, func(data []byte) cattrack.CatTrack {

				feat, err := geojson.UnmarshalFeature(data)
				if err != nil {
					slog.Error("cmd/populate : Failed to unmarshal track", "error", err)
					slog.Error(string(data))
					return cattrack.CatTrack{}
				}

				return cattrack.CatTrack(*feat)
			}, stream.Slice(ctx, w.lines))

			// Ensure ordered cat tracks per cat.
			o := sync.Once{}
			//for !atomic.CompareAndSwapInt32(&workingWorkN, w.n-1, w.n) {
			o.Do(func() {
				slog.Warn("Worker unblocking", "worker", workerI, "cat", cat.CatID, "work-n", w.n)
			})
			//}

			// Populate is blocking. It holds a lock on the cat state.
			err := cat.Populate(ctx, optSortTrackBatches, pipe)
			if err != nil {
				slog.Error("Failed to populate CatTracks", "error", err)
			} else {
				slog.Info("Populator worker done", "cat", cat.CatID)
			}
		}

		// Spin up the workers.
		for i := 0; i < optWorkersN; i++ {
			workerI := i + 1
			go func() {
				workerI := workerI
				for w := range workCh {
					workerFn(workerI, w)
				}
			}()
		}

		// receivedWorkN is used to ensure per-cat populate chronology.
		// Work packages are indexed and the workers consume them likewise.
		// With a blocking per-cat Populate function, this means that cats
		// must (attempt/blocking) Populate in the order in which work was received.
		var receivedWorkN atomic.Int32
		receivedWorkN.Store(0)

		handleLinesBatch := func(lines [][]byte) {
			cat := names.AliasOrSanitizedName(gjson.GetBytes(lines[0], "properties.Name").String())
			workersWG.Add(1)
			receivedWorkN.Add(1)
			workCh <- workT{
				n:     receivedWorkN.Load(),
				name:  cat,
				lines: lines,
			}
		}

		quitScanner := make(chan struct{}, 1)
		linesCh, errCh := stream.ScanLinesBatchingCats(os.Stdin, quitScanner, params.DefaultBatchSize, optWorkersN, optSkipOverrideN)

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

	readLoop:
		for {
			select {
			case lines := <-linesCh:
				handleLinesBatch(lines)

			case err, open := <-errCh:
				if err == io.EOF {
					slog.Info("CatScanner EOF")
					break readLoop
				}
				if err != nil {
					slog.Error("CatScanner errored", "error", err)
					os.Exit(1)
				}
				if !open {
					slog.Warn("CatScanner closed err channel")
					break readLoop
				}
			}
		}

		// Flush any remaining lines
		for linesCh != nil && len(linesCh) > 0 {
			slog.Info("Import flushing remaining cat-line batches", "len", len(linesCh))
			handleLinesBatch(<-linesCh)
		}

		slog.Info("Closing work chan")
		close(workCh)
		slog.Info("Waiting on workers")
		workersWG.Wait()

		if !optTilingOff {
			slog.Info("Interrupting tiled")
			d.Interrupt <- struct{}{}
			slog.Info("Waiting on tiled")
			d.Wait()
		}

		slog.Info("Canceling context")
		ctxCanceler()
		slog.Info("Au revoir!")
	},
}

func init() {
	rootCmd.AddCommand(populateCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// populateCmd.PersistentFlags().String("foo", "", "A help for foo")
	pFlags := populateCmd.PersistentFlags()
	pFlags.BoolVar(&optSortTrackBatches, "sort", true,
		`Sort the batches by track time. 
This is time consuming and makes populate work in batches (vs pure stream).
Also relatively important for cat tracking.`)

	pFlags.IntVar(&optWorkersN, "workers", runtime.NumCPU(),
		`Number of workers to run in parallel. But remember: cat-populate calls are blocking PER CAT.
For best results, use a value approximately equivalent to the total number cats.`)

	pFlags.IntVar(&params.DefaultBatchSize, "batch-size", 100_000,
		`Number of tracks per cat-batch through the cat-scanner. `)

	pFlags.Int64Var(&optSkipOverrideN, "skip", 0,
		`Skip n lines before the cat scanner scans. (Easier than zcat | tail | head.)`)

	pFlags.BoolVar(&optTilingOff, "tiled.off", false,
		`Disable tiling daemon (and cat tiling requests).`)

	pFlags.DurationVar(&optTilingPendingExpiry, "tiled.pending-after", 100*time.Hour,
		`Pending expiry interval for RequestTiling requests, aka "debounce" time.
A long interval will cause pending tiling requests
to (optionally) wait til daemon shutdown sequence.`)

	pFlags.BoolVar(&optTilingAwaitPending, "tiled.await-pending", true,
		`Await pending tiling requests on shutdown.`)

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// populateCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// workT is passed to concurrent workers.
// It represents a batch of cat tracks for some (one) cat.
type workT struct {
	name  string
	n     int32
	lines [][]byte
}
