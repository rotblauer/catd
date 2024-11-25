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
	"encoding/json"
	"fmt"
	"github.com/paulmach/orb"
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

			slog.Info("Populating",
				"worker", fmt.Sprintf("%d/%d", workerI, optWorkersN),
				"work-n", w.n,
				"cat", cat.CatID, "lines", len(w.lines))

			pipe := stream.Transform(ctx, func(data []byte) cattrack.CatTrack {
				//ct := &cattrack.CatTrack{}
				/*
						2024/11/24 05:38:06 ERROR Failed to unmarshal track error="json: cannot unmarshal object
					into Go struct field CatTrack.geometry of type orb.Geometry"
						2024/11/24 05:38:06 ERROR   track data data="{\"id\":0,\"type\":\"Feature\",\"bbox\":[-113.4735517,47.1789267,-113.4735517,47.1789267],\"geometry\":{\"type\":\"Point\",\"coordinates\":[-113.4735517,47.1789267]},\"properties\":{\"AccelerometerX\":-0.21,\"AccelerometerY\":-0.07,\"AccelerometerZ\":-9.79,\"Accuracy\":4,\"Activity\":\"Stationary\",\"ActivityConfidence\":100,\"AmbientTemp\":null,\"BatteryLevel\":0.71,\"BatteryStatus\":\"unplugged\",\"CurrentTripStart\":\"2024-10-02T20:36:05.642738Z\",\"Distance\":1226092,\"Elevation\":1229.8,\"GyroscopeX\":0,\"GyroscopeY\":0,\"GyroscopeZ\":0,\"Heading\":-1,\"Lightmeter\":0,\"Name\":\"ranga-moto-act3\",\"NumberOfSteps\":1360706,\"Pressure\":null,\"Speed\":0,\"Time\":\"2024-10-04T02:33:27.801Z\",\"UUID\":\"76170e959f967f40\",\"UnixTime\":1728009207,\"UserAccelerometerX\":0.01,\"UserAccelerometerY\":0,\"UserAccelerometerZ\":0.02,\"Version\":\"gcps/v0.0.0+4\",\"heading_accuracy\":-1,\"speed_accuracy\":2.8,\"vAccuracy\":6}}"

				*/
				ct := cattrack.NewCatTrack(orb.Point{})

				if err := json.Unmarshal(data, ct); err != nil {
					slog.Error("cmd/populate : Failed to unmarshal track", "error", err)
					slog.Error(string(data))
					return cattrack.CatTrack{}
				}

				//ct := cattrack.NewCatTrackFromFeature(f)
				return *ct
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

	pFlags.DurationVar(&optTilingPendingExpiry, "tiling.pending-after", 100*time.Hour,
		`Pending expiry interval for RequestTiling requests, aka "debounce" time.
A long interval will cause pending tiling requests
to (optionally) wait til daemon shutdown sequence.`)

	pFlags.BoolVar(&optTilingAwaitPending, "tiling.await-pending", false,
		`Await pending tiling requests on shutdown.`)

	pFlags.BoolVar(&optTilingOff, "tiling.off", false,
		`Disable tiling daemon (and cat tiling requests).`)

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
