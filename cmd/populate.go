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
	//_ "net/http/pprof"
	"os"
	"runtime"
	"sync"
	"time"
)

var optSortTrackBatches bool
var optWorkersN int = runtime.NumCPU()
var optSkipOverrideN int64
var optTilingSkipEdge bool
var optTilingPendingExpiry time.Duration
var optTilingAwaitPending bool
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

		//defer profile.Start(profile.MemProfile).Stop()

		ctx, ctxCanceler := context.WithCancel(context.Background())
		interrupt := common.Interrupted()

		var d *tiled.TileDaemon
		if !optTilingOff {
			dConfig := params.DefaultTileDaemonConfig()
			dConfig.AwaitPendingOnShutdown = optTilingAwaitPending
			dConfig.TilingPendingExpiry = optTilingPendingExpiry
			dConfig.SkipEdge = optTilingSkipEdge
			var err error
			d, err = tiled.NewDaemon(dConfig)
			if err != nil {
				log.Fatalln(err)
			}
			if err := d.Run(); err != nil {
				log.Fatalln(err)
			}
		} else {
			slog.Warn("Tiling daemon disabled")
		}

		// workersWG is used for clean up processing after the reader has finished.
		workersWG := new(sync.WaitGroup)

		quitScanner := make(chan struct{}, 3)
		catChCh, errCh := stream.ScanLinesUnbatchedCats(
			os.Stdin, quitScanner,
			optWorkersN, params.DefaultBatchSize, params.RPCTrackBatchSize)

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

		catN := 0
	readLoop:
		for {
			select {
			case catCh, open := <-catChCh:
				if !open {
					slog.Info("Channel of cat channels closed")
					break readLoop
				}
				catN++
				workersWG.Add(1)
				slog.Info("Received cat chan", "cat", catN)
				go catWorkerFn(ctx, catN, catCh, workersWG, d)

			case err, open := <-errCh:
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
			}
		}

		slog.Info("Waiting on workers")
		workersDone := make(chan struct{})
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					slog.Info("Still waiting on workers...")
				case <-workersDone:
					return
				}
			}
		}()
		workersWG.Wait()
		workersDone <- struct{}{}
		close(workersDone)

		if !optTilingOff {
			slog.Info("Interrupting tiled")
			d.Stop()
			slog.Info("Waiting on tiled")
			d.Wait()
		}

		slog.Info("Canceling context")
		ctxCanceler()
		slog.Info("Populate graceful dismount")
	},
}

func catWorkerFn(ctx context.Context, catN int, catCh chan []byte, done *sync.WaitGroup, d *tiled.TileDaemon) {
	defer done.Done()

	var tiledConfig *params.TileDaemonConfig
	if d != nil {
		tiledConfig = d.Config
	}

	var cat *api.Cat
	var err error

	first := <-catCh
	slog.Info("First track", "cat", catN, "track", string(first))

	catID := names.AliasOrSanitizedName(gjson.GetBytes(first, "properties.Name").String())
	cat, err = api.NewCat(conceptual.CatID(catID), tiledConfig)
	if err != nil {
		panic(err)
	}
	slog.Info("Populating",
		"cat-worker", fmt.Sprintf("%d/%d", catN, optWorkersN),
		"cat", cat.CatID)

	defer func() {
		slog.Info("Cat worker populator done", "cat", cat.CatID)
	}()

	recat := make(chan []byte, params.DefaultBatchSize)
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
		slog.Error("Failed to populate CatTracks", "error", err)
	} else {
		slog.Info("Populator worker done", "cat", cat.CatID)
	}
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

	pFlags.BoolVar(&optTilingSkipEdge, "tiled.skip-edge", false,
		`Skip edge tiling. All PushFeature requests will be treated as canonical.
This is useful for development and initial runs, 
where long-deferred edge tiling could amass large amounts of edge data needlessly.`)

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
