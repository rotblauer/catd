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
	"github.com/rotblauer/catd/api"
	"github.com/rotblauer/catd/conceptual"
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
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
)

var optSortTrackBatches bool
var optWorkersN int

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import cat tracks from stdin",
	Long: `

Tracks from mixed cats ARE supported, e.g. master.json.gz.

Tracks are decoded as JSON lines from stdin and grouped by cat before decoding into CatTracks.
Once the cat batch size is fulfilled, the cat batch is passed to an async Populate worker.
Because cat.Populate is holds a lock on cat state, same-cat workers will block.
This can't be helped because we want to process each cat's tracks in order the order they were originally saved.

Flags:

  --sort        Sort the batches by track time. This is time consuming and makes populate work in batches (vs pure stream). (Default is true).
  --workers     Number of workers to run in parallel. But remember: cat-populate calls are blocking PER CAT. 
                For best results, use a value approximately equivalent to the total number cats. (Default is 8.)
  --batch-size  Number of tracks per cat-batch. (Default is 100_000.)

Examples:

  zcat master.json.gz | catd import --workers 12 --batch-size 100_000 --sort true

A note on ordering and sorting:

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

Processing goes at the rate of about 8000 tracks/cat/second.
A master.json.gz with a majority of 2 cats represented flows at around 16000 tracks/second.

ThinkPad P52. 
Missoula, Montana
20241120
`,
	Run: func(cmd *cobra.Command, args []string) {
		setDefaultSlog(cmd, args)

		ctx, ctxCanceler := context.WithCancel(context.Background())
		interrupt := make(chan os.Signal)
		signal.Notify(interrupt,
			os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGQUIT,
		)
		defer func() {
			slog.Info("Import done")
		}()

		// workersWG is used for clean up processing after the reader has finished.
		workersWG := new(sync.WaitGroup)
		workCh := make(chan workT, optWorkersN)

		// workingWorkN is used to ensure cat chronology.
		// It permits 2 cats to populate simultaneously,
		// but does not permit the same cat to Populate out of order.
		// It is incremented for each work package received.
		// Workers then block until their increment matches the latest package number.
		// Since cat Populators do not block each other, the worker
		// will only block if it is the same cat as the previous package.
		var workingWorkN int32 = 0

		workerFn := func(workerI int, w workT) {
			defer workersWG.Done()
			if len(w.lines) == 0 {
				return
			}

			cat := api.NewCat(conceptual.CatID(w.name))
			slog.Info("Populating",
				"worker", fmt.Sprintf("%d/%d", workerI, optWorkersN),
				"work-n", w.n,
				"cat", cat.CatID, "lines", len(w.lines))

			pipe := stream.Transform(ctx, func(data []byte) *cattrack.CatTrack {
				ct := &cattrack.CatTrack{}
				if err := ct.UnmarshalJSON(data); err != nil {
					slog.Error("Failed to unmarshal track", "error", err)
					return nil
				}
				return ct
			}, stream.Slice(ctx, w.lines))

			// Ensure ordered cat tracks per cat.
			for !atomic.CompareAndSwapInt32(&workingWorkN, w.n-1, w.n) {
			}

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
			workCh <- workT{n: receivedWorkN.Load(), name: cat, lines: lines} //
		}

		quit := make(chan struct{})
		linesCh, errCh, _ := stream.ScanLinesBatchingCats(os.Stdin, quit, params.DefaultBatchSize, optWorkersN)

		go func() {
			for i := 0; i < 2; i++ {
				select {
				case sig := <-interrupt:
					slog.Warn("Received signal", "signal", sig, "i", i)
					if i == 0 {
						quit <- struct{}{}
					} else {
						log.Fatalln("Force exit")
					}
				}
			}
		}()

	readLoop:
		for {
			select {
			case lines := <-linesCh:
				handleLinesBatch(lines)

			case err := <-errCh:
				if err == io.EOF {
					log.Println("EOF")
					break readLoop
				}
				if err == nil {
					break readLoop
				}
				log.Fatal(err)
			}
		}

		// Flush any remaining lines
		for linesCh != nil && len(linesCh) > 0 {
			slog.Info("Import flushing remaining cat-line batches", "len", len(linesCh))
			handleLinesBatch(<-linesCh)
		}

		slog.Warn("Closing work chan")
		close(workCh)
		slog.Warn("Waiting on workers")
		workersWG.Wait()
		slog.Warn("Canceling context")
		ctxCanceler()
	},
}

func init() {
	rootCmd.AddCommand(importCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// importCmd.PersistentFlags().String("foo", "", "A help for foo")
	importCmd.PersistentFlags().BoolVar(&optSortTrackBatches, "sort", true, "Sort the track batches by time")
	importCmd.PersistentFlags().IntVar(&optWorkersN, "workers", 8, "Number of workers to run parallel")
	importCmd.PersistentFlags().IntVar(&params.DefaultBatchSize, "batch-size", 100_000, "Batch size (sort, cat/scan)")
	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// importCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// workT is passed to concurrent workers.
// It represents a batch of cat tracks for some (one) cat.
type workT struct {
	name  string
	n     int32
	lines [][]byte
}
