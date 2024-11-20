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
	"syscall"
)

var optSortTrackBatches bool
var optWorkersN int

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import cat tracks from stdin",
	Long: `Scans geojson.Feature lines from stdin and passes them to api.Populate.

Tracks from mixed cats are supported.

Flags:

  --sort      Sort the batches by track time. This is time consuming and makes populate work in batches (vs pure stream).
  --workers   Number of workers to run in parallel. But remember: cat-populate calls are blocking PER CAT.

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
		workerFn := func(workerI int, w workT) {
			defer workersWG.Done()
			if len(w.lines) == 0 {
				return
			}

			cat := api.NewCat(conceptual.CatID(w.name))
			slog.Info("Populating", "worker", workerI, "cat", cat.CatID, "lines", len(w.lines))

			pipe := stream.Transform(ctx, func(data []byte) *cattrack.CatTrack {
				ct := &cattrack.CatTrack{}
				if err := ct.UnmarshalJSON(data); err != nil {
					slog.Error("Failed to unmarshal track", "error", err)
					return nil
				}
				return ct
			}, stream.Slice(ctx, w.lines))

			// TODO: Flag me.
			err := cat.Populate(ctx, optSortTrackBatches, false, pipe)
			if err != nil {
				slog.Error("Failed to populate CatTracks", "error", err)
			} else {
				slog.Info("Populator done", "cat", cat.CatID)
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

		handleLinesBatch := func(lines [][]byte) {
			cat := names.AliasOrSanitizedName(gjson.GetBytes(lines[0], "properties.Name").String())
			workersWG.Add(1)
			workCh <- workT{name: cat, lines: lines} //
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
	importCmd.PersistentFlags().IntVar(&optWorkersN, "workers", 12, "Number of workers to run parallel")
	importCmd.PersistentFlags().IntVar(&params.DefaultBatchSize, "batch-size", 100_000, "Batch size (sort, cat/scan)")
	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// importCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// workT is passed to concurrent workers.
// It represents a batch of cat tracks for some (one) cat.
type workT struct {
	name  string
	lines [][]byte
}
