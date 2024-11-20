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

// workT is passed to concurrent workers.
// It represents a batch of cat tracks for some (one) cat.
type workT struct {
	name  string
	lines [][]byte
}

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import cat tracks from stdin",
	Long: `Scans geojson.Feature lines from stdin and passes them to api.Populate.

Tracks from mixed cats are supported.
But (as-is) it is NOT A GOOD SORTING HAT. 
Like, 50 tracks/second instead of 5000 tracks/second (or, 8000+).

It IS more efficient to sort them by cat up front, 
and then run this command in parallel for each cat.
You can use 'tdata-commander sort-cats' to sort your cats, 
which will take about 15 minutes for a 6GB master.json.gz. Fast.

The sorting slowness is because in 'zcat master.json.gz'
we're basically importing the tracks as they were originally posted.
So each post (a mini cat-batch of tracks) call cat.Populate, which
blocks on DB access.
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
		workersN := 12
		workCh := make(chan workT, workersN)
		workerFn := func(workerI int, w workT) {
			defer workersWG.Done()
			if len(w.lines) == 0 {
				return
			}

			cat := &api.Cat{CatID: conceptual.CatID(w.name)}
			slog.Info("Populating", "cat", cat.CatID, "worker", workerI, "lines", len(w.lines))

			pipe := stream.Transform(ctx, func(data []byte) *cattrack.CatTrack {
				ct := &cattrack.CatTrack{}
				if err := ct.UnmarshalJSON(data); err != nil {
					slog.Error("Failed to unmarshal track", "error", err)
					return nil
				}
				return ct
			}, stream.Slice(ctx, w.lines))

			// TODO: Flag me.
			err := cat.Populate(ctx, true, false, pipe)
			if err != nil {
				slog.Error("Failed to populate CatTracks", "error", err)
			} else {
				slog.Info("Populator done", "cat", cat.CatID)
			}
		}

		// Spin up the workers.
		for i := 0; i < workersN; i++ {
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
		linesCh, errCh, _ := stream.ScanLinesBatchingCats(os.Stdin, quit, 100_000, workersN)

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
		slog.Info("Flushing remaining lines", "len", len(linesCh))
		for linesCh != nil && len(linesCh) > 0 {
			handleLinesBatch(<-linesCh)
		}

		slog.Info("Closing lines chan")
		close(linesCh)
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

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// importCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
