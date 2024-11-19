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
	"errors"
	"github.com/rotblauer/catd/api"
	"github.com/rotblauer/catd/app"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/conceptual"
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
	"time"
)

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import cat tracks from stdin",
	Long: `Scans geojson.Feature lines from stdin and passes them to api.PopulateCat.

Tracks from mixed cats ARE supported, eg. edge.json.gz; the reader is a cat-sorter.

But, BEWARE, it is not a fast cat sorter. 
It is a slow cat sorter. Why? Probably decoding, I guess.
You may want to sort your cats before piping them in.
If so, you can use 'tdata-commander sort-cats' to sort your cats, 
which will take about 15 minutes for a 6GB master.json.gz. Fast.

Then, run this command in parallel for each actual cat.
`,
	Run: func(cmd *cobra.Command, args []string) {
		setDefaultSlog(cmd, args)

		ctx, ctxCanceler := context.WithCancel(context.Background())
		interrupt := make(chan os.Signal)
		signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)

		populating := sync.WaitGroup{}

		catHat := func(id conceptual.CatID) chan *cattrack.CatTrack {
			in := make(chan *cattrack.CatTrack)

			populating.Add(1)

			go func() {
				defer populating.Done()

				// TODO: Flag me.
				err := api.PopulateCat(ctx, id, true, true, in)
				if err != nil {
					slog.Error("Failed to populate CatTracks", "error", err)
				} else {
					slog.Info("Populator done")
				}
			}()

			return in
		}

		var hat chan *cattrack.CatTrack

		var lastCatID conceptual.CatID
		var lastTrack *cattrack.CatTrack

		readN, skippedN := int64(0), int64(0)
		seenLast := false
		skippingLog, skippedLog := sync.Once{}, sync.Once{}
		nt := time.Now()
		dec := json.NewDecoder(os.Stdin)

	readLoop:
		for {
			// Decoding JSON is slow (...er than handling raw []bytes).
			// So if we can, we avoid struct-decoding as we skip already-seen tracks.
			// We compare the timestamp of the last-seen track for some cat,
			// and only break the no-decode condition once we find it.
			// Be advised that this assumes (!) incoming track consistent order.
			if !seenLast && lastTrack != nil {
				m := json.RawMessage{}
				err := dec.Decode(&m)
				if err != nil {
					// The unexpected can/will happen, e.g. SIGINT.
					// Only a warning.
					if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
						slog.Warn("Decode error", "error", err)
						break
					}
					// Else a real error.
					slog.Error("Decode error", "error", err)
					break
				}
				res := gjson.GetBytes(m, "properties.Time")
				if res.Exists() {

					if !res.Time().Equal(lastTrack.MustTime()) {
						skippingLog.Do(func() {
							slog.Warn("Skipping decode on already-seen tracks...")
						})

						skippedN++
						continue
					} else {
						seenLast = true
						continue
					}
				}
			}

			if readN > 0 {
				skippedLog.Do(func() {
					slog.Info("Reading tracks", "skipped", skippedN)
				})
			}

			ct := &cattrack.CatTrack{}
			err := dec.Decode(ct)
			if err != nil {
				// The unexpected can/will happen, e.g. SIGINT.
				// Only a warning.
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					slog.Warn("Decode error", "error", err)
					break
				}
				// Else a real error.
				slog.Error("Decode error", "error", err)
				break
			}

			readN++
			if readN%(10_000) == 0 {
				t, _ := ct.Time() // Track timestamp.
				tps := float64(readN) / time.Since(nt).Seconds()
				tps = common.DecimalToFixed(tps, 0)
				slog.Info("Read tracks", "reads", readN, "current_track.time", t, "tps", tps)
			}

			if lastCatID != ct.CatID() {
				appCat := app.Cat{CatID: ct.CatID()}
				if r, err := appCat.NewCatReader(); err == nil {
					if last, err := r.ReadLastTrack(); err == nil {
						lastTrack = last
					} else {
						slog.Warn("Failed to read last track", "error", err)
					}
				}
				if hat != nil {
					close(hat)
				}
				lastCatID = ct.CatID()
				hat = catHat(lastCatID)
			}

			select {
			case <-ctx.Done():
				break readLoop
			case sig := <-interrupt:
				slog.Warn("Received signal", "signal", sig)
				break readLoop

				// Fire away!
			case hat <- ct:
			}
		}

		// Provide a way to break of out of deadlocks.
		// Hit CTRL-C twice to force exit.
		go func() {
			for {
				select {
				case sig := <-interrupt:
					slog.Warn("Received signal again", "signal", sig)
					log.Fatalln("Force exit")
				}
			}
		}()

		slog.Warn("Closing cat hat")
		close(hat)
		slog.Warn("Waiting on cat populators")
		populating.Wait()
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
