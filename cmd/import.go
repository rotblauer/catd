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
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/types/cattrack"
	"github.com/spf13/cobra"
	"io"
	"log"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var lastTrackStorePath = "/tmp/catd-last-track.json"

func importStoreReadN(n int64) error {
	f, err := os.OpenFile(lastTrackStorePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte(strconv.FormatInt(n, 10)))
	if err != nil {
		return err
	}
	return nil
}

func importReadMarker() (int64, error) {
	f, err := os.Open(lastTrackStorePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return 0, err
	}
	// Trim newlines.
	return strconv.ParseInt(string(data), 10, 64)
}

type readTrackLogger struct {
	once          sync.Once
	started       time.Time
	n             atomic.Uint64
	lastTrackTime time.Time
	interval      time.Duration
	ticker        *time.Ticker
}

func (rl *readTrackLogger) mark(trackTime time.Time) {
	rl.lastTrackTime = trackTime
	rl.n.Add(1)
}

func (rl *readTrackLogger) run() {
	rl.ticker = time.NewTicker(rl.interval)
	for range rl.ticker.C {
		rl.log()
	}
}

func (rl *readTrackLogger) log() {
	n := rl.n.Load()
	tps := math.Round(float64(n) / time.Since(rl.started).Seconds())
	slog.Info("Read tracks", "n", n, "read.last", rl.lastTrackTime, "tps", tps)
}

func (rl *readTrackLogger) done() {
	rl.ticker.Stop()
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
		signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)

		populating := sync.WaitGroup{}

		catHat := func(id conceptual.CatID) chan *cattrack.CatTrack {
			in := make(chan *cattrack.CatTrack)

			populating.Add(1)

			go func() {
				defer populating.Done()

				cat := &api.Cat{CatID: id}

				// TODO: Flag me.
				err := cat.Populate(ctx, true, true, in)
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

		lastReadN, lastReadNRestoreErr := importReadMarker()
		if lastReadNRestoreErr == nil {
			slog.Info("Restored last read track n", "n", lastReadN)
		} else {
			slog.Warn("Failed to restore last read track", "error", lastReadNRestoreErr)
		}

		readN, skippedN := int64(0), int64(0)
		skipLog, readLog := sync.Once{}, sync.Once{}
		dec := json.NewDecoder(os.Stdin)

		tlogger := &readTrackLogger{
			interval: 5 * time.Second,
		}

	readLoop:
		for {
			// Decoding JSON is slow (...er than handling raw []bytes).
			// So if we can, we avoid struct-decoding as we skip already-seen tracks.
			// We compare the timestamp of the last-seen track for some cat,
			// and only break the no-decode condition once we find it.
			// Be advised that this assumes (!) incoming track consistent order.
			if skippedN < lastReadN {
				skipLog.Do(func() {
					slog.Warn("Skipping decode on already-seen tracks...")
				})

				m := json.RawMessage{}
				err := dec.Decode(&m)
				if err != nil {
					// The unexpected can/will happen, e.g. SIGINT.
					// Only a warning.
					if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
						slog.Warn("Decode error", "error", err)
						break readLoop
					}
					// Else a real error.
					slog.Error("Decode error", "error", err)
					break readLoop
				}
				skippedN++
				continue readLoop
			}

			readLog.Do(func() {
				slog.Info("Reading tracks", "skipped", skippedN)
				readN = skippedN
				tlogger.started = time.Now()
				tlogger.n.Store(0)
				go tlogger.run()
			})

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

			tlogger.mark(ct.MustTime())

			if lastCatID != ct.CatID() {
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
				readN++
			}
		}

		tlogger.done()

		if err := importStoreReadN(readN); err != nil {
			slog.Error("Import command failed to restore last imported track", "error", err)
		} else {
			slog.Info("Import command stored last track")
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
