package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rotblauer/catd/names"
	"github.com/tidwall/gjson"
	"io"
	"log/slog"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type readTrackLogger struct {
	once     sync.Once
	started  time.Time
	n        atomic.Uint64
	logV     any // e.g. track.time
	interval time.Duration
	ticker   *time.Ticker
}

func (rl *readTrackLogger) mark(val any) {
	rl.logV = val
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
	slog.Info("Read tracks", "n", n, "read.last", rl.logV, "tps", tps,
		"running", time.Since(rl.started).Round(time.Second))
}

func (rl *readTrackLogger) done() {
	if rl == nil || rl.ticker == nil {
		return
	}
	rl.ticker.Stop()
}

func storeReadCount(n int64, lastTrackStorePath string) error {
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

func readReadCount(lastTrackStorePath string) (int64, error) {
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

func ScanLinesBatchingCats(reader io.Reader, quit <-chan struct{}, batchSize int, workers int, skip int64) (chan [][]byte, chan error, error) {

	ch := make(chan [][]byte, workers)
	errs := make(chan error)

	lastReadN, lastReadNRestoreErr := readReadCount("/tmp/catscann")
	if lastReadNRestoreErr == nil {
		slog.Info("Restored last read track n", "n", lastReadN)
	} else {
		slog.Warn("Failed to get last read track", "error", lastReadNRestoreErr)
	}

	go func(ch chan [][]byte, errs chan error, reader io.Reader) {
		defer close(ch)
		defer close(errs)

		readN, skippedN := int64(0), int64(0)
		skipLog, skipLog2, readLog := sync.Once{}, sync.Once{}, sync.Once{}

		catBatches := map[string][][]byte{}
		dec := json.NewDecoder(reader)

		tlogger := &readTrackLogger{
			interval: 5 * time.Second,
		}

		didSkip := int64(0)
	readLoop:
		for {
			msg := json.RawMessage{}
			err := dec.Decode(&msg)
			if err != nil {
				errs <- err

				// Send remaining lines, an error expected when EOF.
				for cat, lines := range catBatches {
					if len(lines) > 0 {
						ch <- lines
						delete(catBatches, cat)
					}
				}

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

			if didSkip < skip {
				skipLog2.Do(func() {
					slog.Warn("Skipping --skip lines...", "skip", skip)
				})
				didSkip++
				continue
			}

			readN++

			if readN <= lastReadN {
				skipLog.Do(func() {
					slog.Warn("Skipping decode on already-seen tracks...")
				})

				skippedN++
				continue
			}

			readLog.Do(func() {
				slog.Info("Reading tracks", "skipped", skippedN)
				tlogger.started = time.Now()
				tlogger.n.Store(0)
				go tlogger.run()
			})

			tlogger.mark(gjson.GetBytes(msg, "properties.Time").String())

			result := gjson.GetBytes(msg, "properties.Name")
			if !result.Exists() {
				// Send error, continue.
				errs <- fmt.Errorf("missing properties.Name in line: %s", string(msg))
				continue
			}

			name := names.AliasOrSanitizedName(result.String())
			if _, ok := catBatches[name]; !ok {
				catBatches[name] = [][]byte{}
			}

			catBatches[name] = append(catBatches[name], msg)
			if len(catBatches[name]) >= batchSize {
				ch <- catBatches[name]
				catBatches[name] = [][]byte{}
			}

			select {
			case <-quit:
				slog.Warn("Reader received quit")
				tlogger.done()
				break readLoop
			default:
			}
		}

		tlogger.done()

		if err := storeReadCount(readN, "/tmp/catscann"); err != nil {
			slog.Error("Cat scanner failed to store last read track", "error", err)
		} else {
			slog.Info("Cat scanner stored last read n", "n", readN)
		}

		// Flush any remaining cats (partial batches)
		for cat, lines := range catBatches {
			if len(lines) > 0 {
				slog.Info("Flushing remaining cat lines", "cat", cat, "len", len(lines))
				ch <- lines
				delete(catBatches, cat)
			}
		}

	}(ch, errs, reader)

	return ch, errs, nil
}

/*
panic: runtime error: invalid memory address or nil pointer dereference
[signal SIGSEGV: segmentation violation code=0x1 addr=0x0 pc=0x5b58f1]

goroutine 14 [running]:
time.(*Ticker).Stop(...)
        /home/ia/go1.22.2.linux-amd64/go/src/time/tick.go:45
github.com/rotblauer/catd/stream.(*readTrackLogger).done(...)
        /home/ia/dev/rotblauer/catd/stream/batch_cats.go:47
github.com/rotblauer/catd/stream.ScanLinesBatchingCats.func1(0xc00005e780, 0xc000032660, {0xb3c880, 0xc00011a018})
        /home/ia/dev/rotblauer/catd/stream/batch_cats.go:176 +0xc71
created by github.com/rotblauer/catd/stream.ScanLinesBatchingCats in goroutine 1
        /home/ia/dev/rotblauer/catd/stream/batch_cats.go:89 +0x255

*/
