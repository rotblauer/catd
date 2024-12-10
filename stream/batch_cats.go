package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rotblauer/catd/names"
	"github.com/tidwall/gjson"
	"io"
	"log"
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

func restoreReadCount(lastTrackStorePath string) (int64, error) {
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

// ScanLinesBatchingCats returns a buffered channel (of 'workers' size)
// of same-cat track line batches.
func ScanLinesBatchingCats(reader io.Reader, quit <-chan struct{}, batchSize int, workers int, skip int64) (<-chan [][]byte, chan error) {
	if workers == 0 {
		panic("cats too fast (refusing to send on unbuffered channel)")
	}
	output := make(chan [][]byte, workers)
	err := make(chan error, 1)

	lastReadN, lastReadNRestoreErr := restoreReadCount("/tmp/catscann")
	if lastReadNRestoreErr == nil {
		slog.Info("Restored last read track n", "n", lastReadN)
	} else {
		slog.Warn("Failed to get last read track", "error", lastReadNRestoreErr)
	}

	go func(out chan [][]byte, errs chan error, reader io.Reader) {
		defer close(out)
		defer close(errs)

		readN, skippedN := int64(0), int64(0)
		skipLog, skipLog2, readLog := sync.Once{}, sync.Once{}, sync.Once{}

		catBatches := map[string][][]byte{}
		dec := json.NewDecoder(reader)

		tlogger := &readTrackLogger{
			interval: 5 * time.Second,
		}

		defer func() {
			if err := storeReadCount(readN, "/tmp/catscann"); err != nil {
				slog.Error("Cat scanner failed to store last read track", "error", err)
			} else {
				slog.Info("Cat scanner stored last read n", "n", readN)
			}
		}()

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
						out <- lines
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
				out <- catBatches[name]
				catBatches[name] = [][]byte{}
			}

			select {
			case <-quit:
				slog.Warn("Reader received quit")
				break readLoop
			default:
			}
		}

		tlogger.done()

		// Flush any remaining cats (partial batches)
		for cat, trackLines := range catBatches {
			if len(trackLines) > 0 {
				slog.Info("Cat-batch streamer flushing pending", "cat", cat, "track_lines", len(trackLines))
				out <- trackLines
				delete(catBatches, cat)
			}
		}

	}(output, err, reader)

	return output, err
}

func sendErr(errs chan error, err error) {
	select {
	case errs <- err:
	default:
		log.Println("error channel full, dropping error", err)
	}
}

func ScanLinesUnbatchedCats(reader io.Reader, quit <-chan struct{}, workersN, bufferN int) (chan chan []byte, chan error) {
	out := make(chan chan []byte, workersN)
	errs := make(chan error, 1)
	go func() {
		defer close(errs)
		defer close(out)
		dec := json.NewDecoder(reader)
		catMap := sync.Map{}
		catCount := 0
		quitCat := make(chan struct{})
		for {
			select {
			case <-quit:
				log.Println("Unbatcher quitting")
				for i := 0; i < catCount; i++ {
					log.Println("Unbatcher quitting cat", i)
					quitCat <- struct{}{}
				}
				close(quitCat)
				return
			default:
			}

			msg := json.RawMessage{}
			err := dec.Decode(&msg)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				sendErr(errs, fmt.Errorf("scanner(%w)", err))
				return
			}
			cat := gjson.GetBytes(msg, "properties.Name").String()
			if cat == "" {
				sendErr(errs, fmt.Errorf("[scanner] missing properties.Name in line: %s", string(msg)))
				return
			}
			catID := names.AliasOrSanitizedName(cat)
			if _, loaded := catMap.LoadOrStore(catID, struct{}{}); loaded {
				continue // questing fresh cats
			}

			// fresh cat
			log.Println("Unbatcher starting cat", catID)
			catCount++
			catLine := dec.Buffered()
			go func() {
				catR, catW := io.Pipe()
				catLines, catErrs := readCat(catID, catR, bufferN, quitCat)

				go func() {
					for {
						select {
						case out <- catLines:
						case err := <-catErrs:
							if err != nil {
								sendErr(errs, err)
								return
							}
						case <-quit:
							return
						}
					}
				}()

				// Tee the original reader
				if _, err := io.Copy(catW, io.MultiReader(catLine, reader)); err != nil {
					sendErr(errs, err)
					return
				}
			}()
		}
	}()
	return out, errs
}

func readCat(catId string, reader io.Reader, bufferN int, quit chan struct{}) (chan []byte, chan error) {
	catCh := make(chan []byte, bufferN)
	errCh := make(chan error, 1)
	go func(catID string, reader io.Reader) {
		defer close(catCh)
		defer close(errCh)
		// read catReader and send []byte to cat chan
		catDec := json.NewDecoder(reader)
		for {
			select {
			case <-quit:
				return
			default:
			}
			msg := json.RawMessage{}
			err := catDec.Decode(&msg)
			if err != nil {
				if errors.Is(err, io.EOF) {
					close(catCh)
					return
				}
				sendErr(errCh, fmt.Errorf("%s(%w): %s", catID, err, string(msg)))
				return
			}
			name := gjson.GetBytes(msg, "properties.Name").String()
			if name == "" {
				sendErr(errCh, fmt.Errorf("cat(%s) missing properties.Name in line: %s", catID, string(msg)))
				return
			}
			cat := names.AliasOrSanitizedName(name)
			if cat != catId {
				continue
			}
			catCh <- msg
		}
	}(catId, reader)
	return catCh, errCh
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
