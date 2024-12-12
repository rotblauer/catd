package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/names"
	"github.com/rotblauer/catd/types/cattrack"
	"github.com/tidwall/gjson"
	"io"
	"log"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type tickScanMeter struct {
	label      time.Time // any value, eg track.time
	interval   time.Duration
	started    time.Time
	ticker     *time.Ticker
	nn         atomic.Uint64
	reg        metrics.Registry
	count      metrics.Counter
	size       metrics.Counter
	countMeter metrics.Meter
	sizeMeter  metrics.Meter
}

func newTickScanMeter(interval time.Duration) *tickScanMeter {
	metrics.Enabled = true // wtf
	reg := metrics.NewRegistry()
	rl := &tickScanMeter{
		reg:        reg,
		interval:   interval,
		started:    time.Now(),
		nn:         atomic.Uint64{},
		count:      metrics.NewCounter(),
		size:       metrics.NewCounter(),
		countMeter: metrics.NewMeter(),
		sizeMeter:  metrics.NewMeter(),
	}

	if err := reg.Register("count.count", rl.count); err != nil {
		panic(err)
	}
	if err := reg.Register("size.count", rl.size); err != nil {
		panic(err)
	}
	if err := reg.Register("line.meter", rl.countMeter); err != nil {
		panic(err)
	}
	if err := reg.Register("size.meter", rl.sizeMeter); err != nil {
		panic(err)
	}
	rl.nn.Store(0)
	go rl.run()
	return rl
}

func (rl *tickScanMeter) mark(label time.Time, data []byte) {
	rl.label = label
	rl.nn.Add(1)
	rl.count.Inc(1)
	rl.size.Inc(int64(len(data)))
	rl.countMeter.Mark(1)
	rl.sizeMeter.Mark(int64(len(data)))
}

func (rl *tickScanMeter) run() {
	rl.ticker = time.NewTicker(rl.interval)
	for range rl.ticker.C {
		rl.log()
	}
}

func (rl *tickScanMeter) log() {

	countSnap := rl.countMeter.Snapshot()
	sizeSnap := rl.sizeMeter.Snapshot()

	slog.Info("Read tracks", "n", humanize.Comma(countSnap.Count()),
		"read.last", rl.label.Format(time.DateTime),
		"tps", common.DecimalToFixed(countSnap.Rate1(), 0),
		"bps", humanize.Bytes(uint64(sizeSnap.Rate1())),
		"total.bytes", humanize.Bytes(uint64(sizeSnap.Count())),
		"running", time.Since(rl.started).Round(time.Second))
}

func (rl *tickScanMeter) stop() {
	if rl == nil || rl.ticker == nil {
		return
	}
	rl.ticker.Stop()
	rl.countMeter.Stop()
	rl.sizeMeter.Stop()
}

// ScanLinesBatchingCats returns a buffered channel (of 'workers' size)
// of same-cat track line batches.
func ScanLinesBatchingCats(reader io.Reader, quit <-chan struct{}, batchSize int, workers int) (<-chan [][]byte, chan error) {
	if workers == 0 {
		panic("cats too fast (refusing to send on unbuffered channel)")
	}
	output := make(chan [][]byte, workers)
	err := make(chan error, 1)

	go func(out chan [][]byte, errs chan error, reader io.Reader) {
		defer close(out)
		defer close(errs)

		readN := int64(0)
		readOnce := sync.Once{}

		catBatches := map[string][][]byte{}
		dec := json.NewDecoder(reader)

		met := newTickScanMeter(5 * time.Second)

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

			readOnce.Do(func() {
				slog.Info("Reading tracks")
				met.started = time.Now()
			})

			readN++
			t := gjson.GetBytes(msg, "properties.Time")
			if !t.Exists() {
				errs <- fmt.Errorf("missing properties.Time in line: %s", string(msg))
				continue
			}
			met.mark(t.Time(), msg)

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

		met.stop()

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

var ErrMissingAttribute = errors.New("missing attribute in read line")

func ScanLinesUnbatchedCats(reader io.Reader, quit <-chan struct{}, workersN, bufferN, closeCatAfterInt int) (chan chan []byte, chan error) {
	// FIXME: What happens if there are more cats than workersN?
	// Will the scanner ever free itself from the cat race?
	// CHECKME. Leaky faucet somewhere. Smells in here.
	catChCh := make(chan chan []byte, workersN)
	errs := make(chan error, 1)
	go func() {
		defer close(errs)
		defer close(catChCh)
		dec := json.NewDecoder(reader)

		// A cat not seen in this many lines will have their channel closed.
		// Upon meeting this cat later, a new channel will be opened.
		closeCatAfter := uint64(closeCatAfterInt)
		// A map of cat:line_index.
		catLastMap := sync.Map{}
		// A map of cat:channel.
		catChMap := sync.Map{}
		defer catChMap.Range(func(key, value interface{}) bool {
			close(value.(chan []byte))
			return true
		})

		met := newTickScanMeter(5 * time.Second)
		defer met.stop()

		catCount := 0
		defer func() {
			total := met.countMeter.Snapshot().Count()
			slog.Info("Unbatcher done", "catCount", catCount,
				"lines", humanize.Comma(total), "running", time.Since(met.started).Round(time.Second))
		}()
		for {
			select {
			case <-quit:
				slog.Info("Unbatcher quitting")
				break
			default:
			}
			msg := json.RawMessage{}
			err := dec.Decode(&msg)
			if err != nil {
				if errors.Is(err, io.EOF) {
					slog.Info("Unbatcher EOF")
					break
				}
				sendErr(errs, fmt.Errorf("scanner(%w)", err))
				return
			}

			t := gjson.GetBytes(msg, "properties.Time")
			if !t.Exists() {
				sendErr(errs, fmt.Errorf("%w: properties.Time in line: %s", ErrMissingAttribute, string(msg)))
				continue
			}

			met.mark(t.Time(), msg)

			cat := gjson.GetBytes(msg, "properties.Name").String()
			if cat == "" {
				sendErr(errs, fmt.Errorf("%w: properties.Name in line: %s", ErrMissingAttribute, string(msg)))
				continue
			}

			catID := conceptual.CatID(names.AliasOrSanitizedName(cat))

			// Every once in bufferN, check to see if there are cats we haven't seen tracks from since last.
			// For these expired cats, close their chans to free up resources, and make way for more cats.

			n := met.nn.Load()

			if n%closeCatAfter == 0 {
				expired := []conceptual.CatID{}
				catLastMap.Range(func(catID, last interface{}) bool {
					if n-last.(uint64) > closeCatAfter {
						expired = append(expired, catID.(conceptual.CatID))
					}
					return true
				})
				for _, catID := range expired {
					slog.Warn(fmt.Sprintf("👋 Unbatcher cat not seen in %d lines", closeCatAfter), "cat", catID)
					v, loaded := catChMap.LoadAndDelete(catID)
					if !loaded {
						panic("where is cat")
					}
					close(v.(chan []byte))

					// This is the single most important line of code in the whole program.
					v = nil
					catLastMap.Delete(catID)
				}
			}
			catLastMap.Store(catID, n)

			v, loaded := catChMap.LoadOrStore(catID, make(chan []byte, bufferN))
			if loaded {
				v.(chan []byte) <- msg
				continue
			}

			ct := cattrack.CatTrack{}
			err = json.Unmarshal(msg, &ct)
			if err != nil {
				sendErr(errs, fmt.Errorf("cat(%s) unmarshal error: %w", catID, err))
				return
			}
			slog.Info("🐈 Unbatcher fresh cat", "cat", catID, "track", ct.StringPretty())
			v.(chan []byte) <- msg

			// If catChCh is buffered (workersN), this will block until a worker is available.
			// If catChCh is unbuffered, this will block until a worker is available,
			// but it might stack lots of cats very high... if there are many cats.
			catChCh <- v.(chan []byte)
			catCount++
		}
	}()
	return catChCh, errs
}

//func readCat(catId string, lines chan []byte, bufferN int, quit chan struct{}) (chan []byte, chan error) {
//	catCh := make(chan []byte, bufferN)
//	errCh := make(chan error, 1)
//	go func() {
//		defer close(catCh)
//		defer close(errCh)
//		// read catReader and send []byte to cat chan
//		catDec := json.NewDecoder(reader)
//		for line := range lines {
//			select {
//			case <-quit:
//				return
//			default:
//			}
//			msg := json.RawMessage{}
//			err := catDec.Decode(&msg)
//			if err != nil {
//				if errors.Is(err, io.EOF) {
//					return
//				}
//				sendErr(errCh, fmt.Errorf("%s(%w): %s", catID, err, string(msg)))
//				return
//			}
//			name := gjson.GetBytes(msg, "properties.Name").String()
//			if name == "" {
//				sendErr(errCh, fmt.Errorf("cat(%s) missing properties.Name in line: %s", catID, string(msg)))
//				return
//			}
//			if catId != names.AliasOrSanitizedName(name) {
//				continue
//			}
//			catCh <- msg
//		}
//	}()
//	return catCh, errCh
//}

/*
panic: runtime error: invalid memory address or nil pointer dereference
[signal SIGSEGV: segmentation violation code=0x1 addr=0x0 pc=0x5b58f1]

goroutine 14 [running]:
time.(*Ticker).Stop(...)
        /home/ia/go1.22.2.linux-amd64/go/src/time/tick.go:45
github.com/rotblauer/catd/stream.(*tickScanMeter).done(...)
        /home/ia/dev/rotblauer/catd/stream/batch_cats.go:47
github.com/rotblauer/catd/stream.ScanLinesBatchingCats.func1(0xc00005e780, 0xc000032660, {0xb3c880, 0xc00011a018})
        /home/ia/dev/rotblauer/catd/stream/batch_cats.go:176 +0xc71
created by github.com/rotblauer/catd/stream.ScanLinesBatchingCats in goroutine 1
        /home/ia/dev/rotblauer/catd/stream/batch_cats.go:89 +0x255

*/
