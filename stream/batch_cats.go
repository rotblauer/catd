package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/names"
	"github.com/rotblauer/catd/types/cattrack"
	"github.com/tidwall/gjson"
	"io"
	"log"
	"log/slog"
	"math"
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

		tlogger := &readTrackLogger{
			interval: 5 * time.Second,
		}

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

			readN++

			readOnce.Do(func() {
				slog.Info("Reading tracks")
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

type catSeeker struct {
	reader io.Reader
	dec    *json.Decoder
	dict   *sync.Map
	cat    string
	send   chan []byte
	errs   chan error
	quit   chan struct{}
}

//func (cs *catSeeker) seek(cat string) {
//	cs.cat = cat
//
//	for {
//		select {
//		case <-cs.quit:
//			return
//		default:
//		}
//		msg := json.RawMessage{}
//		err := cs.dec.Decode(&msg)
//		if err != nil {
//			if errors.Is(err, io.EOF) {
//				return
//			}
//			sendErr(cs.errs, fmt.Errorf("scanner(%w)", err))
//			return
//		}
//		name := gjson.GetBytes(msg, "properties.Name").String()
//		if name == "" {
//			sendErr(cs.errs, fmt.Errorf("cat(%s) missing properties.Name in line: %s", cat, string(msg)))
//			return
//		}
//		// we are the seeker
//		if cs.cat == "" {
//			// lookup cat in hat
//			alias := names.AliasOrSanitizedName(name)
//			if _, loaded := cs.dict.LoadOrStore(alias, struct{}{}); loaded {
//				continue // questing fresh cats
//			}
//			// fresh cat
//
//			r, w := io.Pipe()
//			go io.TeeReader(cs.reader, w)
//			catMatcher := &catSeeker{
//				// Tee reader
//				reader: r,
//				dec:    json.NewDecoder(r),
//				dict:   cs.dict,
//				cat:    alias,
//				send:   cs.send,
//				errs:   cs.errs,
//				quit:   cs.quit,
//			}
//		} else {
//			// we are cat herder
//			if cs.cat != names.AliasOrSanitizedName(name) {
//				continue
//			}
//		}
//
//		cs.send <- msg
//	}
//}

func ScanLinesUnbatchedCats(reader io.Reader, quit <-chan struct{}, workersN, bufferN int) (chan chan []byte, chan error) {
	catChCh := make(chan chan []byte, workersN)
	errs := make(chan error, 1)
	go func() {
		defer close(errs)
		defer close(catChCh)
		dec := json.NewDecoder(reader)

		catLineExpiry := uint64(bufferN)
		catChMap := sync.Map{}
		catLastMap := sync.Map{}

		//catMapOpts := ttlcache.WithTTL[conceptual.CatID, struct{}](10 * time.Second)
		//catTTL := ttlcache.New[conceptual.CatID, struct{}](catMapOpts)
		//catTTL.OnEviction(func(ctx context.Context,
		//	reason ttlcache.EvictionReason, i *ttlcache.Item[conceptual.CatID, struct{}]) {
		//	slog.Info("👋 Unbatcher evicting cat", "cat", i.Key())
		//	_, loaded := catChMap.LoadAndDelete(i.Key())
		//	if !loaded {
		//		panic("cat not found in map")
		//	}
		//	//close(v.(chan []byte))
		//})
		//go catTTL.Start()

		catCount := 0
		tlogger := &readTrackLogger{
			interval: 5 * time.Second,
		}
		tlogger.started = time.Now()
		tlogger.n.Store(0)
		go tlogger.run()
		defer tlogger.done()
		defer func() {
			slog.Info("Unbatcher done", "catCount", catCount, "lines", tlogger.n.Load())
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
			tlogger.mark(gjson.GetBytes(msg, "properties.Time").String())
			cat := gjson.GetBytes(msg, "properties.Name").String()
			if cat == "" {
				sendErr(errs, fmt.Errorf("[scanner] missing properties.Name in line: %s", string(msg)))
				return
			}
			catID := conceptual.CatID(names.AliasOrSanitizedName(cat))

			// Every once in a while, check to see if there are cats we haven't seen tracks from in a while.
			// For these expired cats, close their chans to free up resources.
			if n := tlogger.n.Load(); n%catLineExpiry == 0 {
				expired := []conceptual.CatID{}
				catLastMap.Range(func(catID, last interface{}) bool {
					if n-last.(uint64) > catLineExpiry {
						expired = append(expired, catID.(conceptual.CatID))
					}
					return true
				})
				for _, catID := range expired {
					slog.Warn(fmt.Sprintf("👋 Unbatcher cat not seen in %d lines", catLineExpiry), "cat", catID)
					v, loaded := catChMap.LoadAndDelete(catID)
					if !loaded {
						panic("where is cat")
					}
					close(v.(chan []byte))
					catLastMap.Delete(catID)
				}
			}

			catLastMap.Store(catID, tlogger.n.Load())
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
			catChCh <- v.(chan []byte)
			catCount++
		}
		//catTTL.Stop()
		//catTTL.Range(func(item *ttlcache.Item[conceptual.CatID, struct{}]) bool {
		//	return true
		//})
		catChMap.Range(func(key, value interface{}) bool {
			close(value.(chan []byte))
			return true
		})
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
github.com/rotblauer/catd/stream.(*readTrackLogger).done(...)
        /home/ia/dev/rotblauer/catd/stream/batch_cats.go:47
github.com/rotblauer/catd/stream.ScanLinesBatchingCats.func1(0xc00005e780, 0xc000032660, {0xb3c880, 0xc00011a018})
        /home/ia/dev/rotblauer/catd/stream/batch_cats.go:176 +0xc71
created by github.com/rotblauer/catd/stream.ScanLinesBatchingCats in goroutine 1
        /home/ia/dev/rotblauer/catd/stream/batch_cats.go:89 +0x255

*/
