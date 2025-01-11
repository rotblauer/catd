package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/names"
	"github.com/rotblauer/catd/types/cattrack"
	"github.com/tidwall/gjson"
	"io"
	"log"
	"log/slog"
	"sync"
	"time"
)

const AttrName = "properties.Name"
const AttrTime = "properties.Time"

var ErrMissingAttribute = errors.New("missing attribute in read line")

type CatCh struct {
	ID string
	Ch chan []byte
}

// ScanLinesUnbatchedCats reads a stream of lines from reader, and sends them to a channel of (raw bytes)/cat channels.
// The cat channels are buffered, and will be closed after closeCatAfterInt lines of incativity.
// The cat channels are sent to workersN cat-workers, who will process the tracks.
// Each cat should have one worker.
// The quit channel should be used to interrupt the read loop.
func ScanLinesUnbatchedCats(reader io.Reader, quit <-chan struct{},
	workersN, catChannelCap, catStaleInt, catMaxInt int, whitelistCats []conceptual.CatID) (chan CatCh, chan error) {

	// FIXME: What happens if there are more cats than workersN?
	// Will the scanner ever free itself from the cat race?
	// The workaround is to use unbuffered cat channel cap, but that's not ideal in case of lots of cats.
	catChCh := make(chan CatCh, workersN)
	errs := make(chan error, 1)
	go func() {
		defer close(errs)
		defer close(catChCh)
		dec := json.NewDecoder(reader)

		// A cat not seen in this many lines will have their channel closed.
		// Upon meeting this cat later, a new channel will be opened.
		closeCatAfter := uint64(catStaleInt)
		// A map of cat:line_index, where line_index is the last line index this cat was seen on.
		catLastMap := sync.Map{}
		// A map of cat:integer, where integer is the number of messages sent on this cat's chan.
		catSentMap := map[conceptual.CatID]int{}
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
				slog.Info("Unbatcher received quit")
				return
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

			t := gjson.GetBytes(msg, AttrTime)
			if !t.Exists() {
				sendErr(errs, fmt.Errorf("%w: %s in line: %s", ErrMissingAttribute, AttrTime, string(msg)))
				continue
			}

			cat := gjson.GetBytes(msg, AttrName).String()
			if cat == "" {
				sendErr(errs, fmt.Errorf("%w: %s in line: %s", ErrMissingAttribute, AttrName, string(msg)))
				continue
			}

			met.mark(t.Time(), msg)

			catID := conceptual.CatID(names.AliasOrSanitizedName(cat))
			if len(whitelistCats) > 0 {
				if !conceptual.CatIDIn(catID, whitelistCats) {
					continue
				}
			}

			// Cat staleness check.
			// Every once in catChannelCap, check to see if there are cats we haven't seen tracks from since last.
			// For these expired cats, close their chans to free up resources, and make way for more cats.

			n := met.nn.Load()

			if n%closeCatAfter == 0 {
				// Collect any stale cats.
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
					met.dropCat(string(catID))
				}
			}

			// Store the last line index for this cat (freshen), whether we already have a channel for them or not.
			catLastMap.Store(catID, n)

			// Check cat-sent tally; if we've met the catMaxInt threshold, close the channel.
			if catSentMap[catID] >= catMaxInt {
				slog.Warn(fmt.Sprintf("👋 Unbatcher cat sent %d messages", catMaxInt), "cat", catID)
				v, loaded := catChMap.LoadAndDelete(catID)
				if !loaded {
					panic("where is cat")
				}
				close(v.(chan []byte))

				// This is the single most important line of code in the whole program.
				v = nil
				delete(catSentMap, catID)
				met.dropCat(string(catID))
			}

			// Get or create a channel for this cat.
			v, loaded := catChMap.LoadOrStore(catID, make(chan []byte, catChannelCap))
			if loaded {
				// If a cat channel exists, use it, and we're done here.
				select {
				case <-quit:
					slog.Info("Unbatcher received quit")
					return
				case v.(chan []byte) <- msg:
					catSentMap[catID]++
				}
				continue
			}

			// Otherwise, new cat.
			met.addCat(string(catID))

			ct := cattrack.CatTrack{}
			err = json.Unmarshal(msg, &ct)
			if err != nil {
				sendErr(errs, fmt.Errorf("cat(%s) unmarshal error: %w", catID, err))
				return
			}

			slog.Info("🐈 Unbatcher fresh cat", "cat", catID, "track", ct.StringPretty())

			// Send the first track.
			select {
			case <-quit:
				slog.Info("Unbatcher received quit")
				return
			case v.(chan []byte) <- msg:
				catSentMap[catID]++
			}

			// Send the channel.
			// If catChCh is buffered (workersN), this will block until a worker is available.
			// If catChCh is unbuffered, this will block until a worker is available,
			// but it might stack lots of cats very high... if there are many cats.
			select {
			case <-quit:
				slog.Info("Unbatcher received quit")
				return
			case catChCh <- CatCh{ID: cat, Ch: v.(chan []byte)}:
			}
			catCount++
		}
	}()
	return catChCh, errs
}

func sendErr(errs chan error, err error) {
	select {
	case errs <- err:
	default:
		log.Println("error channel full, dropping error", err)
	}
}

// DEPRECATED
// scanLinesBatchingCats returns a buffered channel (of 'workers' size)
// of same-cat track line batches.
func scanLinesBatchingCats(reader io.Reader, quit <-chan struct{}, batchSize int, workers int) (<-chan [][]byte, chan error) {
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
