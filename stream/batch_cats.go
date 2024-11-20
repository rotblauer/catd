package stream

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/rotblauer/catd/names"
	"github.com/tidwall/gjson"
	"io"
)

func ScanLinesBatchingCats(raw io.Reader, batchSize int, workers int) (chan [][]byte, chan error, error) {

	//myReader := bufio.NewReader(raw)
	reader := bufio.NewReaderSize(raw, 64*1024)
	ch := make(chan [][]byte, workers)
	errs := make(chan error)

	go func(ch chan [][]byte, errs chan error, contents *bufio.Reader) {

		defer func(ch chan [][]byte, errs chan error) {
			// close(ch)
			close(errs)
		}(ch, errs)

		catBatches := map[string][][]byte{}
		dec := json.NewDecoder(reader)
		for {
			msg := json.RawMessage{}
			err := dec.Decode(&msg)
			if err != nil {
				// Send remaining lines, an error expected when EOF.
				for cat, lines := range catBatches {
					if len(lines) > 0 {
						ch <- lines
						delete(catBatches, cat)
					}
				}
				errs <- err
				if err != io.EOF {
					return
				}
				continue
			}
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
		}
	}(ch, errs, reader)

	return ch, errs, nil
}
