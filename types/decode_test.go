package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rotblauer/catd/types/cattrack"
	"testing"
)

func TestScanMessages(t *testing.T) {
	for _, data := range []decodeTestCase{
		featuresNDJSON_T,
		featuresArrayCompact_T,
		featuresArrayIndented_T,
		trackpointsNDJSON_T,
		trackpointsJSONCompact_T,
		trackpointsJSONIndented_T,
		featureCollectionIndented_T,
		featureCollectionCompactArray_T,
		featureCollectionIndentedArray_T,
		empty_T,
		nil_T,
		malformed_T,
		edge_1000_T,
	} {
		t.Run(data.name, testScanMessages(data))
	}
}

func testScanMessages(c decodeTestCase) func(t *testing.T) {
	return func(t *testing.T) {
		d := bytes.NewBuffer(c.input)
		msgs := []json.RawMessage{}
		err := ScanJSONMessages(d, func(message json.RawMessage) error {
			msgs = append(msgs, message)
			return nil
		})
		checkDecodeError(t, c, err)
		if c.expectError != nil {
			return
		}

		// If only one message returned, must be feature collection.
		if len(msgs) != c.expectScanMessages {
			t.Errorf("returned %d messages, expected %d", len(msgs), c.expectScanMessages)
		}
		for i, msg := range msgs {
			if i == 5 {
				t.Logf("...")
				break
			}
			t.Log(loggingMustStringify(msg))
		}
	}
}

func TestDecodingTrackObject(t *testing.T) {
	for _, data := range []decodeTestCase{
		featuresNDJSON_T,
		featuresArrayCompact_T,
		featuresArrayIndented_T,
		trackpointsNDJSON_T,
		trackpointsJSONCompact_T,
		trackpointsJSONIndented_T,
		featureCollectionIndented_T,
		featureCollectionCompactArray_T,
		featureCollectionIndentedArray_T,
		empty_T,
		nil_T,
		malformed_T,
		edge_1000_T,
	} {
		t.Run(data.name, testDecodingTrackObject(data))
	}
}

func testDecodingTrackObject(c decodeTestCase) func(t *testing.T) {
	return func(t *testing.T) {
		count := 0
		err := ScanJSONMessages(bytes.NewBuffer(c.input), func(message json.RawMessage) error {
			return DecodingJSONTrackObject(message, func(ct *cattrack.CatTrack) error {
				if !ct.IsValid() {
					t.Errorf("invalid track: %v", ct)
				}
				count++
				if count <= 5 {
					t.Log(loggingMustStringify(ct))
				} else if count == 6 {
					t.Logf("...")
				}
				return nil
			})
		})
		checkDecodeError(t, c, err)
		if c.expectError != nil {
			return
		}
		if count != c.expectDecodeMessages {
			t.Fatalf("returned %d tracks, expected %d", count, c.expectDecodeMessages)
		}
	}
}

func checkDecodeError(t *testing.T, c decodeTestCase, err error) {
	if c.expectError != nil {
		if err == nil {
			t.Fatalf("wanted error: %v (got: nil)", c.expectError)
		}
		var serr *json.SyntaxError
		if errors.As(c.expectError, &serr) {
			if !errors.As(err, &serr) {
				t.Fatalf("returned error: %v", err)
			}
		} else {
			if !errors.Is(err, c.expectError) {
				t.Fatalf("returned error: %v", err)
			}
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func loggingMustStringify(msg any) string {
	j, _ := json.Marshal(msg)
	return fmt.Sprintf("%s", j)
}

func TestDecodeCatTracksShotgun(t *testing.T) {
	for i, data := range []decodeTestCase{
		featureCollectionIndented_T,
		featureCollectionCompact_T,
		trackpointsJSONIndented_T,
		trackpointsJSONCompact_T,
		// Not supported:
		//featuresNDJSON_T,
		//trackpointsNDJSON_T,
	} {
		tracks, err := DecodeCatTracksShotgun(data.input)
		if err != nil {
			t.Errorf("%d DecodeCatTracksShotgun(%v)\nreturned error: %v", i, data.name, err)
		}
		if len(tracks) != 2 {
			t.Fatalf("%d DecodeCatTracksShotgun(%v)\nreturned %d tracks, expected 2", i, data.name, len(tracks))
		}
		for _, track := range tracks {
			if !track.IsValid() {
				t.Errorf("%d DecodeCatTracksShotgun(%v)\nreturned invalid track: %v", i, data.name, track)
			}
		}
	}
}
