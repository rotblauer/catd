package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/testing/testdata"
	"github.com/rotblauer/catd/types/cattrack"
	"io"
	"testing"
)

type decodeTestCase struct {
	name                 string
	input                []byte
	expectScanMessages   int
	expectDecodeMessages int
	expectError          error
}

var gf1 = `{"type":"Feature","properties":{"UUID":"76170e959f967f40","Name":"ranga-moto-act3","Time":"2024-12-20T22:19:53.713Z","UnixTime":1734733193,"Version":"gcps/v0.0.0+4","Speed":0.18,"Elevation":1258.4,"Heading":270,"Accuracy":4.1,"vAccuracy":2,"speed_accuracy":2.8,"heading_accuracy":80,"Activity":"Stationary","ActivityConfidence":100,"BatteryLevel":0.94,"BatteryStatus":"unplugged","CurrentTripStart":"2024-12-20T19:50:39.904386Z","NumberOfSteps":13861,"Pressure":null,"Lightmeter":0,"AmbientTemp":null,"Distance":11592,"AccelerometerX":0.49,"AccelerometerY":-1,"AccelerometerZ":9.89,"UserAccelerometerX":0,"UserAccelerometerY":-0.03,"UserAccelerometerZ":0.13,"GyroscopeX":0,"GyroscopeY":0,"GyroscopeZ":0},"geometry":{"type":"Point","coordinates":[-113.4733911,47.178916]},"bbox":[-113.4733911,47.178916,-113.4733911,47.178916]}`
var gf2 = `{"type":"Feature","properties":{"UUID":"76170e959f967f40","Name":"ranga-moto-act3","Time":"2024-12-20T22:19:54.713Z","UnixTime":1734733194,"Version":"gcps/v0.0.0+4","Speed":0.18,"Elevation":1258.4,"Heading":270,"Accuracy":4,"vAccuracy":2,"speed_accuracy":2.9,"heading_accuracy":78,"Activity":"Stationary","ActivityConfidence":100,"BatteryLevel":0.94,"BatteryStatus":"unplugged","CurrentTripStart":"2024-12-20T19:50:39.904386Z","NumberOfSteps":13861,"Pressure":null,"Lightmeter":0,"AmbientTemp":null,"Distance":11592,"AccelerometerX":0.49,"AccelerometerY":-0.98,"AccelerometerZ":9.94,"UserAccelerometerX":0,"UserAccelerometerY":-0.01,"UserAccelerometerZ":0.18,"GyroscopeX":0,"GyroscopeY":0,"GyroscopeZ":0},"geometry":{"type":"Point","coordinates":[-113.473419,47.1788913]},"bbox":[-113.473419,47.1788913,-113.473419,47.1788913]}`
var tp1 = `{"heading":-1,"speed":-1,"uuid":"5D37B5DA-6E0B-41FE-8A72-2BB681D661DA","version":"V.customizableCatTrackHat","long":-93.255531311035156,"time":"2024-12-20T22:09:01.458Z","elevation":322.59848022460938,"notes":"{\"floorsAscended\":23,\"customNote\":\"\",\"heartRateS\":\"81 count\\\/min\",\"currentTripStart\":\"2024-12-16T00:29:02.773Z\",\"floorsDescended\":19,\"averageActivePace\":0.44561766736099268,\"networkInfo\":\"{\\\"ssidData\\\":\\\"{length = 12, bytes = 0x42616e616e6120486f74656c}\\\",\\\"bssid\\\":\\\"6c:70:9f:de:59:89\\\",\\\"ssid\\\":\\\"Banana Hotel\\\"}\",\"numberOfSteps\":28854,\"visit\":\"{\\\"validVisit\\\":false}\",\"relativeAltitude\":-177.529541015625,\"currentCadence\":1.5344011783599854,\"heartRateRawS\":\"8B4DD1AC-89CC-40E2-BEBB-3E7E5880AB66 81 count\\\/min 8B4DD1AC-89CC-40E2-BEBB-3E7E5880AB66, (2), \\\"iPhone17,1\\\" (18.1.1) (2024-12-20 08:22:00 -0600 - 2024-12-20 08:22:00 -0600)\",\"batteryStatus\":\"{\\\"level\\\":0.60000002384185791,\\\"status\\\":\\\"unplugged\\\"}\",\"activity\":\"Stationary\",\"currentPace\":1.1007883548736572,\"imgb64\":\"\",\"pressure\":99.300796508789062,\"distance\":33449.814082269906}","lat":44.988998413085938,"accuracy":3.800194263458252,"name":"Rye16"}`
var tp2 = `{"heading":-1,"speed":-1,"uuid":"5D37B5DA-6E0B-41FE-8A72-2BB681D661DA","version":"V.customizableCatTrackHat","long":-93.255531311035156,"time":"2024-12-20T22:09:06.964Z","elevation":322.59832763671875,"notes":"{\"floorsAscended\":23,\"customNote\":\"\",\"heartRateS\":\"81 count\\\/min\",\"currentTripStart\":\"2024-12-16T00:29:02.773Z\",\"floorsDescended\":19,\"averageActivePace\":0.44561766736099268,\"networkInfo\":\"{\\\"ssidData\\\":\\\"{length = 12, bytes = 0x42616e616e6120486f74656c}\\\",\\\"bssid\\\":\\\"6c:70:9f:de:59:89\\\",\\\"ssid\\\":\\\"Banana Hotel\\\"}\",\"numberOfSteps\":28854,\"visit\":\"{\\\"validVisit\\\":false}\",\"relativeAltitude\":-177.38421630859375,\"currentCadence\":1.5344011783599854,\"heartRateRawS\":\"8B4DD1AC-89CC-40E2-BEBB-3E7E5880AB66 81 count\\\/min 8B4DD1AC-89CC-40E2-BEBB-3E7E5880AB66, (2), \\\"iPhone17,1\\\" (18.1.1) (2024-12-20 08:22:00 -0600 - 2024-12-20 08:22:00 -0600)\",\"batteryStatus\":\"{\\\"level\\\":0.60000002384185791,\\\"status\\\":\\\"unplugged\\\"}\",\"activity\":\"Stationary\",\"currentPace\":1.1007883548736572,\"imgb64\":\"\",\"pressure\":99.299102783203125,\"distance\":33449.814082269906}","lat":44.988998413085938,"accuracy":3.7969114780426025,"name":"Rye16"}`

var featsNDJSON_T = decodeTestCase{
	name:                 "featsNDJSON",
	input:                []byte(fmt.Sprintf("%s\n%s\n", gf1, gf2)),
	expectScanMessages:   2,
	expectDecodeMessages: 2,
}
var featsArrayCompact_T = decodeTestCase{
	name:                 "featsArrayCompact",
	input:                []byte(fmt.Sprintf("[%s,%s]", gf1, gf2)),
	expectScanMessages:   2,
	expectDecodeMessages: 2,
}
var featsArrayIndented_T = decodeTestCase{
	name:                 "featsArrayIndented",
	input:                []byte(fmt.Sprintf("[\n\t%s,\n\t%s\n]\n", gf1, gf2)),
	expectScanMessages:   2,
	expectDecodeMessages: 2,
}
var trackpointsNDJSON_T = decodeTestCase{
	name:                 "trackpointsNDJSON",
	input:                []byte(fmt.Sprintf("%s\n%s\n", tp1, tp2)),
	expectScanMessages:   2,
	expectDecodeMessages: 2,
}
var trackpointsJSONCompact_T = decodeTestCase{
	name:                 "trackpointsJSONCompact",
	input:                []byte(fmt.Sprintf("[%s,%s]", tp1, tp2)),
	expectScanMessages:   2,
	expectDecodeMessages: 2,
}
var trackpointsJSONIndented_T = decodeTestCase{
	name:                 "trackpointsJSONIndented",
	input:                []byte(fmt.Sprintf("[\n\t%s,\n\t%s\n]\n", tp1, tp2)),
	expectScanMessages:   2,
	expectDecodeMessages: 2,
}
var geojsonFeatureCollectionIndented_T = decodeTestCase{
	name: "geojsonFeatureCollectionIndented",
	input: []byte(fmt.Sprintf(`{
	"type": "FeatureCollection",
	"features": [
		%s,
		%s
	]
}`, gf1, gf2)),
	expectScanMessages:   1,
	expectDecodeMessages: 2,
}
var geojsonFeatureCollectionCompact_T = decodeTestCase{
	name:                 "geojsonFeatureCollectionCompact",
	input:                []byte(`{"type": "FeatureCollection","features": [` + gf1 + "," + gf2 + `]}`),
	expectScanMessages:   1,
	expectDecodeMessages: 2,
}
var empty_T = decodeTestCase{
	name:        "empty",
	input:       []byte{},
	expectError: io.EOF,
}
var nil_T = decodeTestCase{
	name:        "nil",
	input:       nil,
	expectError: io.EOF,
}
var malformed_T = decodeTestCase{
	name:        "malformed",
	input:       []byte("malformed"),
	expectError: &json.SyntaxError{},
}
var edge_1000_T = decodeTestCase{
	name: "edge_1000",
	input: func() []byte {
		r, err := catz.NewGZFileReader(testdata.Path(testdata.Source_EDGE1000))
		if err != nil {
			panic(err)
		}
		defer r.Close()
		got, err := io.ReadAll(r)
		if err != nil {
			panic(err)
		}
		return got
	}(),
	expectScanMessages:   1000,
	expectDecodeMessages: 1000,
	expectError:          nil,
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

func TestScanMessages(t *testing.T) {
	for _, data := range []decodeTestCase{
		featsNDJSON_T,
		featsArrayCompact_T,
		featsArrayIndented_T,
		trackpointsNDJSON_T,
		trackpointsJSONCompact_T,
		trackpointsJSONIndented_T,
		geojsonFeatureCollectionIndented_T,
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
		featsNDJSON_T,
		featsArrayCompact_T,
		featsArrayIndented_T,
		trackpointsNDJSON_T,
		trackpointsJSONCompact_T,
		trackpointsJSONIndented_T,
		geojsonFeatureCollectionIndented_T,
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

func TestDecodeCatTracksShotgun(t *testing.T) {
	for i, data := range []decodeTestCase{
		geojsonFeatureCollectionIndented_T,
		geojsonFeatureCollectionCompact_T,
		trackpointsJSONIndented_T,
		trackpointsJSONCompact_T,
		// Not supported:
		//featsNDJSON_T,
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
