package types

import (
	"encoding/json"
	"fmt"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/testing/testdata"
	"io"
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

var featuresNDJSON_T = decodeTestCase{
	name:                 "featsNDJSON",
	expectScanMessages:   2,
	expectDecodeMessages: 2,
	input:                []byte(fmt.Sprintf("%s\n%s\n", gf1, gf2)),
}

var featuresArrayCompact_T = decodeTestCase{
	name:                 "featsArrayCompact",
	expectScanMessages:   2,
	expectDecodeMessages: 2,
	input:                []byte(fmt.Sprintf("[%s,%s]", gf1, gf2)),
}

var featuresArrayIndented_T = decodeTestCase{
	name:                 "featsArrayIndented",
	expectScanMessages:   2,
	expectDecodeMessages: 2,
	input:                []byte(fmt.Sprintf("[\n\t%s,\n\t%s\n]\n", gf1, gf2)),
}

var trackpointsNDJSON_T = decodeTestCase{
	name:                 "trackpointsNDJSON",
	expectScanMessages:   2,
	expectDecodeMessages: 2,
	input:                []byte(fmt.Sprintf("%s\n%s\n", tp1, tp2)),
}

var trackpointsJSONCompact_T = decodeTestCase{
	name:                 "trackpointsJSONCompact",
	expectScanMessages:   2,
	expectDecodeMessages: 2,
	input:                []byte(fmt.Sprintf("[%s,%s]", tp1, tp2)),
}

var trackpointsJSONIndented_T = decodeTestCase{
	name:                 "trackpointsJSONIndented",
	expectScanMessages:   2,
	expectDecodeMessages: 2,
	input:                []byte(fmt.Sprintf("[\n\t%s,\n\t%s\n]\n", tp1, tp2)),
}

var featureCollectionIndented_T = decodeTestCase{
	name:                 "geojsonFeatureCollectionIndented",
	expectScanMessages:   1, // Only one message scanned (one obj).
	expectDecodeMessages: 2,
	input: []byte(fmt.Sprintf(`{
	"type": "FeatureCollection",
	"features": [
		%s,
		%s
	]
}`, gf1, gf2)),
}

var featureCollectionCompact_T = decodeTestCase{
	name:                 "geojsonFeatureCollectionCompact",
	expectScanMessages:   1, // Only one message scanned (one obj).
	expectDecodeMessages: 2,
	input:                []byte(`{"type": "FeatureCollection","features": [` + gf1 + "," + gf2 + `]}`),
}

var featureCollectionCompactArray_T = decodeTestCase{
	name:                 "geojsonFeatureCollectionCompactArray",
	expectScanMessages:   2,
	expectDecodeMessages: 4,
	input:                []byte(`[{"type": "FeatureCollection","features": [` + gf1 + "," + gf2 + `]},{"type": "FeatureCollection","features": [` + gf1 + "," + gf2 + `]}]`),
}

var featureCollectionIndentedArray_T = decodeTestCase{
	name:                 "geojsonFeatureCollectionIndentedArray",
	expectScanMessages:   2,
	expectDecodeMessages: 4,
	input: []byte(`[
	{"type": "FeatureCollection","features": [` + gf1 + "," + gf2 + `]},
	{"type": "FeatureCollection","features": [` + gf1 + "," + gf2 + `]}
]`),
}

var edge_1000_T = decodeTestCase{
	name:                 "edge_1000",
	expectScanMessages:   1000,
	expectDecodeMessages: 1000,
	expectError:          nil,
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
}
