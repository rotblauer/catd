package trackpoint

import "testing"

var trackpointJSONValid = `{
  "heading": 310.8944091796875,
  "speed": 1.1056904792785645,
  "uuid": "5D37B5DA-6E0B-41FE-8A72-2BB681D661DA",
  "version": "V.customizableCatTrackHat",
  "long": -93.259307861328125,
  "time": "2024-11-15T22:57:43.999Z",
  "elevation": 246.0128173828125,
  "notes": "{\"floorsAscended\":33,\"customNote\":\"\",\"heartRateS\":\"76 count\\\/min\",\"currentTripStart\":\"2024-11-12T13:15:26.996Z\",\"floorsDescended\":38,\"averageActivePace\":0.40198381066403732,\"networkInfo\":\"{}\",\"numberOfSteps\":40581,\"visit\":\"{\\\"validVisit\\\":false}\",\"relativeAltitude\":-8.6324615478515625,\"currentCadence\":1.7470487356185913,\"heartRateRawS\":\"89DC00DF-0A03-461C-AB25-15716F11C927 76 count\\\/min 89DC00DF-0A03-461C-AB25-15716F11C927, (2), \\\"iPhone17,1\\\" (18.0.1) (2024-11-15 16:30:00 -0600 - 2024-11-15 16:30:00 -0600)\",\"batteryStatus\":\"{\\\"level\\\":0.40000000596046448,\\\"status\\\":\\\"unplugged\\\"}\",\"activity\":\"Walking\",\"currentPace\":0.64149290323257446,\"imgb64\":\"\",\"pressure\":98.721549987792969,\"distance\":48118.658915759355}",
  "lat": 44.985164642333984,
  "pushToken": "b1874b7923da4dbded73e3097c0de4d154b462feacf5eee22b7e6fef2ecf38f3",
  "accuracy": 4.2884750366210938,
  "name": "Rye16"
}`

func TestTrackPoint_UnmarshalJSON1(t *testing.T) {
	tp := &TrackPoint{}
	err := tp.UnmarshalJSON([]byte(trackpointJSONValid))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tp.Uuid != "5D37B5DA-6E0B-41FE-8A72-2BB681D661DA" {
		t.Errorf("expected UUID to be '5D37B5DA-6E0B-41FE-8A72-2BB681D661DA', got %q", tp.Uuid)
	}
	if tp.Name != "Rye16" {
		t.Errorf("expected Name to be 'Rye16', got %q", tp.Name)
	}
	if tp.Time.String() != "2024-11-15 22:57:43.999 +0000 UTC" {
		t.Errorf("expected Time to be '2024-11-15 22:57:43.999 +0000 UTC', got %v", tp.Time)
	}
	if tp.Speed != 1.1056904792785645 {
		t.Errorf("expected Speed to be 1.1056904792785645, got %v", tp.Speed)
	}
	if tp.Elevation != 246.0128173828125 {
		t.Errorf("expected Elevation to be 246.0128173828125, got %v", tp.Elevation)
	}
	if tp.Lng != -93.259307861328125 {
		t.Errorf("expected Long to be -93.259307861328125, got %v", tp.Lng)
	}
	if tp.Lat != 44.985164642333984 {
		t.Errorf("expected Lat to be 44.985164642333984, got %v", tp.Lat)
	}
	if tp.Accuracy != 4.2884750366210938 {
		t.Errorf("expected Accuracy to be 4.2884750366210938, got %v", tp.Accuracy)
	}
	if tp.Heading != 310.8944091796875 {
		t.Errorf("expected Heading to be 310.8944091796875, got %v", tp.Heading)
	}
	if tp.Version != "V.customizableCatTrackHat" {
		t.Errorf("expected Version to be 'V.customizableCatTrackHat', got %q", tp.Version)
	}
}

var featureGeoJSON = `{"id":0,"type":"Feature","geometry":{"type":"Point","coordinates":[-111.6902967,45.5710024]},"properties":{"AccelerometerX":0.73,"AccelerometerY":0.54,"AccelerometerZ":9.78,"Accuracy":4.9,"Activity":"Stationary","ActivityConfidence":100,"BatteryLevel":0.93,"BatteryStatus":"unplugged","CurrentTripStart":"2024-02-03T18:55:20.824384Z","Distance":2325,"Elevation":1463.6,"GyroscopeX":0,"GyroscopeY":0,"GyroscopeZ":0,"Heading":274,"Name":"ia","NumberOfSteps":9659,"Pressure":850.49,"Speed":0.45,"Time":"2024-02-04T18:04:31.172Z","UUID":"63b2bab96ca49573","UnixTime":1707069871,"UserAccelerometerX":0,"UserAccelerometerY":0.01,"UserAccelerometerZ":0.03,"Version":"gcps/v0.0.0+4","vAccuracy":1.4}}`

// TestTrackPoint_UnmarshalJSON2 tests the TrackPoint.UnmarshalJSON method
// will return an error when attempting to unmarshal a GeoJSON Feature.
func TestTrackPoint_UnmarshalJSON2(t *testing.T) {
	tp := &TrackPoint{}
	err := tp.UnmarshalJSON([]byte(featureGeoJSON))
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}
