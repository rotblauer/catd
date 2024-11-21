package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/paulmach/orb/geojson"
	"github.com/rotblauer/catd/types/cattrack"
	"github.com/rotblauer/catd/types/trackpoint"
	"github.com/tidwall/gjson"
)

var ErrDecodeTracks = fmt.Errorf("could not decode as trackpoints or geojson or geojsonfc or ndtrackpoints or ndgeojson")

// DecodeTrackPoints is a function that attempts to decode a byte slice into a slice of TrackPoints.
// An error will be returned if the unmarshaling fails,
// OR if the resulting slice is empty,
// OR if the first trackpoint in the slice has a zero Time field.
// A Time value is required for all cat tracks (whether TrackPoints or GeoJSON JSONBytes),
// and an unmarshal of a geojson slice will not fill the flat Time field of a TrackPoint,
// since the geojson will locate the Time field in the properties of the feature; '.properties.Time' vs. '.time'.
func DecodeTrackPoints(data []byte) (trackpoint.TrackPoints, error) {
	trackPoints := trackpoint.TrackPoints{}
	if err := json.Unmarshal(data, &trackPoints); err != nil {
		return nil, err
	}
	if len(trackPoints) > 0 {
		if trackPoints[0].Time.IsZero() {
			return nil, errors.New("invalid trackpoint (missing or zero 'time' field)")
		}
		return trackPoints, nil
	}
	return nil, errors.New("empty trackpoints")
}

// DecodeCatTracksShotgun is a serial collection of handy-bandy attempts
// to turn the input data into a slice of geojson features.
// This is useful for a legacy-supporting API.
func DecodeCatTracksShotgun(data []byte) (out []*cattrack.CatTrack, err error) {

	// Is it a geojson.FeatureCollection object?
	// https://datatracker.ietf.org/doc/html/rfc7946#section-3.3
	// > A GeoJSON object with the type "FeatureCollection" is a
	// > FeatureCollection object.  A FeatureCollection object has a member
	// > with the name "features".  The value of "features" is a JSON array.
	// > Each element of the array is a Feature object as defined above.  It
	// > is possible for this array to be empty.
	if res := gjson.GetBytes(data, "features"); res.Exists() {
		gjfc := geojson.NewFeatureCollection()
		err = gjfc.UnmarshalJSON(data)
		if err != nil {
			return nil, err
		}
		for _, f := range gjfc.Features {
			ct := cattrack.CatTrack(*f)
			out = append(out, &ct)
		}
		return out, nil
	}

	parsed := gjson.ParseBytes(data)
	if !parsed.IsArray() {
		return nil, fmt.Errorf("unknown data type (not an array)")
	}

	arr := parsed.Array()
	if len(arr) == 0 {
		return nil, fmt.Errorf("empty set")
	}

	for _, el := range arr {
		if !el.IsObject() {
			return nil, fmt.Errorf("non-object element in array")
		}

		// Is it a legacy TrackPoint?
		tp := &trackpoint.TrackPoint{}
		err = json.Unmarshal([]byte(el.Raw), tp)
		if err == nil {
			// Convert the legacy trackpoint to a geojson feature.
			gj := TrackToFeature(tp)
			ct := cattrack.CatTrack(*gj)
			out = append(out, &ct)
			continue
		}

		// Is it a geojson feature?
		gj := &geojson.Feature{}
		err = json.Unmarshal([]byte(el.Raw), gj)
		if err == nil {
			ct := cattrack.CatTrack(*gj)
			out = append(out, &ct)
			continue
		}
	}
	return
}
