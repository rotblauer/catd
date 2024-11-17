package trackpoint

import (
	"encoding/json"
	"time"
)

// TrackPoint Stores a snippet of life, love, and location
type TrackPoint struct {
	Uuid            string    `json:"uuid"`
	PushToken       string    `json:"pushToken"`
	Version         string    `json:"version"`
	ID              int64     `json:"id"` // either bolt auto id or unixnano //think nano is better cuz can check for dupery
	Name            string    `json:"name"`
	Lat             float64   `json:"lat"`
	Lng             float64   `json:"long"`
	Accuracy        float64   `json:"accuracy"`       // horizontal, in meters
	VAccuracy       float64   `json:"vAccuracy"`      // vertical, in meteres
	Elevation       float64   `json:"elevation"`      // in meters
	Speed           float64   `json:"speed"`          // in m/s
	SpeedAccuracy   float64   `json:"speed_accuracy"` // in meters per second
	Tilt            float64   `json:"tilt"`           // degrees?
	Heading         float64   `json:"heading"`        // in degrees
	HeadingAccuracy float64   `json:"heading_accuracy"`
	HeartRate       float64   `json:"heartrate"` // bpm
	Time            time.Time `json:"time"`
	Floor           int       `json:"floor"` // building floor if available
	Notes           string    `json:"notes"` // special events of the day
	COVerified      bool      `json:"COVerified"`
	RemoteAddr      string    `json:"remoteaddr"`
}

// UnmarshalJSON is a custom unmarshaler for TrackPoint.
// It asserts that the Time field is a valid RFC3339 time.
// If this method attempts to unmarshal data which is actually a GeoJSON Feature
// it will fail, as the GeoJSON Feature will not have a flat Time field.
func (tp *TrackPoint) UnmarshalJSON(data []byte) error {
	type Alias TrackPoint
	aux := &struct {
		Time string `json:"time"`
		*Alias
	}{
		Alias: (*Alias)(tp),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	var err error
	tp.Time, err = time.Parse(time.RFC3339, aux.Time)
	if err != nil {
		return err
	}
	return nil
}

type TrackPoints []*TrackPoint
