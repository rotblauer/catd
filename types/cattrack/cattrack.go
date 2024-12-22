package cattrack

import (
	"errors"
	"fmt"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/names"
	"math"
	"strings"
	"time"
)

// CatTrack is a track a cat makes.
// It's an alias of geojson.Feature, with definite point geometry, and a time property.
// (This might want to - someday - become a more generic type of type;
// for example, a struct with a geojson.Feature embedded, or pure []byte's.)
// A geojson.Feature is a data structure with a type, geometry, and properties,
// which implements a special encoding spec (GeoJSON), which is conventional for geographic data.
// Notably, a geojson.Feature has no concept of time, but a CatTrack is
// as much a point in time as it is a point in space.
// catd and Cat Tracks use `properties.Time` and `properties.UnixTime` almost interchangeably,
// and they're in use constantly. UnixTime is preferred, since it represents 1-second granularity,
// which Cat Tracks asserts.
type CatTrack geojson.Feature

// NewCatTrack creates and initializes a GeoJSON feature given the required attributes.
func NewCatTrack(geometry orb.Geometry) *CatTrack {
	return &CatTrack{
		Type:       "Feature",
		Geometry:   geometry,
		Properties: make(map[string]interface{}),
	}
}

type SinkerFn func(track CatTrack)
type SinkerErrFn func(track CatTrack) error
type TransformerFn func(track CatTrack) CatTrack
type TransformerErrFn func(track CatTrack) (CatTrack, error)

// SetPropertySafe sets a property on a track in a thread safe way.
// Thread safe is not atomic, nor guaranteed stable.
// Unexpected side effects may occur.
func (ct *CatTrack) SetPropertySafe(key string, val any) {
	p := ct.Properties.Clone()
	p[key] = val
	ct.Properties = p
}

// SetPropertiesSafe sets properties in thread safety. See SetPropertySafe.
func (ct *CatTrack) SetPropertiesSafe(props map[string]interface{}) {
	p := ct.Properties.Clone()
	for k, v := range props {
		p[k] = v
	}
	ct.Properties = p
}

// DeletePropertySafe deletes properties in thread safety. See SetPropertySafe.
func (ct *CatTrack) DeletePropertySafe(key string) {
	p := ct.Properties.Clone()
	delete(p, key)
	ct.Properties = p
}

// MarshalJSON implements the json.Marshaler interface.
func (ct CatTrack) MarshalJSON() ([]byte, error) {
	f := geojson.Feature(ct)
	return f.MarshalJSON()
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (ct *CatTrack) UnmarshalJSON(data []byte) error {
	f, err := geojson.UnmarshalFeature(data)
	if err != nil {
		return err
	}
	*ct = *(*CatTrack)(f)
	return nil
}

// Copy returns a deep copy of the track.
func (ct *CatTrack) Copy() *CatTrack {
	cp := &CatTrack{}
	*cp = *ct
	return cp
}

// IsEmpty is useful for dealing with zero-value tracks.
func (ct *CatTrack) IsEmpty() bool {
	return ct == nil || ct.Geometry == nil ||
		ct.Properties == nil ||
		len(ct.Properties) == 0
}

// CatID conceptually returns the CatID of the track.
func (ct *CatTrack) CatID() conceptual.CatID {
	return conceptual.CatID(names.AliasOrSanitizedName(
		ct.Properties.MustString("Name", names.UknownName)))
}

// Time
// Here's a big deal.
// Cat Tracks only deals in tracks with a granularity of 1 second.
// Cat Tracks prefers the UnixTime property, but if it's not there, falls back to Time,
// which should be a string in RFC3339 format.
// If both times exist, they must be within 1 second of each other.
func (ct *CatTrack) Time() (time.Time, error) {
	unix, ok := ct.Properties["UnixTime"]
	if ok {
		if v, ok := unix.(int64); ok { // int64
			return time.Unix(v, 0), nil
		} else if v, ok := unix.(float64); ok { // float64
			return time.Unix(int64(v), 0), nil
		}
	}
	rfc3339, ok := ct.Properties["Time"]
	if !ok {
		return time.Time{}, fmt.Errorf("missing Time property")
	}
	if v, ok := rfc3339.(time.Time); ok {
		return v, nil
	}
	ts, ok := rfc3339.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("property Time is not a string")
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return time.Time{}, err
	}
	if t.IsZero() {
		return time.Time{}, fmt.Errorf("zero time")
	}
	return t, nil
}

// MustTime gets the time or panics.
func (ct *CatTrack) MustTime() time.Time {
	t, err := ct.Time()
	if err != nil {
		panic(err)
	}
	return t
}

// MustTimeOffset gets the time offset between two tracks, assuming a track happens before b track.
func MustTimeOffset(a, b CatTrack) time.Duration {
	if a.IsEmpty() || b.IsEmpty() {
		return time.Second
	}
	return b.MustTime().Sub(a.MustTime())
}

// MustContinuousTimeOffset is more than it seems.
// It returns the time offset between two tracks, but with magic/opinionated/adhoc caveats.
// If the offset is more than 24 hours, it resets to 1 second.
// If the offset is negative, it resets to 0.
func MustContinuousTimeOffset(a, b CatTrack) time.Duration {
	offset := MustTimeOffset(a, b)

	// If offset is more than 24 hours, reset to 1 second.
	// This handles signal loss/missing data.
	// If you start tracking on Monday, stop on Tuesday, and start again on Friday,
	// you don't get offset points for Wednesday and Thursday.
	if offset > time.Hour*24 {
		offset = time.Second
	}

	// If offset is negative, reset to 0.
	// This is an out-of-order non-chronological track. Unsorted.
	if offset < 0 {
		offset = 0
	}
	return offset
}

// Point returns the Point a cat is or was at.
func (ct *CatTrack) Point() orb.Point {
	return ct.Geometry.Bound().Center()
}

// Sanitize normalizes and streamlines a track.
// It sets an Alias (cat alias/catID/conceptual.CatID/nee-Name) property unless already set.
// It removes unnecessary properties, and sets a zero-value ID.
func Sanitize(ct CatTrack) CatTrack {
	// Mutate the ID to a zero-value constant in case the client decides to fill it.
	// CatTracks does not use this ID for anything, and we want to avoid false-negative
	// duplicates due to ID mismatches.
	ct.ID = 0
	if ct.Properties["Alias"] == nil {
		ct.Properties["Alias"] = names.AliasOrSanitizedName(ct.Properties.MustString("Name", ""))
	}
	// DELETE any properties without values; JSON nulls.
	// Some clients report empty properties, which is not a problem,
	// and not all client report the same properties.
	// All this does (...or should do!) is save a little space.
	for k, v := range ct.Properties {
		if v == nil {
			delete(ct.Properties, k)
		}
	}
	// The bounding box of a point is useless.
	// Only the GCPS client includes this, as an artifcat of its geojson lib.
	if ct.BBox != nil {
		ct.BBox = nil
	}
	return ct
}

func (ct *CatTrack) IsValid() bool {
	return ct.Validate() == nil
}

// Validate checks the track for basic validity.
// It returns the first error it encounters.
// TODO: JSON schema?
func (ct *CatTrack) Validate() error {
	if ct.Type != "Feature" {
		return fmt.Errorf("not a feature")
	}

	// Geometry is a point.
	if ct.Geometry == nil {
		return fmt.Errorf("nil geometry")
	}
	pt, ok := ct.Geometry.(orb.Point)
	if !ok {
		return fmt.Errorf("not a point")
	}

	// Point is valid (x,y::lng,lat).
	ptLng, ptLat := pt[0], pt[1]

	if ptLat < -90 || ptLat > 90 {
		return fmt.Errorf("invalid coordinate: lat=%.14f", ptLat)
	}
	if ptLng < -180 || ptLng > 180 {
		return fmt.Errorf("invalid coordinate: lng=%.14f", ptLng)
	}

	// Point has some properties.
	if ct.Properties == nil {
		return fmt.Errorf("nil properties")
	}
	if len(ct.Properties) == 0 {
		return fmt.Errorf("empty properties")
	}

	// Conceptually, CatID.
	if ct.Properties["Name"] == nil {
		return fmt.Errorf("nil name")
	}
	if n, ok := ct.Properties["Name"].(string); !ok {
		return fmt.Errorf("name not a string")
	} else if n == "" {
		return fmt.Errorf("empty name")
	}

	// Note: some historic tracks UUIDs are "".
	// Otherwise, effectively cat/device uuids.
	if ct.Properties["UUID"] == nil {
		return fmt.Errorf("nil uuid")
	}
	if _, ok := ct.Properties["UUID"].(string); !ok {
		return fmt.Errorf("uuid not a string")
	}

	if t, err := ct.Time(); err != nil {
		return fmt.Errorf("invalid time: %v", err)
	} else if t.IsZero() {
		return fmt.Errorf("zero time")
	}

	// If the track has both Time and UnixTime properties,
	// they must be within 1 second of each other.
	pTime, pTimeOK := ct.Properties["Time"]
	pUnixTime, pUnixTimeOK := ct.Properties["UnixTime"]
	if pTimeOK && pUnixTimeOK {
		var rfc3339, unix time.Time
		if v, ok := pTime.(string); ok {
			rfc3339, _ = time.Parse(time.RFC3339, v)
		} else if v, ok := pTime.(time.Time); ok {
			rfc3339 = v
		}
		if v, ok := pUnixTime.(int64); ok {
			unix = time.Unix(v, 0)
		} else if v, ok := pUnixTime.(float64); ok {
			unix = time.Unix(int64(v), 0)
		}
		delta := unix.Sub(rfc3339).Seconds()
		if math.Abs(delta) > 1 {
			return fmt.Errorf("time and unixtime mismatch (> +/- 1 second) Δ=%v rfc3339=%v unix=%v",
				delta, rfc3339, unix)
		}
	}

	if v, ok := ct.Properties["Accuracy"]; !ok {
		return fmt.Errorf("missing field: Accuracy")
	} else if _, ok := v.(float64); !ok {
		return fmt.Errorf("accuracy not a float64")
	}
	if ct.HasRawB64Image() {
		if v, ok := ct.Properties["img64"]; ok {
			if _, k := v.(string); !k {
				return fmt.Errorf("imgB64 not a string")
			}
		}
	}
	return nil
}

// SlicesSortFunc implements the slices.SortFunc for CatTrack slices.
// Sorting is done by time (chronologically, at 1 second granularity);
// then, in case of equivalence, by accuracy.
// > cmp(a, b) should return a negative number when a < b,
// > a positive number when a > b, and zero when a == b
func SlicesSortFunc(a, b CatTrack) int {
	aUUID := a.Properties.MustString("UUID", "a")
	bUUID := b.Properties.MustString("UUID", "b")
	if aUUID < bUUID {
		return -1
	} else if aUUID > bUUID {
		return 1
	}

	ti, err := a.Time()
	if err != nil {
		return 0
	}
	tj, err := b.Time()
	if err != nil {
		return 0
	}
	if ti.Unix() < tj.Unix() {
		return -1
	}
	if ti.Unix() > tj.Unix() {
		return 1
	}

	ai := a.Properties.MustFloat64("Accuracy", 0)
	aj := b.Properties.MustFloat64("Accuracy", 0)
	if ai > aj {
		return 1
	}
	if ai < aj {
		return -1
	}
	return 0
}

func (ct *CatTrack) StringPretty() string {
	pt := ct.Point()
	t := time.Time{}
	t, _ = ct.Time()
	return fmt.Sprintf("%s %v %s+/-%.0fm %.2fm/s",
		ct.CatID(),
		t.In(time.Local).Format("2006-01-02 15:04:05"),
		fmt.Sprintf("[%v,%v]",
			common.DecimalToFixed(pt.Lat(), common.GPSPrecision5),
			common.DecimalToFixed(pt.Lon(), common.GPSPrecision5)),
		ct.Properties.MustFloat64("Accuracy", -1),
		ct.Properties.MustFloat64("Speed", -1),
	)
}

func (ct *CatTrack) IsSnap() bool {
	return ct.HasRawB64Image() || ct.HasS3URL()
}

func (ct *CatTrack) ValidateSnap() error {
	if !ct.IsSnap() {
		return fmt.Errorf("not a snap")
	}
	if ct.HasRawB64Image() {
		if "" == ct.Properties.MustString("imgB64", "") {
			return fmt.Errorf("empty imgB64 data")
		}
		return nil
	} else if ct.HasS3URL() {
		if "" == ct.Properties.MustString("imgS3", "") {
			return fmt.Errorf("empty imgS3 URL")
		}
		return nil
	}
	return errors.New("no image data rly?") // don't get here / panic
}

func (ct *CatTrack) HasRawB64Image() bool {
	_, ok := ct.Properties["imgB64"]
	return ok
}

func (ct *CatTrack) HasS3URL() bool {
	_, ok := ct.Properties["imgS3"]
	return ok
}

// MustS3Key returns the conventional "key" for use primarily with catsnaps.
// If AWS S3 upload configured, that uses: AWS_BUCkETNAME/this-key.
// It is a cat/device/time-unique value.
func (ct *CatTrack) MustS3Key() string {
	if v, ok := ct.Properties["imgS3"]; ok {
		if s, ok := v.(string); ok {
			if strings.Contains(s, "/") {
				return strings.Split(s, "/")[1]
			}
			return s
		}
	}

	// Otherwise we get to construct it.
	catID := ct.CatID()
	uuid := ct.Properties.MustString("UUID")
	unixt := ct.MustTime().Unix() // snaps won't/can't happen more than once/cat/second

	// Note: CatTracks v1 used plain %d for unix time.
	// Now zero-padding thru 11 digits in case of the future.
	// Current time 1732040799.
	return fmt.Sprintf("%s_%s_%010d",
		catID,
		uuid,
		unixt,
	)
}

// S3SnapBucketName returns the bucket name from the imgS3 field,
// conventionally using the first of (two) /-delimited values.
func (ct *CatTrack) S3SnapBucketName() string {
	s3url := ct.Properties.MustString("imgS3", "")
	if s3url == "" {
		return ""
	}
	return s3url[:strings.Index(s3url, "/")]
}
