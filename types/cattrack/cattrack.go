package cattrack

import (
	"fmt"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/names"
	"strings"
	"time"
)

type CatTrack geojson.Feature

// NewCatTrack creates and initializes a GeoJSON feature given the required attributes.
func NewCatTrack(geometry orb.Geometry) *CatTrack {
	return &CatTrack{
		Type:       "Feature",
		Geometry:   geometry,
		Properties: make(map[string]interface{}),
	}
}

func (ct *CatTrack) SetPropertySafe(key string, val any) {
	p := ct.Properties.Clone()
	p[key] = val
	ct.Properties = p
}

func (ct *CatTrack) DeletePropertySafe(key string) {
	p := ct.Properties.Clone()
	delete(p, key)
	ct.Properties = p
}

func (ct CatTrack) MarshalJSON() ([]byte, error) {
	f := geojson.Feature(ct)
	return f.MarshalJSON()
}

func (ct *CatTrack) UnmarshalJSON(data []byte) error {
	f, err := geojson.UnmarshalFeature(data)
	if err != nil {
		return err
	}
	*ct = *(*CatTrack)(f)
	//ct.Geometry = geojson.NewGeometry(f.Geometry).Geometry()
	return nil
}

func NewCatTrackFromFeature(f *geojson.Feature) *CatTrack {
	return (*CatTrack)(f)
}

func (ct *CatTrack) Copy() *CatTrack {
	cp := &CatTrack{}
	*cp = *ct
	return cp
}

func (ct *CatTrack) IsEmpty() bool {
	return ct == nil ||
		ct.Geometry == nil ||
		ct.Properties == nil ||
		len(ct.Properties) == 0
}

func (ct *CatTrack) CatID() conceptual.CatID {
	return conceptual.CatID(names.AliasOrSanitizedName(
		ct.Properties.MustString("Name", names.UknownName)))
}

func (ct *CatTrack) Time() (time.Time, error) {
	pt, ok := ct.Properties["Time"]
	if !ok {
		return time.Time{}, fmt.Errorf("missing Time property")
	}
	if v, ok := pt.(time.Time); ok {
		return v, nil
	}
	ts, ok := pt.(string)
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

func (ct *CatTrack) MustTime() time.Time {
	t, err := ct.Time()
	if err != nil {
		panic(err)
	}
	return t
}

func (ct *CatTrack) Point() orb.Point {
	return ct.Geometry.Bound().Center()
}

func Sanitize(ct CatTrack) CatTrack {
	// Mutate the ID to a zero-value constant in case the client decides to fill it.
	// CatTracks does not use this ID for anything, and we want to avoid false-negative
	// duplicates due to ID mismatches.
	ct.ID = 0
	if ct.Properties["Alias"] == nil {
		ct.Properties["Alias"] = names.AliasOrSanitizedName(ct.Properties.MustString("Name", ""))
	}
	return ct
}

func (ct *CatTrack) IsValid() bool {
	return ct.Validate() == nil
}

func (ct *CatTrack) Validate() error {
	if ct == nil {
		return fmt.Errorf("nil track")
	}

	// Geometry is a point.
	if ct.Geometry == nil {
		return fmt.Errorf("nil geometry")
	}
	pt, ok := ct.Geometry.(orb.Point)
	if !ok {
		return fmt.Errorf("not a point")
	}

	// Point is valid (lat, lng).
	ptLng, ptLat := pt[0], pt[1]

	if ptLat > 90 || ptLat < -90 {
		return fmt.Errorf("invalid coordinate: lat=%.14f", ptLat)
	}
	if ptLng > 180 || ptLng < -180 {
		return fmt.Errorf("invalid coordinate: lng=%.14f", ptLng)
	}

	// Point has some properties.
	if ct.Properties == nil {
		return fmt.Errorf("nil properties")
	}

	if ct.Properties["Name"] == nil {
		return fmt.Errorf("nil name")
	}
	if _, ok := ct.Properties["Name"].(string); !ok {
		return fmt.Errorf("name not a string")
	}
	if ct.Properties["UUID"] == nil {
		return fmt.Errorf("nil uuid")
	}
	if _, ok := ct.Properties["UUID"].(string); !ok {
		return fmt.Errorf("uuid not a string")
	}
	if _, err := ct.Time(); err != nil {
		return fmt.Errorf("invalid time: %v", err)
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
	// TODO: Validate more. Expected/required types.
	return nil
}

// SortFunc implements the slices.SortFunc for CatTrack slices.
// Sorting is done by time (chronologically, at 1 second granularity);
// then, in case of equivalence, by accuracy.
// > cmp(a, b) should return a negative number when a < b,
// > a positive number when a > b, and zero when a == b
func SortFunc(a, b CatTrack) int {
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
	return fmt.Sprintf("%s/%v/%q/%.0f/%.2f",
		ct.CatID(),
		t.In(time.Local).Format("2006-01-02 15:04:05"),
		fmt.Sprintf("%v, %v", pt.Lat(), pt.Lon()),
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
		if ct.Properties.MustString("imgB64", "") == "" {
			return fmt.Errorf("empty imgB64 data")
		}
	}
	if ct.HasS3URL() {
		if ct.Properties.MustString("imgS3", "") == "" {
			return fmt.Errorf("empty imgS3 URL")
		}
	}
	return nil
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
