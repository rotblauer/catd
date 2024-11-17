package cattrack

import (
	"fmt"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/names"
	"time"
)

type CatTrack geojson.Feature

func (ct *CatTrack) CatID() conceptual.CatID {
	return conceptual.CatID(names.AliasOrSanitizedName(ct.Properties.MustString("Name", "")))
}

func (ct *CatTrack) MarshalJSON() ([]byte, error) {
	return (*geojson.Feature)(ct).MarshalJSON()
}

func (ct *CatTrack) UnmarshalJSON(data []byte) error {
	return (*geojson.Feature)(ct).UnmarshalJSON(data)
}

func (ct *CatTrack) Time() (time.Time, error) {
	if v, ok := ct.Properties["Time"].(time.Time); ok {
		return v, nil
	}
	ts, ok := ct.Properties["Time"].(string)
	if !ok {
		return time.Time{}, fmt.Errorf("missing Time property (string)")
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}

func Sanitize(ct *CatTrack) *CatTrack {
	// Mutate the ID to a zero-value constant in case the client decides to fill it.
	// CatTracks does not use this ID for anything, and we want to avoid false-negative
	// duplicates due to ID mismatches.
	ct.ID = 0
	if ct.Properties["Alias"] == nil {
		ct.Properties["Alias"] = names.AliasOrSanitizedName(ct.Properties.MustString("Name", ""))
	}
	return ct
}

func (ct *CatTrack) Validate() error {
	// Point is not nil.
	if ct == nil {
		return fmt.Errorf("nil point")
	}

	// Point is a point.
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
	if ct.Properties["Time"] == nil {
		return fmt.Errorf("nil time")
	}
	if _, ok := ct.Properties["Time"]; !ok {
		return fmt.Errorf("missing field: Time")
	}
	if v, ok := ct.Properties["Accuracy"]; !ok {
		return fmt.Errorf("missing field: Accuracy")
	} else if _, ok := v.(float64); !ok {
		return fmt.Errorf("accuracy not a float64")
	}
	return nil
}

// Sorter implements the slices.SortFunc for CatTrack slices.
// Sorting is done by time (chronologically, at 1 second granularity);
// then, in case of equivalence, by accuracy.
// > cmp(a, b) should return a negative number when a < b,
// > a positive number when a > b, and zero when a == b
func Sorter(a, b *CatTrack) int {
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
	alias := names.AliasOrSanitizedName(ct.Properties.MustString("Name", ""))
	dot := ""
	switch alias {
	case "rye":
		dot = "🔵"
	case "ia":
		dot = "🔴"
	}
	return fmt.Sprintf("%s Name: %s (%s), Time: %v, Coords: %v, Accuracy: %v, Speed: %v",
		dot,
		ct.Properties["Name"],
		alias,
		ct.Properties["Time"],
		ct.Geometry.(orb.Point),
		ct.Properties["Accuracy"],
		ct.Properties["Speed"],
	)
}