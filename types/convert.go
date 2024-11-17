package types

import (
	"encoding/json"
	"errors"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/names"
	"github.com/rotblauer/catd/types/trackpoint"
	"reflect"
	"time"
)

// TrackToFeature converts a TrackPoint to a GeoJSON feature.
func TrackToFeature(trackPointCurrent *trackpoint.TrackPoint) *geojson.Feature {
	p := geojson.NewFeature(orb.Point{trackPointCurrent.Lng, trackPointCurrent.Lat})

	// currently need speed, name,time
	props := make(map[string]interface{})
	defer func() {
		p.Properties = props
	}()

	// If an alias exists for the cat, install it as a property.
	if alias := names.AliasOrName(trackPointCurrent.Name); alias != trackPointCurrent.Name {
		props["Alias"] = alias
	}

	props["UUID"] = trackPointCurrent.Uuid
	props["Name"] = trackPointCurrent.Name
	props["Time"] = trackPointCurrent.Time
	props["UnixTime"] = trackPointCurrent.Time.Unix()
	props["Version"] = trackPointCurrent.Version
	props["Speed"] = common.DecimalToFixed(trackPointCurrent.Speed, 3)
	props["Elevation"] = common.DecimalToFixed(trackPointCurrent.Elevation, 2)
	props["Heading"] = common.DecimalToFixed(trackPointCurrent.Heading, 1)
	props["Accuracy"] = common.DecimalToFixed(trackPointCurrent.Accuracy, 2)

	if trackPointCurrent.VAccuracy > 0 {
		props["vAccuracy"] = trackPointCurrent.VAccuracy
	}
	if trackPointCurrent.SpeedAccuracy > 0 {
		props["speed_accuracy"] = trackPointCurrent.SpeedAccuracy
	}
	if trackPointCurrent.HeadingAccuracy > 0 {
		props["heading_accuracy"] = trackPointCurrent.HeadingAccuracy
	}

	// not implemented yet
	if hr := trackPointCurrent.HeartRate; hr != 0 {
		props["HeartRate"] = hr
	}

	if ns, e := trackpoint.NotesField(trackPointCurrent.Notes).AsNoteStructured(); e == nil {
		props["Activity"] = ns.Activity

		if v := ns.ActivityConfidence; v != nil {
			props["ActivityConfidence"] = *v
		}

		props["Pressure"] = common.DecimalToFixed(ns.Pressure, 2)
		if ns.CustomNote != "" {
			props["Notes"] = ns.CustomNote
		}
		if ns.ImgS3 != "" {
			props["imgS3"] = ns.ImgS3
		}
		if ns.HasRawImage() {
			props["imgB64"] = ns.ImgB64
		}
		if ns.HasValidVisit() {
			// TODO: ok to use mappy sub interface here?
			props["Visit"] = ns.Visit
		}

		if trackPointCurrent.HeartRate == 0 {
			if i := ns.HeartRateI(); i > 0 {
				props["HeartRate"] = common.DecimalToFixed(i, 2)
			}
		}

		// these properties might exist in the track, but we haven't been dumping them to json,
		// they're not deal breakers, but nice to have
		if ns.NumberOfSteps > 0 {
			props["NumberOfSteps"] = ns.NumberOfSteps
		}
		if ns.AverageActivePace > 0 {
			props["AverageActivePace"] = common.DecimalToFixed(ns.AverageActivePace, 2)
		}
		if ns.CurrentPace > 0 {
			props["CurrentPace"] = common.DecimalToFixed(ns.CurrentPace, 2)
		}
		if ns.CurrentCadence > 0 {
			props["CurrentCadence"] = common.DecimalToFixed(ns.CurrentCadence, 2)
		}
		if ns.CustomNote != "" {
			props["CustomNote"] = ns.CustomNote
		}
		if ns.FloorsAscended > 0 {
			props["FloorsAscended"] = ns.FloorsAscended
		}
		if ns.FloorsDescended > 0 {
			props["FloorsDescended"] = ns.FloorsDescended
		}
		if !ns.CurrentTripStart.IsZero() {
			props["CurrentTripStart"] = ns.CurrentTripStart
		}
		if ns.Distance > 0 {
			props["Distance"] = common.DecimalToFixed(ns.Distance, 2)
		}

		if ns.Lightmeter > 0 {
			props["Lightmeter"] = common.DecimalToFixed(ns.Lightmeter, 2)
		}
		if ns.AmbientTemp > 0 {
			props["AmbientTemp"] = common.DecimalToFixed(ns.AmbientTemp, 2)
		}
		if ns.Humidity > 0 {
			props["Humidity"] = common.DecimalToFixed(ns.Humidity, 2)
		}
		if v := ns.Accelerometer.X; v != nil {
			props["AccelerometerX"] = *v
		}
		if v := ns.Accelerometer.Y; v != nil {
			props["AccelerometerY"] = *v
		}
		if v := ns.Accelerometer.Z; v != nil {
			props["AccelerometerZ"] = *v
		}
		if v := ns.UserAccelerometer.X; v != nil {
			props["UserAccelerometerX"] = *v
		}
		if v := ns.UserAccelerometer.Y; v != nil {
			props["UserAccelerometerY"] = *v
		}
		if v := ns.UserAccelerometer.Z; v != nil {
			props["UserAccelerometerZ"] = *v
		}
		if v := ns.Gyroscope.X; v != nil {
			props["GyroscopeX"] = *v
		}
		if v := ns.Gyroscope.Y; v != nil {
			props["GyroscopeY"] = *v
		}
		if v := ns.Gyroscope.Z; v != nil {
			props["GyroscopeZ"] = *v
		}
		if v := ns.BatteryStatus; v != "" {
			bs := trackpoint.BatteryStatus{}
			if err := json.Unmarshal([]byte(v), &bs); err == nil {
				props["BatteryStatus"] = bs.Status
				props["BatteryLevel"] = common.DecimalToFixed(bs.Level, 2)
			}
		}
		if v := ns.NetworkInfo; v != "" {
			props["NetworkInfo"] = v
		}

		// if trackPointCurrent.HeartRate == 0 && ns.HeartRateType != "" {
		// 	props["HeartRateType"] = ns.HeartRateType
		// }

	} else if _, e := trackpoint.NotesField(trackPointCurrent.Notes).AsFingerprint(); e == nil {
		// maybe do something with identity consolidation?
	} else {
		// NOOP normal
		// props["Notes"] = note.NotesField(trackPointCurrent.Notes).AsNoteString()
	}
	return p
}

func FeatureToTrack(f geojson.Feature) (trackpoint.TrackPoint, error) {
	var err error
	tp := trackpoint.TrackPoint{}

	p, ok := f.Geometry.(orb.Point)
	if !ok {
		return tp, errors.New("not a point")
	}
	tp.Lng = p.Lon()
	tp.Lat = p.Lat()

	if v, ok := f.Properties["UUID"]; ok {
		tp.Uuid = v.(string)
	}
	if v, ok := f.Properties["Name"]; ok {
		tp.Name = v.(string)
	}
	if v, ok := f.Properties["Time"]; ok {
		tp.Time, err = time.Parse(time.RFC3339, v.(string))
		if err != nil {
			return tp, err
		}
	}
	if v, ok := f.Properties["Version"]; ok {
		tp.Version = v.(string)
	}
	if v, ok := f.Properties["Speed"]; ok {
		tp.Speed = v.(float64)
	}
	if v, ok := f.Properties["Elevation"]; ok {
		tp.Elevation = v.(float64)
	}
	if v, ok := f.Properties["Heading"]; ok {
		tp.Heading = v.(float64)
	}
	if v, ok := f.Properties["Accuracy"]; ok {
		tp.Accuracy = v.(float64)
	}
	if v, ok := f.Properties["HeartRate"]; ok {
		tp.HeartRate = v.(float64)
	}
	anyNotes := false
	notes := trackpoint.NoteStructured{}
	if v, ok := f.Properties["Activity"]; ok {
		notes.Activity = v.(string)
		anyNotes = true
	}
	if v, ok := f.Properties["Pressure"]; ok {
		notes.Pressure = v.(float64)
		anyNotes = true
	}
	if v, ok := f.Properties["imgS3"]; ok {
		notes.ImgS3 = v.(string)
		anyNotes = true
	}
	if anyNotes {
		tp.Notes = notes.MustAsString()
	}
	return tp, nil
}

// TrackToFeature2 (WIP/experimental) is a track->geojson function that uses reflection to
// transfer fields. This might be useful for a more dynamic approach to geojson, but it's
// probably better in the broader scheme to just swap trackpoints for geojson entirely, though
// this would require coordinated changes between the client (cattracker) and server.
func TrackToFeature2(tp *trackpoint.TrackPoint) *geojson.Feature {
	if tp == nil {
		return nil
	}

	// config
	var timeFormat = time.RFC3339

	p := geojson.NewFeature(orb.Point{tp.Lng, tp.Lat})
	props := make(map[string]interface{})

	tpV := reflect.ValueOf(*tp)
	typeOfS := tpV.Type()

	stringSliceContains := func(s []string, e string) bool {
		for _, a := range s {
			if a == e {
				return true
			}
		}
		return false
	}

	skipTrackFields := []string{"Lat", "Lng", "PushToken", "Version", "COVerified", "RemoteAddr", "Notes"}

	for i := 0; i < tpV.NumField(); i++ {
		fieldName := typeOfS.Field(i).Name
		if stringSliceContains(skipTrackFields, fieldName) {
			continue
		}
		switch t := tpV.Field(i).Interface().(type) {
		case time.Time:
			props[typeOfS.Field(i).Name] = t.Format(timeFormat)
		case float64:
			props[typeOfS.Field(i).Name] = common.DecimalToFixed(t, 2)
		case string:
			if t != "" {
				props[typeOfS.Field(i).Name] = t
			}
		default:
			props[typeOfS.Field(i).Name] = tpV.Field(i).Interface()
		}
	}

	if ns, e := trackpoint.NotesField(tp.Notes).AsNoteStructured(); e == nil {
		tpN := reflect.ValueOf(ns)
		typeOfN := tpN.Type()

		if ns.HasValidVisit() {
			props["Visit"] = ns.Visit
		}
		skipNoteFields := []string{"Visit", "NetworkInfo"}
		for i := 0; i < tpN.NumField(); i++ {
			if stringSliceContains(skipNoteFields, typeOfN.Field(i).Name) {
				continue
			}
			switch t := tpN.Field(i).Interface().(type) {
			case time.Time:
				props[typeOfN.Field(i).Name] = t.Format(timeFormat)
			case float64:
				props[typeOfN.Field(i).Name] = common.DecimalToFixed(t, 2)
			case string:
				if t != "" {
					props[typeOfN.Field(i).Name] = t
				}
			default:
				props[typeOfN.Field(i).Name] = tpN.Field(i).Interface()
			}
		}
	}

	p.Properties = props
	return p
}
