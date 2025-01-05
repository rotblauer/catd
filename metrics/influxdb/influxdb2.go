package influxdb

import (
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"sync"
	"time"
)

// ExportCatTracks posts tracks to an InfluxDB Write API.
// Because it accepts a slice, use batches. The Write API will buffer and flush.
// The last error encountered is returned.
func ExportCatTracks(tracks []cattrack.CatTrack) error {
	opts := influxdb2.DefaultOptions()
	opts.SetPrecision(time.Second)
	client := influxdb2.NewClientWithOptions(params.INFLUXDB_URL, params.INFLUXDB_TOKEN, opts)
	writeAPI := client.WriteAPI(params.INFLUXDB_ORG, params.INFLUXDB_BUCKET)

	// Errors returns a channel for reading errors which occurs during async writes.
	// Must be called before performing any writes for errors to be collected.
	// The chan is unbuffered and must be drained or the writer will block.
	// https://github.com/influxdata/influxdb-client-go?tab=readme-ov-file#reading-async-errors
	errorsCh := writeAPI.Errors()
	var err error
	wait := sync.WaitGroup{}
	wait.Add(1)
	go func() {
		defer wait.Done()
		for e := range errorsCh {
			if e != nil {
				err = e
			}
		}
	}()

	for _, track := range tracks {
		p := influxdb2.NewPointWithMeasurement("cattrack").
			SetTime(track.MustTime()).
			AddTag("alias", track.Properties["Alias"].(string)).
			AddTag("name", track.Properties["Name"].(string)).
			AddTag("uuid", track.Properties["UUID"].(string)).
			AddTag("activity", track.Properties["Activity"].(string)).
			AddField("latitude", track.Point().Lat()).
			AddField("longitude", track.Point().Lon()).
			AddField("heading", track.Properties["Heading"]).
			AddField("speed", track.Properties["Speed"]).
			AddField("elevation", track.Properties["Elevation"]).
			AddField("accuracy", track.Properties["Accuracy"]).
			// Add activity as a field, in addition to as tag, above.
			AddField("activity", track.Properties["Activity"])

		if track.IsSnap() {
			p.AddField("snap", 1)
		}

		if v, ok := track.Properties["TimeOffset"]; ok {
			p.AddField("time_offset", v)
		}

		// borken:
		if v, ok := track.Properties["TripDistance"]; ok {
			p.AddField("trip_distance", v)
		}
		// borken:
		if v, ok := track.Properties["TripDuration"]; ok {
			p.AddField("trip_duration", v)
		}
		if v, ok := track.Properties["Odometer"]; ok {
			p.AddField("odometer", v)
		}
		if v, ok := track.Properties["Pressure"]; ok {
			p.AddField("barometer", v)
		}
		if v, ok := track.Properties["BatteryLevel"]; ok {
			p.AddField("battery_level", v)
		}
		if v, ok := track.Properties["BatteryStatus"]; ok {
			if s, ok := v.(string); ok {
				// safety first, buf this will always be ok
				vv := 0
				switch s {
				case "charging", "full":
					vv = 1
				case "unplugged":
					// = 0
				default:
					// unknown
					vv = -1
				}
				p.AddField("battery_status", vv)
			}
		}
		if v, ok := track.Properties["Barometer"]; ok {
			p.AddField("barometer", v)
		}
		if v, ok := track.Properties["Lightmeter"]; ok {
			p.AddField("lightmeter", v)
		}
		if v, ok := track.Properties["AmbientTemp"]; ok {
			p.AddField("ambient_temp", v)
		}
		if v, ok := track.Properties["Humidity"]; ok {
			p.AddField("humidity", v)
		}
		if v, ok := track.Properties["AccelerometerX"]; ok {
			p.AddField("accelerometer_x", v)
		}
		if v, ok := track.Properties["AccelerometerY"]; ok {
			p.AddField("accelerometer_y", v)
		}
		if v, ok := track.Properties["AccelerometerZ"]; ok {
			p.AddField("accelerometer_z", v)
		}
		if v, ok := track.Properties["UserAccelerometerX"]; ok {
			p.AddField("user_accelerometer_x", v)
		}
		if v, ok := track.Properties["UserAccelerometerY"]; ok {
			p.AddField("user_accelerometer_y", v)
		}
		if v, ok := track.Properties["UserAccelerometerZ"]; ok {
			p.AddField("user_accelerometer_z", v)
		}
		if v, ok := track.Properties["GyroscopeX"]; ok {
			p.AddField("gyroscope_x", v)
		}
		if v, ok := track.Properties["GyroscopeY"]; ok {
			p.AddField("gyroscope_y", v)
		}
		if v, ok := track.Properties["GyroscopeZ"]; ok {
			p.AddField("gyroscope_z", v)
		}
		if v, ok := track.Properties["HeartRate"]; ok {
			p.AddField("heart_rate", v)
		}
		writeAPI.WritePoint(p)
	}
	writeAPI.Flush()
	client.Close()
	wait.Wait()
	return err
}
