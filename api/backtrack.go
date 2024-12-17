package api

import (
	"context"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"math"
	"sync"
	"time"
)

type Window struct {
	First time.Time
	Last  time.Time
}

// Unbacktrack removes tracks that are within the cat's populated window.
// The cat's window is only allowed to grow, and will grow to include the population window.
// Since each cat can have more than one device, leading to more than "simultaneous" population windows,
// the population window(s) are grouped per UUID.
//
// Be aware that gaps can be created if the population window/s is not contiguous.
// For example: 1-3, then 5-8, yields 1-8; and cat will never back-fill the gap at 4.
//
// BTW ia cat has at least 2879 UUIDs, most with windows < 24hrs.
/*

For example, if a cat...
- Starts cattracks _and another GPS tracker_, and heads on a bike ride,
- and cattracks dies for some reason along the way,
- when cat gets back and starts cattracks again (and pushes -- closing the window),
- cat will be UNABLE to upload the other GPS tracks to backfill,
- since they will fall within the already-populated window.

...But wait! That's wrong!
If the other GPS tracker has a different UUID, it will be allowed to populate,
since the windows are cat/UUID specific.
*/
func (c *Cat) Unbacktrack(ctx context.Context, in <-chan cattrack.CatTrack) (<-chan cattrack.CatTrack, func() error) {

	//	slog.Warn(`######### UNBACKTRACKING ENABLED #########
	//
	//Unbacktrack is a dangerous device.
	//It prevents tracks from getting populated.
	//
	//It drops tracks that lie within a Cat:UUID's already-populated time window,
	//enabling a kind of idempotency.
	//`)
	//time.Sleep(5 * time.Second)

	// catUUIDWindowMap defines the time range of cat's tracks.
	// This window is read-only until the end of the stream.
	catUUIDWindowMap := sync.Map{}

	// popUUIDWindowMap defines the time range of cat tracks for this population.
	popUUIDWindowMap := sync.Map{}

	onClose := func() error {

		// Extend the cat window map by the population window map.
		popUUIDWindowMap.Range(func(k, v interface{}) bool {
			pw := v.(Window)
			catWindow, ok := catUUIDWindowMap.Load(k)
			if !ok {
				catUUIDWindowMap.Store(k, pw)
				return true
			}

			cw := catWindow.(Window)
			if pw.First.Before(cw.First) {
				cw.First = pw.First
			}
			if pw.Last.After(cw.Last) {
				cw.Last = pw.Last
			}
			catUUIDWindowMap.Store(k, cw)
			return true
		})

		// Transform the catUUIDWindowMap to a map[string]Window for marshaling.
		m := map[string]Window{}
		catUUIDWindowMap.Range(func(key, value interface{}) bool {
			m[key.(string)] = value.(Window)
			return true
		})
		skiplog := 0
		for k, v := range m {
			if v.Last.Sub(v.First) > time.Hour*24 {
				c.logger.Info("Storing cat window", "uuid", k, "first", v.First, "last", v.Last)
			} else {
				skiplog++
			}
		}
		if skiplog > 0 {
			c.logger.Warn("Skipped logging of short cat windows", "count", skiplog)
		}
		err := c.State.StoreKVMarshalJSON(params.CatStateBucket, []byte("catUUIDWindowMap"), m)
		if err != nil {
			c.logger.Error("Failed to store UUID window map", "error", err)
		}
		return err
	}

	// Reload the cat's window map from the state.
	m := map[string]Window{}
	if err := c.State.ReadKVUnmarshalJSON(params.CatStateBucket, []byte("catUUIDWindowMap"), &m); err != nil {
		c.logger.Warn("Did not read UUID window map (new cat?)", "error", err)
	} else {
		skiplog := 0
		for k, v := range m {
			if v.Last.Sub(v.First) > time.Hour*24 {
				c.logger.Info("Loaded cat window", "uuid", k, "first", v.First, "last", v.Last)
			} else {
				skiplog++
			}
			catUUIDWindowMap.Store(k, v)
		}
		if skiplog > 0 {
			c.logger.Warn("Skipped logging of short cat windows", "count", skiplog)
		}
	}

	onceDejavu := sync.Once{}
	onceFresh := sync.Once{}

	unbacktracked := stream.Filter(ctx, func(ct cattrack.CatTrack) bool {
		uuid := ct.Properties.MustString("UUID", "")
		t := ct.MustTime()

		// Get or init the population window.
		var popWindow Window
		pwl, ok := popUUIDWindowMap.Load(uuid)
		if ok {
			popWindow = pwl.(Window)
		} else {
			popWindow = Window{
				First: t,
				Last:  t,
			}
			popUUIDWindowMap.Store(uuid, popWindow)
		}

		if t.After(popWindow.First) && t.Before(popWindow.Last) {
			df := t.Sub(popWindow.First).Round(time.Second).Seconds()
			dl := popWindow.Last.Sub(t).Round(time.Second).Seconds()
			d := math.Min(df, dl)
			c.logger.Warn("Track within pop window",
				"by", (time.Second * time.Duration(d)).String(),
				"track", ct.StringPretty(),
				"first", popWindow.First, "last", popWindow.Last)
			return false
		}

		// Track is validated to not be within the population window.

		// Get or init the cat window (read only).
		var catWindow Window
		cwl, catWindowOK := catUUIDWindowMap.Load(uuid)
		if !catWindowOK {
			// There is no cat window for this track/uuid,
			// so spread the pop window and return OK.
			if t.After(popWindow.Last) {
				popWindow.Last = t
			} else if t.Before(popWindow.First) {
				popWindow.First = t
			}
			popUUIDWindowMap.Store(uuid, popWindow)
			return true
		}

		catWindow = cwl.(Window)
		spreadsCatWindow := t.Before(catWindow.First) || t.After(catWindow.Last)
		if !spreadsCatWindow {
			// Do not update the pop window if we're not populating this track.
			onceDejavu.Do(func() {
				c.logger.Warn("Track within cat window", "track", ct.StringPretty(), "first", catWindow.First, "last", catWindow.Last)
			})
			return false
		}
		onceFresh.Do(func() {
			c.logger.Info("Track outside cat window", "track", ct.StringPretty(), "first", catWindow.First, "last", catWindow.Last)
		})
		if t.After(popWindow.Last) {
			popWindow.Last = t
		} else if t.Before(popWindow.First) {
			popWindow.First = t
		}
		popUUIDWindowMap.Store(uuid, popWindow)
		return true
	}, in)
	return unbacktracked, onClose
}
