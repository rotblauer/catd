package api

import (
	"context"
	"fmt"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"log/slog"
	"math"
	"sync"
	"time"
)

type Window struct {
	First time.Time
	Last  time.Time
}

func (w *Window) Duration() time.Duration {
	return w.Last.Sub(w.First)
}

func (w *Window) Contains(t time.Time) bool {
	return t.After(w.First) && t.Before(w.Last)
}

func (w *Window) ExtendFromWindow(ww *Window) {
	if ww.First.Before(w.First) {
		w.First = ww.First
	}
	if ww.Last.After(w.Last) {
		w.Last = ww.Last
	}
}

func (w *Window) Extend(t time.Time) {
	if t.Before(w.First) {
		w.First = t
	}
	if t.After(w.Last) {
		w.Last = t
	}
}

func (w *Window) String() string {
	f := "2006.01.02_15:04:05"
	return fmt.Sprintf("%s from %s to %s",
		w.Duration().Round(time.Second),
		w.First.Format(f), w.Last.Format(f))
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

But still: A Monday's push, then Wednesday, will fail on Tuesday.
Careful.
*/
type uuidWindowMap map[string]Window

// Unbacktrack is a dangerous device.
// It prevents tracks from getting populated.
// It drops (filters) tracks that lie WITHIN a Cat/UUID's already-populated time window.
// This prevents the cat from back-filling tracks that were missed, if they fall within the already-seen window.
// But it enables Cat.Populate runs to run idempotently given the same data source.
func (c *Cat) Unbacktrack(ctx context.Context, in <-chan cattrack.CatTrack) (<-chan cattrack.CatTrack, func() error) {

	// catUUIDWindowMap defines the time range of cat's tracks.
	// This window is read-only until the end of the stream.
	catUUIDWindowMap := sync.Map{}

	// popUUIDWindowMap defines the time range of cat tracks for this population.
	popUUIDWindowMap := sync.Map{}

	// onClose stores the stateful representation of the cat's window map(s).
	onClose := func() error {

		// Extend the cat window map by the population window map.
		recordsPop := uuidWindowMap{} // For logging.
		popUUIDWindowMap.Range(func(popUuid, tt interface{}) bool {
			recordsPop[popUuid.(string)] = tt.(Window)

			popW := tt.(Window)
			catUUIDWindow, ok := catUUIDWindowMap.Load(popUuid)
			if !ok {
				// There was no cat window! Cat's UUID's first rodeo. New cat tracker?
				catUUIDWindow = popW
				catUUIDWindowMap.Store(popUuid, catUUIDWindow)
				return true
			}
			catW := catUUIDWindow.(Window)
			catW.ExtendFromWindow(&popW)
			catUUIDWindowMap.Store(popUuid, catW)
			return true
		})
		logUUIDWindowMap(c.logger, recordsPop, "Pop cat ")

		// Transform the catUUIDWindowMap to a map[string]Window for marshaling.
		records := uuidWindowMap{}
		catUUIDWindowMap.Range(func(key, value interface{}) bool {
			records[key.(string)] = value.(Window)
			return true
		})
		logUUIDWindowMap(c.logger, records, "All-time cat ")

		// Store the cat's UUID:window map to the persistent state.
		err := c.State.StoreKVMarshalJSON(params.CatStateBucket, []byte("catUUIDWindowMap"), records)
		if err != nil {
			c.logger.Error("Failed to store UUID window map", "error", err)
		}
		return err
	}

	// Reload the cat's window map from the state.
	recorded := uuidWindowMap{}
	if err := c.State.ReadKVUnmarshalJSON(params.CatStateBucket, []byte("catUUIDWindowMap"), &recorded); err != nil {
		c.logger.Warn("Did not read UUID window map (new cat?)", "error", err)
	} else {
		for k, v := range recorded {
			catUUIDWindowMap.Store(k, v)
		}
		logUUIDWindowMap(c.logger, recorded, "Reloaded cat ")
	}

	onceDejaVu := sync.Once{}
	onceJamaisVu := sync.Once{}
	unbacktracked := stream.Filter(ctx, func(ct cattrack.CatTrack) bool {
		t := ct.MustTime()
		uuid := ct.Properties.MustString("UUID", "")

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

		if popWindow.Contains(t) {
			df := t.Sub(popWindow.First).Round(time.Second).Seconds()
			dl := popWindow.Last.Sub(t).Round(time.Second).Seconds()
			d := math.Min(df, dl)
			c.logger.Warn("Glitch in matrix: track within pop window",
				"by", (time.Second * time.Duration(d)).String(),
				"track", ct.StringPretty(),
				"first", popWindow.First.Format(time.DateTime),
				"last", popWindow.Last.Format(time.DateTime))
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
			popWindow.Extend(t)
			popUUIDWindowMap.Store(uuid, popWindow)
			return true
		}

		catWindow = cwl.(Window)
		spreadsCatWindow := t.Before(catWindow.First) || t.After(catWindow.Last)
		if !spreadsCatWindow {
			// Do not update the pop window if we're not populating this track.
			onceDejaVu.Do(func() {
				df := t.Sub(catWindow.First).Round(time.Second).Seconds()
				dl := catWindow.Last.Sub(t).Round(time.Second).Seconds()
				d := math.Min(df, dl)
				c.logger.Warn("Deja vu: track within cat window",
					"by", (time.Second * time.Duration(d)).String(),
					"track", ct.StringPretty(),
					"first", catWindow.First,
					"last", catWindow.Last)
			})
			return false
		}
		onceJamaisVu.Do(func() {
			c.logger.Info("Jamais vu: track outside cat window", "track", ct.StringPretty(), "first", catWindow.First, "last", catWindow.Last)
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

func logUUIDWindowMap(logger *slog.Logger, m uuidWindowMap, prefix string) {
	// A short window is a UUID window that is less than 24 hours.
	// These are where, for instance,
	// - the cat got a new phone and pushed tracks with a factory Name/UUID, then quickly fixed it,
	// - development clients used temporary names,
	// - ... anytime the cat/uuid tracked for less than 24 hours
	shortWindows := 0
	shortWindowsLogged, shortWindowsLogMax := 0, 3
	windows := []Window{}
	for uuid, window := range m {
		windows = append(windows, window)
		shortWindow := window.Last.Sub(window.First) < time.Hour*1 //24
		if shortWindow {
			shortWindows++
		}
		if shortWindowsLogged < shortWindowsLogMax && shortWindow {
			logger.Warn(prefix+"UUID window (short)", "uuid", uuid, "window", window.String())
			shortWindowsLogged++
		}
		logger.Info(prefix+"UUID window", "uuid", uuid, "window", window.String())
	}

}
