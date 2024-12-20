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

// Unbacktrack removes tracks that are within the cat/UUID's populated window.
// The cat/UUID's window is only allowed to grow, and will grow to include the population window.
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

But still: A Monday's push (with tracker ABC-123-DEF-456), then Wednesday, will fail on Tuesday.
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
		err := c.State.StoreKVMarshalJSON(params.CatStateBucket, params.CatStateKey_Unbacktracker, records)
		if err != nil {
			c.logger.Error("Failed to store UUID window map", "error", err)
		}
		return err
	}

	// Reload the cat's window map from the state.
	recorded := uuidWindowMap{}
	if err := c.State.ReadKVUnmarshalJSON(params.CatStateBucket, params.CatStateKey_Unbacktracker, &recorded); err != nil {
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
			c.logger.Warn("Glitch in matrix: track within pop/uuid window",
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
			// Cat tracker's first rodeo.
			if t.After(popWindow.Last) {
				popWindow.Last = t
			} else if t.Before(popWindow.First) {
				popWindow.First = t
			}
			popWindow.Extend(t)
			popUUIDWindowMap.Store(uuid, popWindow)
			onceJamaisVu.Do(func() {
				c.logger.Info("Jamais vu: cat/uuid tracker first rodeo", "track", ct.StringPretty(), "first", popWindow.First, "last", popWindow.Last)
			})
			return true
		}

		catWindow = cwl.(Window)
		if catWindow.Contains(t) {
			// Do not update the pop window if we're not populating this track.
			onceDejaVu.Do(func() {
				df := t.Sub(catWindow.First).Round(time.Second).Seconds()
				dl := catWindow.Last.Sub(t).Round(time.Second).Seconds()
				d := math.Min(df, dl)
				c.logger.Warn("Deja vu: track within cat/uuid window",
					"by", (time.Second * time.Duration(d)).String(),
					"track", ct.StringPretty(),
					"first", catWindow.First,
					"last", catWindow.Last)
			})
			return false
		}
		onceJamaisVu.Do(func() {
			c.logger.Info("Jamais vu: track outside cat/uuid window", "track", ct.StringPretty(), "first", catWindow.First, "last", catWindow.Last)
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
		shortWindow := window.Duration() < 24*time.Hour
		if shortWindow {
			shortWindows++
		}
		if shortWindowsLogged < shortWindowsLogMax && shortWindow {
			logger.Warn(prefix+"UUID window (short)", "uuid", uuid, "window", window.String())
			shortWindowsLogged++
		}
		logger.Info(prefix+"UUID window", "uuid", uuid, "window", window.String())
	}
	if shortWindows > shortWindowsLogMax {
		logger.Warn(prefix+"UUID window (short)", "count", shortWindows)
	}

	/*
		...This x2000 is what we're trying to avoid....

		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=a792e5e4-9d82-4bea-a1c3-8bdac1c1e47b window="0s from 2021.01.31_09:06:55 to 2021.01.31_09:06:55"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=b0443dbb-83a3-4400-b004-05a171ad8d73 window="0s from 2021.01.31_09:02:44 to 2021.01.31_09:02:44"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=d3065db0-c1cc-4460-a12a-b9174954e8fe window="0s from 2021.01.31_09:02:01 to 2021.01.31_09:02:01"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=d36e04ffe5097b8b window="11s from 2021.02.02_09:42:00 to 2021.02.02_09:42:11"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=fd4643d8-2238-4f6b-9914-9caf160284d3 window="0s from 2021.01.31_09:04:34 to 2021.01.31_09:04:34"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=0a252778-00b8-4acd-a7c8-1c5346949f00 window="0s from 2021.01.31_08:46:43 to 2021.01.31_08:46:43"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=1561d1d8-b5c0-43ae-9e6a-f50dfb36a13b window="0s from 2021.01.31_09:08:37 to 2021.01.31_09:08:37"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=4bd775b2-fdb5-40ff-b3b5-52ab5a6317bc window="0s from 2021.01.31_09:09:59 to 2021.01.31_09:09:59"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=df1a8553-8729-4ccc-bac4-ab119531a65e window="0s from 2021.01.31_09:12:52 to 2021.01.31_09:12:52"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=f887a545-e78f-47f8-93ee-95a108b72ec8 window="0s from 2021.01.31_09:26:36 to 2021.01.31_09:26:36"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=12ae3089-3394-497b-be29-f941f26b9f78 window="0s from 2021.01.31_09:07:14 to 2021.01.31_09:07:14"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=2a437960-b5de-4f41-99ea-b578d6a1d568 window="0s from 2021.01.31_08:58:54 to 2021.01.31_08:58:54"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=a75a58e5-5a29-4ddf-b328-bdb655c431dd window="0s from 2021.01.31_09:00:57 to 2021.01.31_09:00:57"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=b9fbb4a4-9cc4-467f-a1cc-300ef9d9cb11 window="0s from 2021.01.31_07:50:27 to 2021.01.31_07:50:27"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=4d3d77eb-a02c-4f69-9b9f-ed185e9492f7 window="0s from 2021.01.31_07:50:12 to 2021.01.31_07:50:12"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=5a8c4209-3fc0-44a6-93af-ec84271575f1 window="0s from 2021.01.31_09:20:00 to 2021.01.31_09:20:00"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=6cc4e475-f665-497a-9699-58122a8815a0 window="0s from 2021.01.31_07:46:03 to 2021.01.31_07:46:03"
		2024/12/20 04:54:04 INFO Reloaded cat UUID window cat=ia uuid=e11021d4-5541-47bf-a3b1-1eded24991f9 window="0s from 2021.01.31_07:42:13 to 2021.01.31_07:42:13"
	*/

}

type Window struct {
	First time.Time
	Last  time.Time
}

func (w *Window) Duration() time.Duration {
	if w.First.IsZero() || w.Last.IsZero() {
		return 0
	}
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
