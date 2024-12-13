package api

import (
	"context"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"sync"
	"time"
)

type Window struct {
	First time.Time
	Last  time.Time
}

func (c *Cat) Unbacktrack(ctx context.Context, in <-chan cattrack.CatTrack) (<-chan cattrack.CatTrack, func() error) {

	// catUUIDWindowMap defines the time range of cat's tracks.
	catUUIDWindowMap := sync.Map{}

	// popUUIDWindowMap defines the time range of cat's tracks for this population.
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
		for k, v := range m {
			c.logger.Info("Storing cat window", "uuid", k, "first", v.First, "last", v.Last)
		}
		err := c.State.StoreKVJSON(params.CatStateBucket, []byte("catUUIDWindowMap"), m)
		if err != nil {
			c.logger.Error("Failed to store UUID window map", "error", err)
		}
		return err
	}

	// Reload the cat's window map from the state.
	m := map[string]Window{}
	if err := c.State.ReadKVUnmarshal(params.CatStateBucket, []byte("catUUIDWindowMap"), &m); err != nil {
		c.logger.Warn("Failed to read UUID window map (new cat?)", "error", err)
	} else {
		for k, v := range m {
			c.logger.Info("Loaded cat window", "uuid", k, "first", v.First, "last", v.Last)
			catUUIDWindowMap.Store(k, v)
		}
	}

	onceDejavu := sync.Once{}
	onceFresh := sync.Once{}

	unbacktracked := stream.Filter(ctx, func(ct cattrack.CatTrack) bool {
		uuid := ct.Properties.MustString("UUID", "")
		t := ct.MustTime()

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
			c.logger.Warn("Track within pop window", "track", ct.StringPretty(), "first", popWindow.First, "last", popWindow.Last)
			return false
		}

		// All tracks from here are outside the population window.

		var catWindow Window
		cwl, catWindowOK := catUUIDWindowMap.Load(uuid)
		if !catWindowOK {
			// There is no cat window for this track.
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
