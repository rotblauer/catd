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

	uuidWindowMap := sync.Map{}

	onClose := func() error {
		m := map[string]Window{}
		uuidWindowMap.Range(func(key, value interface{}) bool {
			m[key.(string)] = value.(Window)
			return true
		})
		return c.State.StoreKVJSON(params.CatStateBucket, []byte("uuidWindowMap"), m)
	}

	m := map[string]Window{}
	if err := c.State.ReadKVUnmarshal(params.CatStateBucket, []byte("uuidWindowMap"), &m); err != nil {
		c.logger.Warn("Failed to read UUID window map (new cat?)", "error", err)
	}
	for k, v := range m {
		uuidWindowMap.Store(k, v)
	}

	// for logging
	onceSkip := sync.Map{}
	onceOK := sync.Map{}

	unbacktracked := stream.Filter(ctx, func(ct cattrack.CatTrack) bool {
		uuid := ct.Properties.MustString("UUID", "")
		t := ct.MustTime()

		var w Window
		wl, ok := uuidWindowMap.Load(uuid)
		if !ok {
			w = Window{
				First: t,
				Last:  t,
			}
			uuidWindowMap.Store(uuid, w)
			return true
		}
		w = wl.(Window)

		if t.Before(w.Last) && t.After(w.First) {
			if _, ok := onceSkip.Load(uuid); !ok {
				c.logger.Warn("Skipping backtracks", "track", ct.StringPretty(),
					"uuid", uuid, "first", w.First, "last", w.Last)

				onceSkip.Store(uuid, struct{}{})
			}

			c.logger.Debug("Track out of window", "track", ct.StringPretty(), "first", w.First, "last", w.Last)
			return false
		}
		if _, ok := onceOK.Load(uuid); !ok {
			c.logger.Info("Found fresh tracks", "track", ct.StringPretty(),
				"uuid", uuid, "first", w.First, "last", w.Last)
			onceOK.Store(uuid, struct{}{})
		}
		if t.After(w.Last) {
			w.Last = t
		} else if t.Before(w.First) {
			w.First = t
		}
		uuidWindowMap.Store(uuid, w)
		return true
	}, in)
	return unbacktracked, onClose
}
