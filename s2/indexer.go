package s2

import (
	"encoding/json"
	"fmt"
	"github.com/rotblauer/catd/types/cattrack"
	"time"
)

type Indexer interface {
	Index(old, next Indexer) Indexer
	IsEmpty() bool
}

func UnmarshalIndexer(v []byte) (Indexer, error) {
	var targetIndexCountT IndexCountT
	if err := json.Unmarshal(v, &targetIndexCountT); err == nil {
		return targetIndexCountT, nil
	}
	var targetWrappedTrack WrappedTrack
	if err := json.Unmarshal(v, &targetWrappedTrack); err == nil {
		return targetWrappedTrack, nil
	}
	// TODO: add other possible types
	return nil, fmt.Errorf("unknown type")
}

// IndexCountT is an Indexer that counts the number of elements.
// It is an example of how to implement the Indexer interface.
type IndexCountT struct {
	Count int
}

func (it IndexCountT) Index(old, next Indexer) Indexer {
	if old == nil || old.IsEmpty() {
		old = IndexCountT{}
	}
	return IndexCountT{
		Count: old.(IndexCountT).Count + next.(IndexCountT).Count,
	}
}

func (it IndexCountT) IsEmpty() bool {
	return it.Count == 0
}

// WrappedTrack is an Indexer that wraps a CatTrack.
type WrappedTrack cattrack.CatTrack

func (wt WrappedTrack) MarshalJSON() ([]byte, error) {
	return json.Marshal(cattrack.CatTrack(wt))
}

func (wt *WrappedTrack) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, (*cattrack.CatTrack)(wt))
}

func (wt WrappedTrack) SafeSetProperties(items map[string]any) WrappedTrack {
	props := wt.Properties.Clone()
	for k, v := range items {
		props[k] = v
	}
	wt.Properties = props
	return wt
}

// Index indexes the given CatTracks into the S2 cell index(es).
// Having WrappedTrack implement the Indexer is... maybe a great idea?
// Or maybe just "merge" the index structure to the wrapped track somehow.
func (wt WrappedTrack) Index(old, next Indexer) Indexer {
	cp := wt
	nextWrapped := next.(WrappedTrack)
	nextCount := nextWrapped.Properties.MustInt("Count", 1)
	nextActivity := nextWrapped.Properties.MustString("Activity", "Unknown")
	nextTime, err := time.Parse(time.RFC3339, nextWrapped.Properties.MustString("Time", ""))
	if err != nil {
		panic(err)
	}
	if old == nil || old.IsEmpty() {
		props := map[string]any{
			"Count":                   nextCount,
			"Activity":                nextActivity,
			"FirstTime":               nextTime,
			"ActivityMode.Unknown":    nextWrapped.Properties.MustInt("ActivityMode.Unknown", 0),
			"ActivityMode.Stationary": nextWrapped.Properties.MustInt("ActivityMode.Stationary", 0),
			"ActivityMode.Walking":    nextWrapped.Properties.MustInt("ActivityMode.Walking", 0),
			"ActivityMode.Running":    nextWrapped.Properties.MustInt("ActivityMode.Running", 0),
			"ActivityMode.Bike":       nextWrapped.Properties.MustInt("ActivityMode.Bike", 0),
			"ActivityMode.Automotive": nextWrapped.Properties.MustInt("ActivityMode.Automotive", 0),
			"ActivityMode.Fly":        nextWrapped.Properties.MustInt("ActivityMode.Fly", 0),
		}
		props["ActivityMode."+nextActivity] = nextCount // eg. "Walking": 1, "Running": 1, etc.
		cp = cp.SafeSetProperties(props)
		return cp
	}

	oldWrapped := old.(WrappedTrack)
	updates := map[string]any{}

	oldCount := oldWrapped.Properties.MustInt("Count", 1)
	updates["Count"] = oldCount + nextCount

	for _, act := range []string{"Unknown", "Stationary", "Walking", "Running", "Bike", "Automotive", "Fly"} {
		oldActivityScore := oldWrapped.Properties.MustInt("ActivityMode."+act, 0)
		nextActivityScore := nextWrapped.Properties.MustInt("ActivityMode."+act, 0)
		updates["ActivityMode."+act] = oldActivityScore + nextActivityScore
	}

	oldActivityScore := oldWrapped.Properties.MustInt("ActivityMode."+nextActivity, 1)
	updates["ActivityMode."+nextActivity] = oldActivityScore + nextCount

	// Get the ActivityMode with the greatest value, and assign the activity name to the Activity prop.
	greatest := 0
	name := "Unknown"
	for _, act := range []string{"Unknown", "Stationary", "Walking", "Running", "Bike", "Automotive", "Fly"} {
		if updates["ActivityMode."+act].(int) > greatest {
			greatest = updates["ActivityMode."+act].(int)
			name = act
		}
	}
	updates["Activity"] = name

	updates["LastTime"] = nextTime

	return oldWrapped.SafeSetProperties(updates)
}

func (wt WrappedTrack) IsEmpty() bool {
	_, ok := wt.Properties["Count"]
	return !ok
}
