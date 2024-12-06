package s2

import (
	"encoding/json"
	"github.com/rotblauer/catd/types/activity"
	"github.com/rotblauer/catd/types/cattrack"
	"log"
	"time"
)

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
	wt.Properties = props.Clone()
	return wt

	// Safest
	//cp := wt
	//props := cp.Properties.Clone()
	//for k, v := range items {
	//	props[k] = v
	//}
	//cp.Properties = props
	//return cp

	////Risky... ok?
	//cp := wt
	//for k, v := range items {
	//	cp.Properties[k] = v
	//}
	//return cp
}

// Index indexes the given CatTracks into the S2 cell index(es).
// Having WrappedTrack implement the Indexer is... maybe a great idea?
// Or maybe just "merge" the index structure to the wrapped track somehow.
func (WrappedTrack) Index(old, next Indexer) Indexer {

	nextWrapped := next.(WrappedTrack)
	nextProps := nextWrapped.Properties.Clone()

	nextCount := nextProps.MustInt("Count", 1)
	nextActivityName := nextProps.MustString("Activity")
	nextTime, err := time.Parse(time.RFC3339, nextProps.MustString("Time"))
	if err != nil {
		panic(err)
	}
	if nextTime.IsZero() {
		panic("Time is zero")
	}
	nextTimeFormatted := nextTime.Format(time.RFC3339)

	// On fresh powder...
	if old == nil || old.IsEmpty() {
		props := nextProps.Clone()
		props["Count"] = nextCount
		props["FirstTime"] = nextTimeFormatted
		props["LastTime"] = nextTimeFormatted
		props["Activity"] = nextActivityName
		for _, name := range activity.AllActivityNames {
			props["ActivityMode."+name] = nextProps.MustInt("ActivityMode."+name, 0)
		}
		props["ActivityMode."+nextActivityName] = nextCount // eg. "Walking": 1, "Running": 1, etc.
		out := nextWrapped.SafeSetProperties(props)
		return out
	}

	// From here we can assume that the old value is not empty.
	// The new value, however, may be "empty" (unindexed) or it may be an already-indexed WrappedTrack.

	oldWrapped := old.(WrappedTrack)
	oldProps := oldWrapped.Properties.Clone()
	updates := nextProps.Clone()

	oldCount := oldProps.MustInt("Count")
	updates["Count"] = oldCount + nextCount

	// Merge (sum) the existing activity tallies.
	for _, actName := range activity.AllActivityNames {
		oldActivityScore := oldProps.MustInt("ActivityMode." + actName)
		nextActivityScore := nextProps.MustInt("ActivityMode."+actName, 0)
		updates["ActivityMode."+actName] = oldActivityScore + nextActivityScore
	}

	// Get the ActivityMode with the greatest value, and assign the activity name to the Activity prop.
	greatest := 0
	name := "Unknown"
	for _, actName := range activity.AllActivityNames {
		if updates["ActivityMode."+actName].(int) > greatest {
			greatest = updates["ActivityMode."+actName].(int)
			name = actName
		}
	}
	updates["Activity"] = name

	// Check relative times to guard against unexpected unsorted tracks.
	oldFirstTimeFormatted := oldProps.MustString("FirstTime", "")
	if oldFirstTimeFormatted == "" {
		b, _ := json.MarshalIndent(oldWrapped, "", "  ")
		log.Fatalln(string(b))
	}
	oldFirstTime, err := time.Parse(time.RFC3339, oldFirstTimeFormatted)
	if err != nil {
		panic(err)
	}
	if nextTime.Before(oldFirstTime) {
		updates["FirstTime"] = nextTimeFormatted
	}

	oldLastTime, err := time.Parse(time.RFC3339, oldProps.MustString("LastTime"))
	if err != nil {
		panic(err)
	}
	if nextTime.After(oldLastTime) {
		updates["LastTime"] = nextTimeFormatted
	}

	out := nextWrapped.SafeSetProperties(updates)
	return out
}

func (wt WrappedTrack) IsEmpty() bool {
	_, ok := wt.Properties["Count"]
	return !ok
}
