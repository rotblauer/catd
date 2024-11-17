package events

import (
	"github.com/ethereum/go-ethereum/event"
	"github.com/rotblauer/catd/types/cattrack"
)

// NewStoredTrackFeed is emitted for every new CatTrack that is successfully persisted.
var NewStoredTrackFeed = event.FeedOf[*cattrack.CatTrack]{}

// HTTPPopulateFeed is a feed of CatTracks as they are pushed to the server.
// The tracks included as the payload should be expected to be nearly-exactly as they are received.
// They will have been decoded and sorted, but not validated, deduped, nor necessarily even persisted.
// A reminder, too, that this event is emitted only in the context of an HTTP request.
// catd supports other protocols for cat track population, such as reading stdin.
var HTTPPopulateFeed = event.FeedOf[[]*cattrack.CatTrack]{}
