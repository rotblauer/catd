package s2

import (
	"github.com/rotblauer/catd/types/cattrack"
)

type Indexer interface {
	Index(old, next Indexer) Indexer
	IsEmpty() bool
	ApplyToCattrack(idxr Indexer, ct cattrack.CatTrack) cattrack.CatTrack
	FromCatTrack(ct cattrack.CatTrack) Indexer
}
