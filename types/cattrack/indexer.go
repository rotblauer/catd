package cattrack

// Indexer is an interface that defines the methods for indexing a CatTrack.
// By another name, a Reducer.
type Indexer interface {
	Index(old, next Indexer) Indexer
	IsEmpty() bool
	ApplyToCatTrack(idxr Indexer, ct CatTrack) CatTrack
	FromCatTrack(ct CatTrack) Indexer
}
