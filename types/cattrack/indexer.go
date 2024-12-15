package cattrack

// Indexer is an interface that defines the methods for indexing a CatTrack.
// By another name, a Reducer. Maybe, a dimension reducer.
type Indexer interface {
	// FromCatTrack associates a given cattrack value with an Indexer.
	// The indexer can choose to maintain a reference to the CatTrack or not.
	// Since the Indexer is responsible for managing the/a/some CatTrack's properties,
	// it should be able to read relevant data from the cattrack too,
	// and this is where you do that.
	// It should return a new Indexer instance.
	FromCatTrack(ct CatTrack) Indexer

	// IsEmpty returns true if the Indexer has no data.
	// The initial 'loaded' index value of an Indexer is considered always empty,
	// and may be nil.
	IsEmpty() bool

	// Index applies the next Indexer to the current Indexer.
	// This is where this accumulator is seduced.
	// The Indexer should return a new Indexer instance.
	Index(old, next Indexer) Indexer

	// ApplyToCatTrack applies the Indexer a given CatTrack,
	// usually the one that was used to create the Indexer.
	// The Indexer should return a new CatTrack value ref.
	// This is where you install the properties, or modify the geometry,
	// or do whatever cat track magic you need to do.
	ApplyToCatTrack(idxr Indexer, ct CatTrack) CatTrack
}
