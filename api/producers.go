package api

import (
	"context"
	"github.com/rotblauer/catd/geo/clean"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
)

func (c *Cat) ProducerPipelines(ctx context.Context, in <-chan cattrack.CatTrack) error {

	c.logger.Info("Producer pipelines")
	defer c.logger.Info("Producer pipelines complete")

	// Clean and improve tracks for pipeline handlers.
	cleaned := c.CleanTracks(ctx, in)
	improved := c.ImprovedActTracks(ctx, cleaned)
	woffsets := TracksWithOffset(ctx, improved)

	areaPipeCh := make(chan cattrack.CatTrack, params.DefaultBatchSize)
	vectorPipeCh := make(chan cattrack.CatTrack, params.DefaultBatchSize)
	simpleIndexerCh := make(chan cattrack.CatTrack, params.DefaultBatchSize)
	stream.TeeMany(ctx, woffsets, areaPipeCh, vectorPipeCh, simpleIndexerCh)

	groundedArea := stream.Filter[cattrack.CatTrack](ctx, clean.FilterGrounded, areaPipeCh)

	nPipes := 3
	errs := make(chan error, nPipes)
	go func() { errs <- c.S2IndexTracks(ctx, groundedArea) }()
	go func() { errs <- c.CatActPipeline(ctx, vectorPipeCh) }()
	go func() { errs <- c.SimpleIndexer(ctx, simpleIndexerCh) }()

	c.logger.Debug("Producer pipelines waiting for completion")

	for i := 0; i < nPipes; i++ {
		select {
		case err := <-errs:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	close(errs)
	return nil
}

func (c *Cat) SimpleIndexer(ctx context.Context, in <-chan cattrack.CatTrack) error {

	c.logger.Info("Simple indexer")
	defer c.logger.Info("Simple indexer complete")

	indexer := &cattrack.StackerV1{}
	old := &cattrack.StackerV1{}
	if err := c.State.ReadKVUnmarshalJSON([]byte("state"), []byte("stacker"), old); err != nil {
		c.logger.Error("Failed to read stacker state (new cat?)", "error", err)
	}

	for track := range in {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		indexing := indexer.FromCatTrack(track)
		next := indexer.Index(old, indexing)
		*old = *next.(*cattrack.StackerV1)
	}

	c.logger.Info("Simple indexer complete")

	return c.State.StoreKVMarshalJSON([]byte("state"), []byte("stacker"), old)
}

/*

type ReverseGeocodeIndexer struct {
	Track    cattrack.CatTrack
	Indexer  cattrack.Indexer
	Dataset  string
	Geometry orb.Geometry
}

func (r ReverseGeocodeIndexer) Index(old, next cattrack.Indexer) cattrack.Indexer {
	if old.IsEmpty() {

	}
	r.Indexer.Index(old, next)
	return
}

func (r ReverseGeocodeIndexer) IsEmpty() bool {
	return r.Track.IsEmpty() || r.Indexer == nil || r.Indexer.IsEmpty()
}

func (r ReverseGeocodeIndexer) ApplyToCatTrack(idxr cattrack.Indexer, ct cattrack.CatTrack) cattrack.CatTrack {
	idxrT := idxr.(*ReverseGeocodeIndexer)

	cp.Geometry = r.Geometry
	return cp
}

func (r ReverseGeocodeIndexer) FromCatTrack(ct cattrack.CatTrack) cattrack.Indexer {
	i := r.Indexer.FromCatTrack(ct)
	i.(*ReverseGeocodeIndexer).Geometry = r.Geometry
	return i
}

type RGeoIndexerSink struct {
	datasetIndexKey map[string]func(loc rgeo.Location) string
}

var R *rgeo.Rgeo

func NewRGeoIndexerSink() (*RGeoIndexerSink, error) {
	var err error
	R, err = rgeo.New(rgeo.Countries10, rgeo.US_Counties10, rgeo.Provinces10, rgeo.Cities10)
	if err != nil {
		return nil, err
	}

	// See rgeo.Location fields.

	//	type Location struct {
	//	    Country      string `json:"country,omitempty"`
	//	    CountryLong  string `json:"country_long,omitempty"`
	//	    CountryCode2 string `json:"country_code_2,omitempty"`
	//	    CountryCode3 string `json:"country_code_3,omitempty"`
	//	    Continent    string `json:"continent,omitempty"`
	//	    Region       string `json:"region,omitempty"`
	//	    SubRegion    string `json:"subregion,omitempty"`
	//	    Province     string `json:"province,omitempty"`
	//	    ProvinceCode string `json:"province_code,omitempty"`
	//	    County       string `json:"county,omitempty"`
	//	    City         string `json:"city,omitempty"`
	//	}

	r := &RGeoIndexerSink{
		datasetIndexKey: map[string]func(loc rgeo.Location) string{
			"rgeo.Countries110":  func(loc rgeo.Location) string { return loc.CountryCode3 },
			"rgeo.Countries10":   func(loc rgeo.Location) string { return loc.CountryCode3 },
			"rgeo.Provinces10":   func(loc rgeo.Location) string { return loc.ProvinceCode },
			"rgeo.US_Counties10": func(loc rgeo.Location) string { return loc.County },
			"rgeo.Cities10":      func(loc rgeo.Location) string { return loc.City },
		},
	}

	cache := lru.New(params.DefaultBatchSize)
	var db *bbolt.DB

	cache.OnEvicted = func(key lru.Key, value interface{}) {
		// get or init the db
		if db == nil {
			db, err = bbolt.Open("/tmp/rgeo.db", 0600, nil)
			if err != nil {
				// errs <- err
				return
			}
		}
		db.Batch(func(tx *bbolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte(key.(string)))
			if err != nil {
				return err
			}

			// store the value in the bucket
			return nil
		})
	}

	//cache.RemoveOldest()
	cache.Clear() // :check: calls on-evicted for all :)

	indexerT := &ReverseGeocodeIndexer{}
	loadAndStore := func(key any, rgi cattrack.Indexer) (actual cattrack.Indexer, loaded bool) {
		var old, next cattrack.Indexer

		var v any
		v, loaded = cache.Get(key)
		if loaded {
			old = v.(cattrack.Indexer)
		} else {
			old = &cattrack.StackerV1{}
		}

		next = ct.FromCatTrack(ct.ct)
		actualIndexer := ct.Indexer.Index(old, next)

		cache.Add(key, actualIndexer)

		return wt{ct.ct, actualIndexer.(ReverseGeocodeIndexer)}, loaded
	}

	fn := func(ct cattrack.CatTrack) {
		for _, dataset := range R.DatasetNames() {

			loc, err := R.ReverseGeocodeWithGeometry(ct.Point(), dataset)
			if err != nil {
				// errs <- err
				continue
			}

			bucket := dataset
			keyFn := r.datasetIndexKey[dataset] // eg. (dataset=Countries10):"CountryCode3"
			key := keyFn(loc.Location)

			ict := indexerT.FromCatTrack(ct)
			ict.(*ReverseGeocodeIndexer).Geometry = loc.Geometry
			ict.(*ReverseGeocodeIndexer).Dataset = dataset

			actual, loaded := loadAndStore(dataset, key, wt)

			rgi.(*ReverseGeocodeIndexer).Geometry = loc.Geometry

			// an indexer just like  stackerv1 but with geometry for the dataset added
			// the indexer value will be evaluated and persisted (loadAndStore)
			// in sequential cache->db memory context order
		}
	}

	return r, nil
}

*/
