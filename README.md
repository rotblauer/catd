# catd

The ultimate Cat command, again.

## Design

This is intended as a V2 and drop-in replacement for [CatTracks V1](https://github.com/rotblauer/catTracks),
which ran (in its own iterations and variations) for nearly 7 years.

### Data Storage

The original core data store approach is unchanged: gzipped, append-only files.
This is a fast and very space-efficient pattern for track storage (nearly 400 million tracks take only about 6GB).

Geospatial data is schematized as GeoJSON, and encoded for storage in NDJSON. 
Any other app data is JSON, too. The API will generally accept and return JSON, or, in case of streams, NDJSON.

### Data Handling

Track data functions are generally patterned as _streams_ of tracks, and processed in _pipelines_.
This is an efficient way to handle large amounts of data, and readily allows for parallelization.
It is also a natural fit for its data storage pattern of line- (or otherwise) delimited track objects,
which can be read incrementally and then processed either directly in a pipeline, or batched.
Indeed, it's overkill for what the server will handle most often, namely POST requests of a few (e.g. 100)
tracks at a time, but it's a good fit for the `import` command and the grand schemes of generally extensible cat trackers.
Want to download your Cat Tracks? We'll stream them to you!

## API

### Commands

- `catd import` - Import tracks into the database.
  ```sh
  zcat ~/tdata/master.json.gz  | catd import --verbosity 0 --batch-size 500_000 --workers 6 --sort false
  ``` 

- `catd serve` - Start the API web server.
- `catd import-once-then-serve` - Migrate the cat server, [V1](https://github.com/rotblauer/catTracks) to `catd`.



### HTTP

- `POST /populate` - Push your tracks here.




---

- `api` - Application Protocol Interface
  This is where the API is defined. ... 
  It is where incoming data types are defined (ensured), and accepted.
  It tries to avoid business logic like _how and where_ to store track points.
  Instead, it wants to connect the "consumer/requestor/client" with a "provider/service/model". 
  This backend 

- `catdb` provides accessor functions to persistent and transient data stores.
  - `cache` contains some global in-memory-only caches and other caching logic.
  - `flat` contains functions for writing and reading flat - normally gzipped - files.
  - `kv` contains functions for a key-value database.
  - `postgres` contains functions for a Postgres+PostGIS database. When you need to index and query.