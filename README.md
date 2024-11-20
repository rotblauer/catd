# catd

The ultimate cat command.

Can accept `populate` tracks input via
- web server (HTTP) (batched JSON)
- stdin (NDJSON)

```sh
go build -o ./build/bin &&
   zcat ~/tdata/master.json.gz  | ./build/bin/catd import --verbosity 0 --batch-size 500_000 --workers 6 --sort false
```


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