# catd

Track your cats. 🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈



Cats in cat hats (hats per cat).
Cats have tracks, and snaps.
Tracks are still gzip batches NDJSON (GeoJSON; `Point`s, etc.).
Tracks are laps in time and space.
Naps are time in place.
Trip Detection! (Whole can of worms.)
Streams! (...and sorting).
Streams structure (concurrent) pipelines.
Then there's tiling. Thank goodness (and author/maintainer Rachel) for `tippecanoe`.
Tiling daemon is own thing. Receives HTTP RPC. 
Stores all its own data for reasons.

### Cat Commanders

```
_cuff 11-24_16:53:28 ~/dev/rotblauer/catd master *
zcat ~/tdata/master.json.gz | wc -l
245803174
```

`catd populate` - Import tracks from a gzip file.

```sh
time zcat ~/tdata/master.json.gz  \
| catd import \
  --verbosity 0 \
  --batch-size 100_000 \
  --workers 12 \
  --sort true
``` 

`catd serve` - Start the HTTP API web server.

### HTTP API

- `POST /populate` - Push your tracks here.