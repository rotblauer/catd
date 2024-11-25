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

---

```
/tmp/catd100_000
├── cats
│  ├── chishiki
│  │  ├── laps.geojson.gz
│  │  ├── last_tracks.geojson.gz
│  │  ├── naps.geojson.gz
│  │  ├── s2.db
│  │  ├── s2_level-05.geojson.gz
│  │  ├── s2_level-08.geojson.gz
│  │  ├── s2_level-13.geojson.gz
│  │  ├── s2_level-16.geojson.gz
│  │  ├── s2_level-23.geojson.gz
│  │  ├── snaps.geojson.gz
│  │  ├── state.db
│  │  ├── tracks.geojson.gz
│  │  └── tripdetected.geojson.gz
│  ├── ia
│  │  ├── laps.geojson.gz
│  │  ├── last_tracks.geojson.gz
│  │  ├── naps.geojson.gz
│  │  ├── s2.db
│  │  ├── s2_level-05.geojson.gz
│  │  ├── s2_level-08.geojson.gz
│  │  ├── s2_level-13.geojson.gz
│  │  ├── s2_level-16.geojson.gz
│  │  ├── s2_level-23.geojson.gz
│  │  ├── snaps.geojson.gz
│  │  ├── state.db
│  │  ├── tracks.geojson.gz
│  │  └── tripdetected.geojson.gz
│  ├── iPhone
│  │  ├── laps.geojson.gz
│  │  ├── last_tracks.geojson.gz
│  │  ├── naps.geojson.gz
│  │  ├── s2.db
│  │  ├── s2_level-05.geojson.gz
│  │  ├── s2_level-08.geojson.gz
│  │  ├── s2_level-13.geojson.gz
│  │  ├── s2_level-16.geojson.gz
│  │  ├── s2_level-23.geojson.gz
│  │  ├── snaps.geojson.gz
│  │  ├── state.db
│  │  ├── tracks.geojson.gz
│  │  └── tripdetected.geojson.gz
│  ├── jr
│  │  ├── laps.geojson.gz
│  │  ├── last_tracks.geojson.gz
│  │  ├── naps.geojson.gz
│  │  ├── s2.db
│  │  ├── s2_level-05.geojson.gz
│  │  ├── s2_level-08.geojson.gz
│  │  ├── s2_level-13.geojson.gz
│  │  ├── s2_level-16.geojson.gz
│  │  ├── s2_level-23.geojson.gz
│  │  ├── snaps.geojson.gz
│  │  ├── state.db
│  │  ├── tracks.geojson.gz
│  │  └── tripdetected.geojson.gz
│  ├── kd
│  │  ├── laps.geojson.gz
│  │  ├── last_tracks.geojson.gz
│  │  ├── naps.geojson.gz
│  │  ├── s2.db
│  │  ├── s2_level-05.geojson.gz
│  │  ├── s2_level-08.geojson.gz
│  │  ├── s2_level-13.geojson.gz
│  │  ├── s2_level-16.geojson.gz
│  │  ├── s2_level-23.geojson.gz
│  │  ├── snaps.geojson.gz
│  │  ├── state.db
│  │  ├── tracks.geojson.gz
│  │  └── tripdetected.geojson.gz
│  ├── mat
│  │  ├── laps.geojson.gz
│  │  ├── last_tracks.geojson.gz
│  │  ├── naps.geojson.gz
│  │  ├── s2.db
│  │  ├── s2_level-05.geojson.gz
│  │  ├── s2_level-08.geojson.gz
│  │  ├── s2_level-13.geojson.gz
│  │  ├── s2_level-16.geojson.gz
│  │  ├── s2_level-23.geojson.gz
│  │  ├── snaps.geojson.gz
│  │  ├── state.db
│  │  ├── tracks.geojson.gz
│  │  └── tripdetected.geojson.gz
│  └── rye
│      ├── laps.geojson.gz
│      ├── last_tracks.geojson.gz
│      ├── naps.geojson.gz
│      ├── s2.db
│      ├── s2_level-05.geojson.gz
│      ├── s2_level-08.geojson.gz
│      ├── s2_level-13.geojson.gz
│      ├── s2_level-16.geojson.gz
│      ├── s2_level-23.geojson.gz
│      ├── snaps.geojson.gz
│      ├── state.db
│      ├── tracks.geojson.gz
│      └── tripdetected.geojson.gz
├── master.geojson.gz
└── tiled
    └── source
        ├── chishiki
        ├── ia
        ├── iPhone
        ├── jr
        ├── kd
        ├── mat
        └── rye

17 directories, 92 files
```