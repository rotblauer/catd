# catd

Track your cats. 🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈‍⬛🐈🐈



Cats in cat hats.
Cats have tracks, and snaps.
Tracks are gzip batched NDGeoJSON.
Laps are tracks in time and space. (`LineString`). Vectors.
Naps are time in place. (`Point`). Scalars.
Streams! (... But sorting).
Streams structure (concurrent) pipelines.

Tiling daemon `tiled` is own thing, listens HTTP RPC. 
Stores all its own data.
God bless [tippecanoe](https://github.com/felt/tippecanoe).

Indexes tracks/S2 cell/level, ~6..~18.

### Cat Commanders

```
_cuff 11-24_16:53:28 ~/dev/rotblauer/catd master *
zcat ~/tdata/master.json.gz | wc -l
245803174
```

`catd populate` - Import tracks from a gzip file. Runs a `tiled` daemon.

`catd webd` - Start the HTTP API web server.

`catd tiled` - Start the `tippecanoe` tiling daemon.

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