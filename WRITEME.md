
```
./hack-geojson-polygons.sh
http://localhost:40223/public/?geojson=http://localhost:8010/level-13.json,http://localhost:8010/level-16.json
```

```
http://localhost:40223/puablic/?geojson=http://localhost:8010/out.json
```

```
http://localhost:40223/public/?vector=http://localhost:3001/services/ia/naps/naps/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/laps/laps/tiles/{z}/{x}/{y}.pbf
http://localhost:8080/public/?vector=http://localhost:3001/services/rye/naps/naps/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/laps/laps/tiles/{z}/{x}/{y}.pbf

http://localhost:40223/public/?vector=http://localhost:3001/services/ia/s2_cells/level-16-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-13-polygons/tiles/{z}/{x}/{y}.pbf
http://localhost:8080/public/?vector=http://localhost:3001/services/rye/s2_cells/level-16-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-13-polygons/tiles/{z}/{x}/{y}.pbf
```

---

```
http://localhost:8080/public/?vector=http://localhost:3001/services/rye/naps/naps_edge/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/laps/laps_edge/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/tripdetected/tripdetected_edge/tiles/{z}/{x}/{y}.pbf
http://localhost:8080/public/?vector=http://localhost:3001/services/rye/naps/naps_edge/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/laps/laps_edge/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/tracks/tracks_edge/tiles/{z}/{x}/{y}.pbf
```

---

Track batch optimization. (?)

`catd populate` how to synchronise/serialize/deserialize per-cat.
Faster cat hats. Collapse cat batches? (but CatActing edges?).
(go Batch.x;go batch.y;go batch.z).then(collapse(batch.*)) pattern 
worked pretty well for first try cat sorting hat. 

Cats tdata store CAT/YYYY/MM. (?) Might help with development.

Tiler postponed init whole state after `catd populate`. 
Walk cat dirs? Not if different machine. 
Tiler RPC HTTP streaming.

Cat scanner holds un-closed batches in memory until EOF flush.
Should flush periodically (chishiki no more points after 2020.)

Cat error handling. Especially `Cat.Populate`.
Return first?

Trip detector algos should fan-out/fan-in (worker merger).

Cat last known. Last lap. Last nap. - Latest state (recorded and inferred).
Log cat laps!

S2.Unique indexing. 
On-Dupe(level)(track, dupething). On-New(level)(track, newthing()).
Needs to be callable from zero-value cacher as well as stateful storer. 
```
type S2LevelTracker interface {
  OnDupe(stored, ct) (store CatTrack)
  OnNew(ct) (store CatTrack) 
}
type S2Eventer struct {
  Level00Handler S2LevelTracker
  Level01Handler S2LevelTracker
  Level03Handler S2LevelTracker
  ...
  Level30Handler S2LevelTracker
}
or
type S2Eventer []S2LevelTracker
  -> S2Eventer[CellLevel].OnDupe(ct, stored)
```

Trip detection actually cat act detection. Duh.
```
Cat.Acting.Add(track)
[
Acting.RestoreState()
Acting.Add(track)
Acting.State()
Acting.StoreState()
Acting.On(Act.Started(ActFilter{}))
Acting.On(Act.Completed(ActFilter{Lap.Duration > 120}))
- ie. store lap, upload Strava, ...
]
Cat.Acting.State().Activities() => 
  {Act: Confidence}
  {Act: Confidence}
  {Act: Confidence}
```
Allow nap-consolidator to grow/shrink eligible area (ie re teleport results when Stationary).
Compare ct.point to nap.edge (nap.centroid:radius or nap.bbox:nearest).



---

`TripDetector`. Just realized my stupidity.
Shoulda made an `ActivityFixer` stater. Then split on activities.
Already know what Moving is. Stationary is stationary.
Naps and laps. Ha.
Slow too.
Need to join/smooth activities (bad/doubtful client reports).
Is this what the TripDetector does?
Should be fan-in merger.

---

mbtileserver --port 3001 --cors '*' -d /tmp/catd100_000/tiled/tiles/ --verbose --enable-fs-watch

cattracks-explorer
```
http://localhost:8080/public/?vector=http://localhost:3001/services/rye/laps/laps/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/naps/naps/tiles/{z}/{x}/{y}.pbf
http://localhost:8080/public/?vector=http://localhost:3001/services/rye/tracks/tracks/tiles/{z}/{x}/{y}.pbf
http://localhost:8080/public/?vector=http://localhost:3001/services/rye/laps/laps/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/tracks/tracks/tiles/{z}/{x}/{y}.pbf

compare tripdetected vs raw tracks
http://localhost:8080/public/?vector=http://localhost:3001/services/rye/tracks/tracks/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/tripdetected/tripdetected/tiles/{z}/{x}/{y}.pbf

http://localhost:8080/public/?vector=http://localhost:3001/services/ia/laps/laps/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/naps/naps/tiles/{z}/{x}/{y}.pbf
```

```shell
fatal error: concurrent map read and map write

goroutine 4679 [running]:
github.com/rotblauer/catd/types/cattrack.(*CatTrack).Time(0xc002b042d0)
        /home/ia/dev/rotblauer/catd/types/cattrack/cattrack.go:36 +0x7f
github.com/rotblauer/catd/types/cattrack.(*CatTrack).MustTime(...)
        /home/ia/dev/rotblauer/catd/types/cattrack/cattrack.go:48
github.com/rotblauer/catd/geo/cleaner.TeleportationFilter.func1()
        /home/ia/dev/rotblauer/catd/geo/cleaner/teleportation.go:32 +0x1de
created by github.com/rotblauer/catd/geo/cleaner.TeleportationFilter in goroutine 4746
        /home/ia/dev/rotblauer/catd/geo/cleaner/teleportation.go:15 +0xa5

```

- (event) StoreTracks(tracks) -> tpp
- (event) StoreLap(lap) -> tpl
- (event) StoreNap(nap) -> tpn

---

Careful with catching signal interruptions with `|& tee run.out`.
It seems `import` is not able to catch the interrupt and does 
not exit gracefully, which means important state is lost.

```
2024/11/19 11:48:33 WARN Invalid track, mismatched cat want=rye got=rye
2024/11/19 11:48:33 WARN Blocking on store
2024/11/19 11:48:33 WARN Invalid track, mismatched cat want=rye got=jlc
2024/11/19 11:48:33 WARN Invalid track, mismatched cat want=rye got=rye
2024/11/19 11:48:33 WARN Invalid track, mismatched cat want=rye got=rye
2024/11/19 11:48:33 WARN Blocking on store
2024/11/19 11:48:33 INFO Restored trip-detector state cat=jlc last=2018-11-01T23:34:13.464Z lap=false
panic: runtime error: index out of range [-1]

goroutine 642 [running]:
github.com/rotblauer/catd/api.LapTracks({0xad1980, 0xc0000b4a50}, {0x9ee5a1, 0x3}, 0xc005168600)
        /home/ia/dev/rotblauer/catd/api/lap.go:26 +0x429
github.com/rotblauer/catd/api.PopulateCat.func3()
        /home/ia/dev/rotblauer/catd/api/populate.go:93 +0x1bb
created by github.com/rotblauer/catd/api.PopulateCat in goroutine 329
        /home/ia/dev/rotblauer/catd/api/populate.go:78 +0x4ae
exit status 2

```
