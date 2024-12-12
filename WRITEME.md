
now best `dc475745690069b7928abf64b5376689ea735da9`

2.5 

```
    PID USER      PRI  NI  VIRT   RES   SHR S CPU% MEM%   TIME+  Command                                                                 
   5987 ia         20   0 22.6G 5378M 1213M S 559. 11.2  6h45:16 goland                                                                  
   9453 ia         20   0 7559M 3750M  717M S  0.6  7.8 16:06.55 clion
   6041 ia         20   0 20.8G 3658M  176M S  4.5  7.6  1h19:25 webstorm
   6202 ia         20   0 60.8G 2008M 26140 S  2.6  4.2  5:06.97 copilot-language-server --stdio
 324115 ia         20   0 5276M 1223M  105M S 481.  2.6  1h58:16 catd populate --datadir /tmp/catd --verbosity 0 --batch-size 9000 --work
  96329 ia         20   0 8789M 1191M  250M S  0.6  2.5 30:05.21 firefox -P default -new-window /home/ia/dev/awesomeWM/awesome/awesomei/m
   8219 ia         20   0 8026M  805M 89384 S  0.6  1.7  7:48.86 jetbrains-toolbox --wait-for-pid 7758 --update-successful --minimize

```

```
2024/12/11 22:14:16 INFO Read tracks n=7,699,325 read.last=2019-03-05T23:52:30Z tps=8376 bps="3.5 MB" total.bytes="3.2 GB" running=16m15s           [0/9488]
2024/12/11 22:14:21 INFO Read tracks n=7,752,275 read.last=2019-03-06T07:17:19.001Z tps=8553 bps="3.6 MB" total.bytes="3.2 GB" running=16m20s
2024/12/11 22:14:26 INFO Read tracks n=7,794,322 read.last=2019-03-06T14:24:55.996Z tps=8540 bps="3.6 MB" total.bytes="3.3 GB" running=16m25s
2024/12/11 22:14:31 INFO Read tracks n=7,824,896 read.last=2019-03-07T00:41:36.999Z tps=8540 bps="3.5 MB" total.bytes="3.3 GB" running=16m30s
2024/12/11 22:14:36 INFO Read tracks n=7,869,020 read.last=2019-03-07T07:46:10.999Z tps=8385 bps="3.5 MB" total.bytes="3.3 GB" running=16m35s
2024/12/11 22:14:41 INFO Read tracks n=7,911,526 read.last=2019-03-07T13:52:05.158Z tps=8395 bps="3.5 MB" total.bytes="3.3 GB" running=16m40s
2024/12/11 22:14:46 INFO Read tracks n=7,951,360 read.last=2019-03-07T23:37:32Z tps=8395 bps="3.5 MB" total.bytes="3.3 GB" running=16m45s
2024/12/11 22:14:51 INFO Read tracks n=8,019,391 read.last=2019-03-08T16:46:25Z tps=8779 bps="3.7 MB" total.bytes="3.4 GB" running=16m50s
2024/12/11 22:14:56 INFO Read tracks n=8,058,129 read.last=2019-03-09T02:10:00.256Z tps=8779 bps="3.7 MB" total.bytes="3.4 GB" running=16m55s
2024/12/11 22:15:01 INFO Read tracks n=8,091,034 read.last=2019-03-09T16:50:24.999Z tps=8528 bps="3.6 MB" total.bytes="3.4 GB" running=17m0s
2024/12/11 22:15:06 INFO Read tracks n=8,137,499 read.last=2019-03-10T00:09:11.998Z tps=8589 bps="3.6 MB" total.bytes="3.4 GB" running=17m5s
2024/12/11 22:15:11 INFO Read tracks n=8,172,688 read.last=2019-03-10T05:56:40Z tps=8466 bps="3.6 MB" total.bytes="3.4 GB" running=17m10s
2024/12/11 22:15:16 INFO Read tracks n=8,203,604 read.last=2019-03-10T14:31:33.001Z tps=8283 bps="3.5 MB" total.bytes="3.4 GB" running=17m15s
2024/12/11 22:15:21 INFO Read tracks n=8,246,762 read.last=2019-03-11T00:46:53.991Z tps=8283 bps="3.5 MB" total.bytes="3.5 GB" running=17m20s
```


why so fast
tps>9000
2019-01*..-02-18 in 12m

---

- [ ] move params to params
- s2indexer tippe `maxzoom` `minzoom` for all levels
- more s2indexer for low levels without tiling
- s2indexer should use elapsed/offsets for activity weighting/moding

- [ ] ct.Time should be assuredly 1-second granularity
  use `UnixTime`!?

- [ ] track processor timer wrapper streamer -
  how long does it take to process a track for each/any of these streamers?
  where are the bottlenecks?

---

```sh
mbtileserver --port 3001 --cors '*' -d /tmp/catd100_000/tiled/tiles --verbose --enable-fs-watch

# Use node v16 (`$ nvm use 16`)
cd cattracks-explorer && yarn dev # will auto choose another port if 8080 not open.
```

- areas
nonunique s2 cells at varying levels.
heatmaps ahoy.
http://localhost:40223/public/?vector=http://localhost:3001/services/rye/s2_cells/level-06-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-07-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-08-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-09-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-10-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-11-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-12-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-13-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-14-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-15-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-16-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-17-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-18-polygons/tiles/{z}/{x}/{y}.pbf
http://localhost:8080/public/?vector=http://localhost:3001/services/ia/s2_cells/level-06-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-07-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-08-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-09-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-10-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-11-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-12-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-13-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-14-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-15-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-16-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-17-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-18-polygons/tiles/{z}/{x}/{y}.pbf

- vectors 
naps and laps
http://localhost:40223/public/?vector=http://localhost:3001/services/rye/naps/naps/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/laps/laps/tiles/{z}/{x}/{y}.pbf
http://localhost:8080/public/?vector=http://localhost:3001/services/ia/naps/naps/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/laps/laps/tiles/{z}/{x}/{y}.pbf

naps and laps AND cells! 
http://localhost:40223/public/?vector=http://localhost:3001/services/rye/s2_cells/level-05-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-08-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-09-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-11-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-12-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-13-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-14-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-16-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-17-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-18-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/naps/naps/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/laps/laps/tiles/{z}/{x}/{y}.pbf
http://localhost:8080/public/?vector=http://localhost:3001/services/ia/s2_cells/level-06-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-08-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-09-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-11-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-12-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-13-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-14-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-16-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-17-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/s2_cells/level-18-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/naps/naps/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/ia/laps/laps/tiles/{z}/{x}/{y}.pbf

---

url for levels 08, 11, 13, 16, 18, 20
localhost:40223/public/?vector=http://localhost:3001/services/rye/s2_cells/level-08-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-11-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-13-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-16-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-18-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-20-polygons/tiles/{z}/{x}/{y}.pbf

url for levels 08, 09, 11, 12, 13, 14, 16, 17, 18, 19, 20
localhost:40223/public/?vector=http://localhost:3001/services/rye/s2_cells/level-08-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-09-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-11-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-12-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-13-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-14-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-16-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-17-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-18-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-19-polygons/tiles/{z}/{x}/{y}.pbf,http://localhost:3001/services/rye/s2_cells/level-20-polygons/tiles/{z}/{x}/{y}.pbf


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
