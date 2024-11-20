
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
