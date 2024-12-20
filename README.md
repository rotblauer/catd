# catd

```shell
zcat ~/tdata/master.json.gz | wc -l
245803174

du -sh /tmp/catd
18G     /tmp/catd
```

```shell
zcat master.json.gz | catd populate ...
...
2024/12/20 05:09:20 INFO Populate graceful dismount
1
Bumping mbtileserver

real    578m42.584s
user    4261m58.449s
sys     52m5.132s
```

### Cat commanders

### RPC API

### HTTP API

- `POST /populate` - Push your tracks here.


