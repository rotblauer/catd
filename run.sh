#!/usr/bin/env bash

clear
rm -rf /tmp/catd
zcat ~/tdata/local/catsort/rye/tracks.json.gz \
  | go run . import --verbosity -5
