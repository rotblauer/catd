#!/usr/bin/env bash

set -e
set -x

myroot=/tmp/catd100_000/geojson

for kitty in ia rye; do
  for level in 13 16; do
    mkdir -p $(dirname ${myroot}/${kitty}-level-$level.json)
    zcat /tmp/catd100_000/tiled/source/${kitty}/s2_cells/level-$level-polygons.geojson.gz \
      | ndgeojson2geojsonfc > ${myroot}/${kitty}-level-$level.json
  done
done

cd $myroot

# https://stackoverflow.com/questions/11583562/how-to-kill-a-process-running-on-particular-port-in-linux
fuser -k --silent 8080/tcp || true
fuser -k --silent -SIGNAL 2 8080/tcp || true

pyserv-nocors.py 8010
