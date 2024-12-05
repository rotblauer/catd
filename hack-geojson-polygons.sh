#!/usr/bin/env bash

mkdir /tmp/catd100_000/geojson 
cd /tmp/catd100_000/geojson

for level in 13 16; do
  zcat /tmp/catd100_000/tiled/source/ia/s2_cells/level-$level-polygons.geojson.gz > /tmp/catd100_000/geojson/level-$level-polygons.geojson
  cat level-$level-polygons.geojson | ndgeojson2geojsonfc > level-$level.json
done

pyserv-nocors.py 8010
