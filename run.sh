#!/usr/bin/env bash

tracksource() {
#    echo
#    cat
#    zcat ~/tdata/${source_gz}
#    grep -E '2024-1[1,2]'
#    zcat "${HOME}/tdata/local/yyyy-mm/2023"*.gz "${HOME}/tdata/local/yyyy-mm/2024"*.gz
#     zcat "${HOME}"/tdata/edge.json.gz
     zcat "${HOME}"/tdata/{devop,edge}.json.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2021"*.gz "${HOME}/tdata/local/yyyy-mm/2022"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2019"*.gz "${HOME}/tdata/local/yyyy-mm/2020"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2024-1"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2024-09"*.gz
}

bump_tileservice() {
    if ! pgrep mbtileserver | tail -1 | xargs kill -HUP 2> /dev/null
    then nohup mbtileserver --port 3001 -d /tmp/catd/tiled/tiles --verbose --enable-reload-signal > /dev/null 2>&1 &
    fi
}

run() {
  set -e
  go install .
  rm -rf /tmp/catd
  tracksource | catd populate \
    --datadir /tmp/catd \
    --verbosity 0 \
    --batch-size 10_000 \
    --workers 0 \
    --sort true \
    --tiled.skip-edge
  zcat /tmp/catd/cats/rye/tracks.geojson.gz | wc -l
  check=$?
  if [[ $check -ne 0 ]]; then
    echo "No tracks found or error in cat tracks"
    exit 1
  fi
  bump_tileservice &
}

repro() {
  set -e
  go install .
  catd --datadir /tmp/catd repro rye ia
  bump_tileservice &
}

#run
repro



