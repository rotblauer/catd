#!/usr/bin/env bash

tracksource() {
#    cat
#    zcat ~/tdata/${source_gz}
#    grep -E '2024-1[1,2]'
#    zcat "${HOME}/tdata/local/yyyy-mm/2023"*.gz "${HOME}/tdata/local/yyyy-mm/2024"*.gz
#     zcat "${HOME}"/tdata/edge.json.gz
     zcat "${HOME}"/tdata/{edge,devop}.json.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2021"*.gz "${HOME}/tdata/local/yyyy-mm/2022"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2019"*.gz "${HOME}/tdata/local/yyyy-mm/2020"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2024-1"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2024-09"*.gz
}

run() {
  set -e
  go install .

  rm -rf /tmp/catd_tmp

  rm -rf /tmp/catd/cats/
  rm -rf /tmp/catd/tiled/source/

  tracksource \
  | catd populate \
    --datadir /tmp/catd \
    --verbosity 0 \
    --batch-size 10_000 \
    --workers 0 \
    --sort true \
    --tiled.skip-edge

  { pgrep mbtileserver | tail -1 | xargs kill -HUP ;} || true;
}

repro() {
  catd --datadir /tmp/catd_tmp repro rye
}

run


