#!/usr/bin/env bash

# Batches of 1_000 in 300ms.
# Batches of 100_000 in ~30s = 30_000ms - about 10x faster.

review() {
  for i in 100_000; do
    echo
    echo "- batch=${i} ---";
    shopt -s globstar
    local out=""
    for f in /tmp/catd${i}/{,cats/**/}*.geojson.gz; do
      l=$(zcat "$f" | wc -l)
      out="${out}
${l} ${f}"
    done;
    echo "${out}" | sort -r -k1 -n | tail -n 50
  done
}

tracksource() {
#    zcat ~/tdata/${source_gz}
#    cat
#    grep -E '2024-1[1,2]'
#    zcat "${HOME}/tdata/local/yyyy-mm/2023"*.gz "${HOME}/tdata/local/yyyy-mm/2024"*.gz
#     zcat "${HOME}"/tdata/edge.json.gz
     zcat "${HOME}"/tdata/{edge,devop}.json.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2021"*.gz "${HOME}/tdata/local/yyyy-mm/2022"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2019"*.gz "${HOME}/tdata/local/yyyy-mm/2020"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2024-1"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2024-09"*.gz
}

# BEWARE. Dev only.
# ctrl-cing the tee'd catd command will not allow catd to shutdown gracefully.
run() {
  set -e
  go install .
#  rm -rf /tmp/catd/cats/
#  rm -rf /tmp/catd/tiled/source/
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
  catd repro rye
}
run


