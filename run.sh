#!/usr/bin/env bash

set -e

rm -rf /tmp/catd && rm -f /tmp/catscann && rm -rf /tmp/catd1000{,0,00}

run() {
#  local source_gz="edge.20241008.json.gz"
  local source_gz="edge.json.gz"

  go install . &&\
   for i in 100000; do
    rm -f /tmp/catscann;
    zcat ~/tdata/"${source_gz}" \
    | catd import --datadir "/tmp/catd${i}" --verbosity 0 --batch-size ${i} --workers 6 --sort true \
    |& tee run.out; done
}
run

review() {
  for i in 100000; do
    echo
    echo "- batch=${i} ---";
    shopt -s globstar
    for f in /tmp/catd${i}/cats/**/*.geojson.gz; do
      echo "$f"
      zcat "$f" \
      | wc -l
    done;
  done
}
review