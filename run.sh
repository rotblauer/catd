#!/usr/bin/env bash

set -e

rm -rf /tmp/catd && rm -f /tmp/catscann && rm -rf /tmp/catd1000{,0,00}

run() {
  go install . &&\
   for i in 1000{,0,00}; do
    rm -f /tmp/catscann;
    zcat ~/tdata/edge.20241008.json.gz \
    | catd import --datadir "/tmp/catd${i}" --verbosity 0 --batch-size ${i} --workers 6 --sort true \
    |& tee run.out; done
}
run

review() {
  for i in 1000{,0,00}; do
    echo
    echo "--- ${i} ---";
    for f in {ia,rye}/{tracks,laps,naps}; do
      t=/tmp/catd${i}/cats/$f.geojson.gz; echo "$t"; zcat "$t" \
      | wc -l ;
      done;
      done
}
review