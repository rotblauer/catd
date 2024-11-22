#!/usr/bin/env bash

set -e

rm -rf /tmp/catd && rm -f /tmp/catscann && rm -rf /tmp/catd100{,0}
go install . &&\
 for i in 100 1000 10000; do
  rm -f /tmp/catscann;
  zcat ~/tdata/edge.20241008.json.gz \
  | catd import --datadir "/tmp/catd${i}" --verbosity 0 --batch-size ${i} --workers 6 --sort true \
  |& tee run.out; done

for i in 100 1000 10000; do
  echo
  echo "--- ${i} ---";
  for f in {ia,rye}/{naps,laps}; do
    t=/tmp/catd${i}/cats/$f.geojson.gz; echo "$t"; zcat "$t" \
    | wc -l ;
    done;
    done