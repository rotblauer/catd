#!/usr/bin/env bash

set -e

rm -rf /tmp/catd && rm -f /tmp/catscann && rm -rf /tmp/catd1000{,0,00}

run() {
#  local source_gz="edge.20241008.json.gz"
  local source_gz="edge.json.gz"

  env AWS_BUCKETNAME=rotblauercatsnaps
  env AWS_REGION=us-east-2

  go install . &&\
  zcat ~/tdata/"${source_gz}" \
  | catd import --datadir "/tmp/catd" --verbosity 0 --batch-size 100000 --workers 6 --sort true \
  |& tee run.out
}
run

review() {
  for i in 100000; do
    echo
    echo "--- ${i} ---";
    for f in {ia,rye}/*.geojson.gz; do
      t=/tmp/catd${i}/cats/$f; echo "$t"; zcat "$t" \
      | wc -l ;
      done;
      done
}
review