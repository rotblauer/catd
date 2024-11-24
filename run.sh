#!/usr/bin/env bash

rm -rf /tmp/catd*
rm -f /tmp/catscann

review() {
  for i in 100_000; do
    echo
    echo "- batch=${i} ---";
    shopt -s globstar
    local out=""
    for f in /tmp/catd${i}/cats/**/*.geojson.gz; do
      l=$(zcat "$f" | wc -l)
      out="${out}
${l} ${f}"
    done;
    echo "${out}" | sort -k1 -n | tail -n 5
  done
}
run() {
  set -e

#  local source_gz="edge.20241008.json.gz"
#  local source_gz="edge.json.gz"
  local source_gz="master.json.gz"

  go install . &&\
   for i in 100_000; do
    rm -f /tmp/catscann;
    zcat ~/tdata/"${source_gz}" \
    | catd populate --datadir "/tmp/catd${i}" \
      --verbosity 0 \
      --batch-size ${i} \
      --workers 10 \
      --sort true \
      --skip 10_000_000 \
    |& tee run.out; done

  review
}
run


