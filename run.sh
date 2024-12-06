#!/usr/bin/env bash

# Batches of 1_000 in 300ms.
# Batches of 100_000 in ~30s = 30_000ms - about 10x faster.

rm -rf /tmp/catd*
rm -f /tmp/catscann

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

# BEWARE. Dev only.
# ctrl-cing the tee'd catd command will not allow catd to shutdown gracefully.
run() {
  set -e

#  local source_gz="edge.20241008.json.gz"
#  local source_gz="edge.json.gz"
#  local source_gz="master.json.gz"
#  local source_gz="local/yyyy-mm/2024*.json.gz"
#  local source_gz="local/yyyy-mm/2024-07.json.gz"
#  local source_gz="local/yyyy-mm/2024-09.json.gz"
#  local source_gz="local/yyyy-mm/2024-12.json.gz"
#  local source_gz="local/yyyy-mm/2024-1*.json.gz"
  local source_gz="local/yyyy-mm/2024-1*.json.gz"

  go install . &&\
   for i in 100_000; do
    rm -f /tmp/catscann;
    zcat ~/tdata/${source_gz} | grep -E '2024-1' \
    | catd populate --datadir "/tmp/catd${i}" \
      --verbosity 0 \
      --batch-size ${i} \
      --workers 4 \
      --sort true \
      --tiled.skip-edge
    done

#      --tiled.off
#    |& tee run.out; done
      # --skip 1_000_000 \
  review
}
run


