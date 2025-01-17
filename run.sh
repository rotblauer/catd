#!/usr/bin/env bash

tdata() {
#    echo
#    cat
#    zcat ~/tdata/${source_gz}
#    grep -E '2024-1[1,2]'
#    zcat "${HOME}/tdata/local/yyyy-mm/2023"*.gz "${HOME}/tdata/local/yyyy-mm/2024"*.gz
#     zcat "${HOME}"/tdata/edge.json.gz
#     zcat "${HOME}"/tdata/{devop,edge}.json.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2021"*.gz "${HOME}/tdata/local/yyyy-mm/2022"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2017"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2018"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2019"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2019-03"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2020"*.gz
#     zcat "${HOME}/tdata/local/yyyy-mm/2020-02"*.gz
#     zcat "${HOME}/tdata/local/yyyy-mm/2020"*.gz

#     zcat "${HOME}/tdata/local/yyyy-mm/2021-02"*.gz
#     zcat "${HOME}/tdata/local/yyyy-mm/2021-03"*.gz
#     zcat "${HOME}/tdata/local/yyyy-mm/2021-04"*.gz
#     zcat "${HOME}/tdata/local/yyyy-mm/2021-05"*.gz
#     zcat "${HOME}/tdata/local/yyyy-mm/2021-06"*.gz
#     zcat "${HOME}/tdata/local/yyyy-mm/2021-07"*.gz
#     zcat "${HOME}/tdata/local/yyyy-mm/2021-08"*.gz
#     zcat "${HOME}/tdata/local/yyyy-mm/2021"*.gz
#     zcat "${HOME}/tdata/local/yyyy-mm/2022"*.gz

#     zcat "${HOME}/tdata/local/yyyy-mm/2023-06"*.gz
#     zcat "${HOME}/tdata/local/yyyy-mm/2023-07"*.gz
#     zcat "${HOME}/tdata/local/yyyy-mm/2024-09"*.gz

   zcat "${HOME}/tdata/local/yyyy-mm/2023"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2024"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2024-09"*.gz
#    zcat "${HOME}/tdata/local/yyyy-mm/2024-1"*.gz

#     zcat "${HOME}"/tdata/master.json.gz
#    zcat "${HOME}"/tdata/{devop,edge}.json.gz
#     zcat "${HOME}"/tdata/20250103_500000.json.gz
#    zcat "${HOME}"/tdata/edge.json.gz
#
#  shopt -s globstar;
#  for f in "${HOME}"/tdata/local/yyyy-mm/**/*.gz; do
#    (( RANDOM % 4 )) && zcat "$f"
#  done
#    for f in $(seq -f "%02g" 11 12); do
#      zcat "${HOME}/tdata/local/yyyy-mm/2024-${f}"*.gz
#    done
#       for f in $(seq -f "%02g" 20 24); do
#         zcat "${HOME}/tdata/local/yyyy-mm/20${f}"*.gz
#       done
}

bump_tileservice() {
    { >&2 echo "Bumping mbtileserver" ; }
    if ! pgrep mbtileserver | tail -1 | xargs kill -HUP 2> /dev/null
    then nohup mbtileserver --port 3001 -d /tmp/catd/tiled/tiles --verbose --enable-reload-signal > /dev/null 2>&1 &
    fi

#     pkill -f 'mbtileserver --port 3001' || { echo "WARN: mbtileserver not running" ; }
#     nohup mbtileserver --port 3001 -d /tmp/catd/tiled/tiles --verbose --enable-reload-signal > /dev/null 2>&1 &
}
export -f bump_tileservice

tabula_rasa() {
    >&2 echo "WARN: Removing datadir"
    set -x
    # This way you get to look at maps while catd (re-)runs,
    # .mbtiles get overwritten with a mv, if all goes well.
#     rm -rf /tmp/catd/cats /tmp/catd/tiled/source # /tmp/catd/tiled/tiles
    rm -rf /tmp/catd
    { set +x ;} 2>/dev/null
}

restart_metrics() {
    cd "${HOME}"/dev/jkehres/docker-compose-influxdb-grafana
    docker-compose down --volumes
    docker-compose up --force-recreate --detach
    cd -
}

run() {
  # go tool pprof -http localhost:8001 http://localhost:6060/debug/pprof/heap
  # for i in $(seq 1 360); do curl -sK -v http://localhost:6060/debug/pprof/heap > heap.${i}.pprof && echo $(date) - heap.${i}.prof; sleep 60; done
  # go tool pprof -http localhost:8001 heap.5.pprof

  set -e
  set -x
  go install . || { echo "Install failed" && exit 1 ; }
  [[ -z "${RMCATS}" ]] || { >&2 echo "WARN RMCATS rming cats" && tabula_rasa ; }

  restart_metrics

  # https://github.com/rotblauer/docker-compose-influxdb-grafana-catd
  # docker-compose up --detach
  # docker-compose down --volumes
  export CATD_INFLUXDB_URL=http://localhost:8086
  export CATD_INFLUXDB_TOKEN=catdevelopmenttokeninfluxdb
  export CATD_INFLUXDB_ORG=cats
  export CATD_INFLUXDB_BUCKET=catmetrics
  tdata | catd populate \
    --datadir /tmp/catd \
    --verbosity 0 \
    --workers 0 \
    --sort true \
    --push-limit 0 \
    --tiled.skip-edge
#     --tiled.pending-after 15s \
#     --tiled.await-pending \


  zcat $(find /tmp/catd/cats/rye/tracks/*.gz | head -1) | wc -l
  { set +x ; } 2>/dev/null

  check=$?
  if [[ $check -ne 0 ]]; then
    echo "No tracks found or error in cat tracks"
    exit 1
  fi
  bump_tileservice &
}

time run |& tee --ignore-interrupt run.out



