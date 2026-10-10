#!/usr/bin/env bash
# Live classroom demo of the speed layer, isolated from the 10 Oct dataset:
#   scraper-demo (top ~2,000 busiest torrents, every ~15 s) -> Kafka topic tracker.scrapes.demo
#   -> Spark Structured Streaming (10 s micro-batches) -> dataset/stream/demo (Parquet) + PostgreSQL schema `demo`
#   -> Grafana "Live Demo" dashboard (refreshes every 5 s): http://localhost:3000/d/live-demo
#
#   demo/live_demo.sh start    reset the demo area and start everything (~1 min until the first numbers)
#   demo/live_demo.sh status   rows streamed so far
#   demo/live_demo.sh stop     stop the demo scraper and stream job
set -uo pipefail
cd "$(dirname "$0")/.."
COMPOSE="docker compose -f infra/docker-compose.yml"
PG_DEMO="postgresql://bda:bda@localhost:5432/torrent?options=-csearch_path%3Ddemo,public"
PIDFILE=logs/demo_stream.pid
N=${DEMO_TORRENTS:-2000}
psql() { docker exec postgres psql -U bda -d torrent -tAc "$1"; }

case "${1:-status}" in
  start)
    mkdir -p logs
    $COMPOSE up -d kafka postgres grafana >/dev/null 2>&1
    until docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 2; done
    echo "1/4 targets: the $N torrents with the most seeders on 10 Oct"
    psql "SELECT infohash FROM swarm_latest GROUP BY 1 ORDER BY max(seeders) DESC LIMIT $N" > dataset/raw/scrape_targets/demo.txt
    echo "2/4 reset demo topic, tables, Parquet and checkpoints"
    "$0" stop >/dev/null 2>&1
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic tracker.scrapes.demo >/dev/null 2>&1
    sleep 2
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
      --topic tracker.scrapes.demo --partitions 3 --replication-factor 1 >/dev/null
    docker exec -i -e PGOPTIONS="-c client_min_messages=warning" postgres psql -U bda -d torrent -q < infra/postgres/demo.sql
    psql "TRUNCATE demo.swarm_minute, demo.swarm_latest, demo.stream_batches" >/dev/null
    rm -rf dataset/stream/demo dataset/stream/_checkpoints_demo dataset/stream/jsonl_demo
    echo "3/4 Spark Structured Streaming (10 s micro-batches)"
    nohup uv run python -m streaming.swarm_stream --topic tracker.scrapes.demo --out dataset/stream/demo \
      --checkpoint dataset/stream/_checkpoints_demo --pg "$PG_DEMO" --trigger "10 seconds" > logs/demo_stream.log 2>&1 &
    echo $! > "$PIDFILE"
    echo "4/4 demo scraper"
    $COMPOSE --profile demo up -d --build scraper-demo >/dev/null 2>&1
    echo "running. Open http://localhost:3000/d/live-demo  (first numbers in ~1 min)"
    ;;
  status)
    echo "micro-batches: $(psql 'SELECT count(*) FROM demo.stream_batches')  rows: $(psql 'SELECT coalesce(sum(rows),0) FROM demo.stream_batches')  torrents: $(psql 'SELECT count(DISTINCT infohash) FROM demo.swarm_latest')"
    docker ps --filter name=scraper-demo --format 'scraper-demo: {{.Status}}'
    [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null && echo "stream job: running" || echo "stream job: stopped"
    ;;
  stop)
    $COMPOSE --profile demo stop scraper-demo >/dev/null 2>&1
    [ -f "$PIDFILE" ] && kill "$(cat "$PIDFILE")" 2>/dev/null; rm -f "$PIDFILE"
    echo "demo stopped"
    ;;
  *) echo "usage: $0 start|status|stop"; exit 1 ;;
esac
