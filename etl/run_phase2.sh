#!/usr/bin/env bash
# Runs the rest of Phase 2 in order, one Spark job at a time (15 GB RAM machine), then goes live.
# Safe to re-run: each step overwrites its own output. Log: logs/phase2.log
set -uo pipefail
cd "$(dirname "$0")/.."
log() { echo "$(date '+%F %T') $*"; }
run() { log "START $1"; if uv run python -m "$1" > "logs/$2.log" 2>&1; then log "OK    $1"; else log "FAIL  $1 (see logs/$2.log)"; return 1; fi; }

log "waiting for etl.wiki_pageviews to finish"
while pgrep -f "[p]ython3 -m etl.wiki_pageviews" >/dev/null; do sleep 30; done
grep -q "^wrote" logs/etl_wiki.log && log "OK    etl.wiki_pageviews" || log "FAIL  etl.wiki_pageviews (see logs/etl_wiki.log)"

run etl.tmdb etl_tmdb
run etl.imdb etl_imdb

log "waiting for RARBG download + md5 check"
until grep -qE "rarbg_db.sqlite: (OK|FAILED)" logs/rarbg.log; do sleep 30; done
grep -q "rarbg_db.sqlite: OK" logs/rarbg.log || { log "FAIL  RARBG checksum mismatch"; exit 1; }
log "OK    RARBG download verified"

run etl.rarbg etl_rarbg || exit 1
run ingest.export_infohashes export_infohashes || exit 1

log "going live: scraper container + Spark streaming"
docker compose -f infra/docker-compose.yml --profile scraper up -d scraper && log "OK    scraper container started"
streaming/run_stream.sh start
log "phase 2 pipeline finished"
