#!/usr/bin/env bash
# Graceful end of live collection at a given local time:
#   1. stop the scraper (no new messages)  2. let Spark drain Kafka for 3 minutes  3. stop the stream job
# Usage: streaming/stop_at.sh 17:00
set -uo pipefail
cd "$(dirname "$0")/.."
target=$(date -d "${1:-17:00}" +%s)
echo "$(date '+%F %T') will stop live collection at $(date -d @"$target" '+%F %T')"
while [ "$(date +%s)" -lt "$target" ]; do sleep 30; done
docker compose -f infra/docker-compose.yml --profile scraper stop scraper scraper-wide
echo "$(date '+%F %T') scrapers stopped; draining Kafka into Parquet for 3 min"
sleep 180
streaming/run_stream.sh stop
echo "$(date '+%F %T') stream job stopped. Rows applied: $(docker exec postgres psql -U bda -d torrent -tAc 'select sum(rows) from stream_batches')"
