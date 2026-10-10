#!/usr/bin/env bash
# Run by cron @reboot during the live-collection day only: wait for Kafka (Docker restarts it and both
# scrapers on boot), then restart the Spark streaming job, the 21:00 stop job and the sleep blocker.
# Does nothing on any other day. Log: logs/on_reboot.log
cd "$(dirname "$0")/.." || exit 1
[ "$(date +%F)" = "2026-10-10" ] || exit 0
export PATH="$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin"
echo "$(date '+%F %T') reboot detected"
for i in $(seq 1 60); do docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break; sleep 10; done
if [ "$(date +%H%M)" -lt 2100 ]; then
  streaming/run_stream.sh start
  nohup streaming/stop_at.sh 21:00 >> logs/stop_at.log 2>&1 &
  secs=$(( $(date -d "21:05" +%s) - $(date +%s) ))
  nohup systemd-inhibit --what=sleep:idle --who=torrent-bda --why="live scraping" --mode=block sleep "$secs" >/dev/null 2>&1 &
  echo "$(date '+%F %T') stream + 21:00 stop restarted"
else
  # past 21:00: just drain Kafka into Parquet once and stop the scrapers
  docker compose -f infra/docker-compose.yml --profile scraper stop scraper scraper-wide
  streaming/run_stream.sh start; sleep 300; streaming/run_stream.sh stop
  echo "$(date '+%F %T') after 21:00: drained Kafka, scrapers stopped"
fi
