#!/usr/bin/env bash
# Start / stop the Spark Structured Streaming job in the background.
#   streaming/run_stream.sh start|stop|status
set -euo pipefail
cd "$(dirname "$0")/.."
PIDFILE=data/logs/swarm_stream.pid
case "${1:-start}" in
  start)
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then echo "already running"; exit 0; fi
    mkdir -p data/logs
    nohup uv run python -m streaming.swarm_stream > data/logs/swarm_stream.log 2>&1 &
    echo $! > "$PIDFILE"; echo "started pid $(cat "$PIDFILE"), log: data/logs/swarm_stream.log" ;;
  stop)
    [ -f "$PIDFILE" ] && kill "$(cat "$PIDFILE")" && rm -f "$PIDFILE" && echo stopped ;;
  status)
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then echo "running"; grep '^\[serving\]' data/logs/swarm_stream.log | tail -3; else echo "not running"; fi ;;
esac
