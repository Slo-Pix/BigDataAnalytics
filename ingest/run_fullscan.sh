#!/usr/bin/env bash
# One full pass over every movie/TV/game torrent in RARBG on one tracker (opentrackr), split into
# PARTS parallel scanners (each waits ~0.5 s per reply, so parallelism is what makes this fast).
# Each part writes its own JSONL folder. When all parts finish, every torrent found alive is added to
# the live scraper's every-round list. Log: logs/fullscan.log
set -uo pipefail
cd "$(dirname "$0")/.."
PARTS=${PARTS:-4}
T=dataset/raw/scrape_targets
OUT=dataset/stream/fullscan_jsonl
rm -f $T/fullscan.part.*
split -n l/$PARTS -d -a 1 $T/fullscan.txt $T/fullscan.part.
echo "$(date '+%F %T') full catalog scan started: $(wc -l < $T/fullscan.txt) torrents in $PARTS parts"
pids=()
for f in $T/fullscan.part.*; do
  p=${f##*.}
  uv run python -m ingest.tracker_scraper --hashes "$f" --once --trackers tracker.opentrackr.org:1337 \
      --jsonl-dir "$OUT/part$p" > "logs/fullscan_part$p.log" 2>&1 &
  pids+=($!)
done
fail=0
for pid in "${pids[@]}"; do wait "$pid" || fail=1; done
[ $fail = 0 ] || { echo "$(date '+%F %T') a scan part failed, see logs/fullscan_part*.log"; exit 1; }
echo "$(date '+%F %T') full scan finished: $(cat $OUT/part*/*/*.jsonl | wc -l) rows"
uv run python -m ingest.build_targets alive --fullscan-jsonl $OUT && echo "$(date '+%F %T') alive list updated"
