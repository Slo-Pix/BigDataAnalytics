#!/usr/bin/env bash
# Download Wikimedia hourly pageview dumps for a date range.
# Usage: download_wiki_pageviews.sh 2026-09-01 7
set -euo pipefail
START="${1:-2026-09-01}"; DAYS="${2:-7}"
OUT="$(dirname "$0")/../data/raw/wiki"; mkdir -p "$OUT"
for d in $(seq 0 $((DAYS - 1))); do
  day=$(date -u -d "$START + $d day" +%Y%m%d)
  ym=$(date -u -d "$START + $d day" +%Y/%Y-%m)
  for h in $(seq -w 0 23); do
    f="pageviews-${day}-${h}0000.gz"
    wget -c -q -O "$OUT/$f" "https://dumps.wikimedia.org/other/pageviews/$ym/$f" || echo "FAILED $f"
  done
  echo "$(date -u +%T) day $day done"
done
echo "Wikipedia done"
