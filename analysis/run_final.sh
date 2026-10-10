#!/usr/bin/env bash
# Run after live collection stops (21:00): rebuild the swarm summaries from the full day of live data and
# regenerate every analysis table and figure. ~10 minutes. Log: logs/final.log
set -euo pipefail
cd "$(dirname "$0")/.."
step() { echo "$(date '+%T') $1"; uv run python -m "$1" > "logs/final_${1##*.}.log" 2>&1; }
step etl.curate_swarm           # live slots / per-torrent day stats + full-scan decay table
for a in a1_activity a2_leak_lag a3_decay a4_wiki_interest a5_games a6_genres a7_swarm_day; do
  step "analysis.$a"
done
step dashboards.load_results            # results -> Postgres schema `analytics`
step dashboards.build_batch_dashboard   # Grafana "Batch Analytics" JSON
echo "$(date '+%T') done: figures in report/figures, tables in analysis/results, Grafana refreshed"
