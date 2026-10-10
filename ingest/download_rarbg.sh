#!/usr/bin/env bash
# Download the RARBG catalog dump (SQLite, June 2023 snapshot) from the Internet Archive and verify it.
# Source: https://archive.org/details/rarbg_db  — metadata only (titles, infohashes, categories, dates).
set -euo pipefail
OUT="$(dirname "$0")/../dataset/raw/rarbg"; mkdir -p "$OUT"
wget -c -q --show-progress -O "$OUT/rarbg_db.sqlite" "https://archive.org/download/rarbg_db/rarbg_db.sqlite"
echo "27eb3229f60d5745583a02b69ad0695d  $OUT/rarbg_db.sqlite" | md5sum -c -
