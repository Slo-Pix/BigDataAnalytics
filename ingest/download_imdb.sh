#!/usr/bin/env bash
# Download IMDb non-commercial datasets (https://developer.imdb.com/non-commercial-datasets/)
set -euo pipefail
OUT="$(dirname "$0")/../dataset/raw/imdb"; mkdir -p "$OUT"
for f in title.basics title.ratings title.akas title.principals title.episode; do
  wget -c -q --show-progress -O "$OUT/$f.tsv.gz" "https://datasets.imdbws.com/$f.tsv.gz"
done
echo "IMDb done"; ls -lh "$OUT"
