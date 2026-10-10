"""Build scrape target lists in dataset/raw/scrape_targets/.

  fullscan  every movie / TV / game torrent in the RARBG dump -> fullscan.txt (one-off catalog pass)
  alive     torrents seen with >= 1 seeder, from the live serving table (Postgres swarm_latest) and,
            optionally, the full-scan JSONL output -> infohashes.txt (the list the live scraper re-reads
            every round). The previous list is kept as infohashes.<timestamp>.bak.

Files are written to a temp name and renamed, so the running scraper never reads a half-written list.

Usage:
    uv run python -m ingest.build_targets fullscan
    uv run python -m ingest.build_targets alive [--fullscan-jsonl dataset/stream/fullscan_jsonl]
"""
import argparse
import json
import shutil
import sqlite3
import time
from pathlib import Path

import psycopg

from common.paths import RAW, STREAM

TARGETS = RAW / "scrape_targets"
PG = "postgresql://bda:bda@localhost:5432/torrent"
WANTED = "(cat LIKE 'movies%' OR cat LIKE 'tv%' OR cat LIKE 'games%')"


def write_atomic(path: Path, hashes) -> int:
    hashes = sorted(set(h.lower() for h in hashes))
    tmp = path.with_suffix(".tmp")
    tmp.write_text("\n".join(hashes) + "\n")
    tmp.replace(path)
    return len(hashes)


def fullscan():
    con = sqlite3.connect(f"file:{RAW / 'rarbg' / 'rarbg_db.sqlite'}?mode=ro", uri=True)
    n = write_atomic(TARGETS / "fullscan.txt", (r[0] for r in con.execute(f"SELECT hash FROM items WHERE {WANTED}")))
    print(f"fullscan.txt: {n:,} torrents")


def alive(fullscan_jsonl: Path | None):
    with psycopg.connect(PG) as con:
        live = {r[0] for r in con.execute("SELECT infohash FROM swarm_latest GROUP BY 1 HAVING max(seeders) > 0")}
    print(f"alive in live scrapes so far: {len(live):,}")
    found = set()
    if fullscan_jsonl and fullscan_jsonl.exists():
        for f in fullscan_jsonl.rglob("*.jsonl"):
            for line in f.open():
                r = json.loads(line)
                if r["seeders"] > 0:
                    found.add(r["infohash"])
        print(f"alive in full catalog scan: {len(found):,} ({len(found - live):,} new)")
    out = TARGETS / "infohashes.txt"
    if out.exists():
        shutil.copy(out, TARGETS / f"infohashes.{time.strftime('%Y%m%d-%H%M%S')}.bak")
    n = write_atomic(out, live | found)
    print(f"infohashes.txt now: {n:,} alive torrents (live scraper picks it up next round)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["fullscan", "alive"])
    ap.add_argument("--fullscan-jsonl", type=Path, default=STREAM / "fullscan_jsonl")
    args = ap.parse_args()
    fullscan() if args.mode == "fullscan" else alive(args.fullscan_jsonl)


if __name__ == "__main__":
    main()
