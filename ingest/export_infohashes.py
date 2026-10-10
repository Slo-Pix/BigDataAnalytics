"""Export the scrape target list from the RARBG SQLite dump.

Movie / TV / game torrents only, in two parts:
  * the --recent most recently added (still-live swarms -> hourly dynamics, streaming demo)
  * --per-year random torrents from every earlier year 2007-2022 (age spread -> popularity decay curve)

Writes one infohash per line to dataset/raw/scrape_targets/infohashes.txt. The scraper re-reads this
file at the start of every round, so changes take effect without a restart.

Usage: uv run python -m ingest.export_infohashes --recent 60000 --per-year 2500
"""
import argparse
import sqlite3

from common.paths import RAW

WANTED = "(cat LIKE 'movies%' OR cat LIKE 'tv%' OR cat LIKE 'games%')"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(RAW / "rarbg" / "rarbg_db.sqlite"))
    ap.add_argument("--recent", type=int, default=60_000)
    ap.add_argument("--per-year", type=int, default=2_500)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    con = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    recent = con.execute(f"SELECT hash, dt FROM items WHERE {WANTED} ORDER BY dt DESC LIMIT ?", (args.recent,)).fetchall()
    cutoff = recent[-1][1]
    hashes = [h for h, _ in recent]
    print(f"recent: {len(recent):,} torrents added since {cutoff}")

    # Infohashes are SHA-1 digests, so ordering by a slice of the hash is a uniform, reproducible random sample.
    offset = args.seed % 32 + 1
    for year in range(2007, int(cutoff[:4]) + 1):
        rows = con.execute(
            f"SELECT hash FROM items WHERE {WANTED} AND dt >= ? AND dt < ? AND dt < ? "
            f"ORDER BY substr(hash, {offset}, 8) LIMIT ?",
            (f"{year}-01-01", f"{year + 1}-01-01", cutoff, args.per_year),
        ).fetchall()
        hashes += [r[0] for r in rows]
        print(f"  {year}: {len(rows):,}")

    hashes = list(dict.fromkeys(h.lower() for h in hashes))
    out = RAW / "scrape_targets" / "infohashes.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(hashes) + "\n")
    print(f"wrote {len(hashes):,} hashes to {out}")


if __name__ == "__main__":
    main()
