"""Export the scrape target list from the RARBG SQLite dump.

Picks the N most recent movie / TV / game torrents (recent torrents still have live swarms)
and writes one infohash per line to data/raw/infohashes.txt for the tracker scraper.

Usage: uv run python -m ingest.export_infohashes --db data/raw/rarbg/rarbg_db.sqlite --n 100000
"""
import argparse
import sqlite3

from common.spark import RAW


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(RAW / "rarbg" / "rarbg_db.sqlite"))
    ap.add_argument("--n", type=int, default=100_000)
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    cols = [r[1] for r in con.execute("PRAGMA table_info(items)")]
    print("items columns:", cols)
    rows = con.execute(
        "SELECT hash FROM items WHERE cat LIKE 'movies%' OR cat LIKE 'tv%' OR cat LIKE 'games%' "
        "ORDER BY dt DESC LIMIT ?",
        (args.n,),
    ).fetchall()
    out = RAW / "infohashes.txt"
    out.write_text("\n".join(r[0].lower() for r in rows) + "\n")
    print(f"wrote {len(rows)} hashes to {out}")


if __name__ == "__main__":
    main()
