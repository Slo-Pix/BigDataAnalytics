"""Add Indian-language films to the TMDB data (the IMDb-votes target list is mostly Hollywood).

For each language, TMDB /discover/movie sorted by vote count (released 2005-2023) gives the top films;
each is fetched with /movie/{id}?append_to_response=release_dates,external_ids and appended to
dataset/raw/tmdb/tmdb_raw.jsonl in the same format as ingest.fetch_tmdb, tagged source="tmdb_discover_in".
Films already in the file (by IMDb id) and films without an IMDb id (cannot join to torrents) are skipped.

Usage: uv run python -m ingest.fetch_tmdb_india
"""
import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm

from common.env import load_env
from common.paths import RAW
from ingest.fetch_tmdb import TMDB, movie_fields

OUT = RAW / "tmdb" / "tmdb_raw.jsonl"
# language code -> number of top films to take
LANGUAGES = {"hi": 1500, "ta": 700, "te": 700, "ml": 600, "kn": 300, "bn": 200, "mr": 200, "pa": 100}


def discover(api: TMDB, lang: str, n: int, years: tuple[int, int]) -> list[int]:
    ids, page = [], 1
    while len(ids) < n and page <= 500:
        res = api.get("/discover/movie", with_original_language=lang, sort_by="vote_count.desc",
                      **{"primary_release_date.gte": f"{years[0]}-01-01", "primary_release_date.lte": f"{years[1]}-12-31"},
                      include_adult="false", page=page) or {}
        ids += [r["id"] for r in res.get("results", [])]
        if page >= res.get("total_pages", 0):
            break
        page += 1
    return ids[:n]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, nargs=2, default=(2005, 2023))
    ap.add_argument("--workers", type=int, default=20)
    ap.add_argument("--rate", type=float, default=35.0)
    args = ap.parse_args()

    load_env()
    api = TMDB(os.environ["TMDB_TOKEN"], args.rate)
    ratings = pd.read_csv(RAW / "imdb" / "title.ratings.tsv.gz", sep="\t", index_col="tconst")

    done_imdb = {json.loads(line)["imdb_id"] for line in OUT.open() if line.strip()}
    candidates = []
    for lang, n in LANGUAGES.items():
        ids = discover(api, lang, n, tuple(args.years))
        candidates += [(lang, i) for i in ids]
        print(f"{lang}: {len(ids)} films from discover")

    def fetch(item):
        lang, tmdb_id = item
        d = api.get(f"/movie/{tmdb_id}", append_to_response="release_dates,external_ids") or {}
        imdb_id = (d.get("external_ids") or {}).get("imdb_id")
        if not imdb_id:
            return None
        votes = ratings.loc[imdb_id] if imdb_id in ratings.index else None
        year = (d.get("release_date") or "")[:4]
        return {
            "imdb_id": imdb_id, "kind": "movie", "source": "tmdb_discover_in", "discover_language": lang,
            "imdb_title": d.get("title"), "imdb_year": int(year) if year.isdigit() else None,
            "imdb_votes": int(votes["numVotes"]) if votes is not None else None,
            "imdb_rating": float(votes["averageRating"]) if votes is not None else None,
        } | movie_fields(d)

    stats = {"added": 0, "already_had": 0, "no_imdb_id": 0}
    with OUT.open("a") as out, ThreadPoolExecutor(args.workers) as pool:
        futures = [pool.submit(fetch, c) for c in candidates]
        for f in tqdm(as_completed(futures), total=len(futures), unit="film"):
            rec = f.result()
            if rec is None:
                stats["no_imdb_id"] += 1
            elif rec["imdb_id"] in done_imdb:
                stats["already_had"] += 1
            else:
                done_imdb.add(rec["imdb_id"])
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                stats["added"] += 1
    print("done:", stats)


if __name__ == "__main__":
    main()
