"""Fetch release dates from TMDB for the most-voted IMDb movies and TV shows.

1. Target list: top-N movies and TV series by IMDb vote count, released 2005-2023 (RARBG era),
   saved to dataset/raw/tmdb/targets.csv.
2. For each IMDb id:  /find/{imdb_id}  ->  TMDB id
      movie: /movie/{id}?append_to_response=release_dates   (all countries, all release types)
      tv:    /tv/{id}
   One JSON line per title is appended to dataset/raw/tmdb/tmdb_raw.jsonl. Titles already in the
   file are skipped, so the script can be stopped and re-run.

TMDB release types: 1 premiere, 2 theatrical (limited), 3 theatrical, 4 digital, 5 physical, 6 TV.

Usage: uv run python -m ingest.fetch_tmdb --movies 8000 --tv 2000
"""
import argparse
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from tqdm import tqdm

from common.env import load_env
from common.paths import RAW

API = "https://api.themoviedb.org/3"
OUT_DIR = RAW / "tmdb"
MOVIE_TYPES = {"movie", "tvMovie"}
TV_TYPES = {"tvSeries", "tvMiniSeries"}


def build_targets(n_movies: int, n_tv: int, years: tuple[int, int]) -> pd.DataFrame:
    imdb = RAW / "imdb"
    ratings = pd.read_csv(imdb / "title.ratings.tsv.gz", sep="\t", dtype={"tconst": str})
    basics = pd.read_csv(
        imdb / "title.basics.tsv.gz", sep="\t", na_values="\\N", quoting=3,
        usecols=["tconst", "titleType", "primaryTitle", "startYear"],
        dtype={"tconst": str, "titleType": str, "primaryTitle": str, "startYear": "Int64"},
    )
    df = basics.merge(ratings, on="tconst")
    df = df[df.startYear.between(*years)]
    movies = df[df.titleType.isin(MOVIE_TYPES)].nlargest(n_movies, "numVotes").assign(kind="movie")
    tv = df[df.titleType.isin(TV_TYPES)].nlargest(n_tv, "numVotes").assign(kind="tv")
    return pd.concat([movies, tv], ignore_index=True)


class RateLimiter:
    """Simple shared limiter: at most `rate` requests per second across threads."""

    def __init__(self, rate: float):
        self.interval, self.next_at, self.lock = 1.0 / rate, time.monotonic(), threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            self.next_at = max(self.next_at, now) + self.interval
            delay = self.next_at - self.interval - now
        if delay > 0:
            time.sleep(delay)


class TMDB:
    def __init__(self, token: str, rate: float):
        self.s = requests.Session()
        self.s.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})
        self.limiter = RateLimiter(rate)

    def get(self, path: str, **params) -> dict | None:
        for attempt in range(6):
            self.limiter.wait()
            try:
                r = self.s.get(f"{API}{path}", params=params, timeout=20)
            except requests.RequestException:
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                return None
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(float(r.headers.get("Retry-After", 2 ** attempt)))
                continue
            r.raise_for_status()
        raise RuntimeError(f"giving up on {path}")


def movie_fields(d: dict) -> dict:
    """Fields kept from a /movie/{id}?append_to_response=release_dates response."""
    return {
        "status": "ok", "tmdb_id": d.get("id"), "title": d.get("title"),
        "release_date": d.get("release_date"), "runtime": d.get("runtime"),
        "budget": d.get("budget"), "revenue": d.get("revenue"),
        "popularity": d.get("popularity"), "original_language": d.get("original_language"),
        "genres": [g["name"] for g in d.get("genres", [])],
        "release_dates": [
            {"country": c["iso_3166_1"], "type": x["type"], "date": x["release_date"][:10], "note": x.get("note") or None}
            for c in d.get("release_dates", {}).get("results", []) for x in c["release_dates"]
        ],
    }


def fetch_one(api: TMDB, row) -> dict:
    rec = {"imdb_id": row.tconst, "kind": row.kind, "imdb_title": row.primaryTitle,
           "imdb_year": int(row.startYear), "imdb_votes": int(row.numVotes), "imdb_rating": float(row.averageRating)}
    found = api.get(f"/find/{row.tconst}", external_source="imdb_id") or {}
    hits = found.get("movie_results" if row.kind == "movie" else "tv_results") or []
    if not hits:
        return rec | {"status": "not_found"}
    tmdb_id = hits[0]["id"]
    if row.kind == "movie":
        d = api.get(f"/movie/{tmdb_id}", append_to_response="release_dates") or {}
        return rec | movie_fields(d)
    d = api.get(f"/tv/{tmdb_id}") or {}
    return rec | {
        "status": "ok", "tmdb_id": tmdb_id, "title": d.get("name"),
        "first_air_date": d.get("first_air_date"), "last_air_date": d.get("last_air_date"),
        "number_of_seasons": d.get("number_of_seasons"), "number_of_episodes": d.get("number_of_episodes"),
        "popularity": d.get("popularity"), "original_language": d.get("original_language"),
        "genres": [g["name"] for g in d.get("genres", [])],
        "networks": [n["name"] for n in d.get("networks", [])],
        "in_production": d.get("in_production"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--movies", type=int, default=8000)
    ap.add_argument("--tv", type=int, default=2000)
    ap.add_argument("--years", type=int, nargs=2, default=(2005, 2023))
    ap.add_argument("--workers", type=int, default=20)
    ap.add_argument("--rate", type=float, default=35.0, help="max requests per second")
    args = ap.parse_args()

    load_env()
    token = os.environ.get("TMDB_TOKEN")
    if not token:
        raise SystemExit("TMDB_TOKEN missing: add it to .env")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    targets = build_targets(args.movies, args.tv, tuple(args.years))
    targets.to_csv(OUT_DIR / "targets.csv", index=False)
    print(f"targets: {(targets.kind == 'movie').sum()} movies, {(targets.kind == 'tv').sum()} tv")

    out_path = OUT_DIR / "tmdb_raw.jsonl"
    done = set()
    if out_path.exists():
        done = {json.loads(line)["imdb_id"] for line in out_path.open() if line.strip()}
    todo = [r for r in targets.itertuples(index=False) if r.tconst not in done]
    print(f"already fetched: {len(done)}, to fetch: {len(todo)}")

    api = TMDB(token, args.rate)
    stats = {"ok": 0, "not_found": 0, "error": 0}
    with out_path.open("a") as out, ThreadPoolExecutor(args.workers) as pool:
        futures = {pool.submit(fetch_one, api, r): r for r in todo}
        for f in tqdm(as_completed(futures), total=len(futures), unit="title"):
            try:
                rec = f.result()
            except Exception as e:  # keep going; the title is retried on the next run
                stats["error"] += 1
                tqdm.write(f"error {futures[f].tconst}: {e}")
                continue
            stats[rec["status"]] += 1
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print("done:", stats, "->", out_path)


if __name__ == "__main__":
    main()
