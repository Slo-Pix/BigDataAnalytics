"""Authoritative catalogue matching and on-demand public tracker observations."""

import logging
import re
import struct
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from typing import Annotated

import pyarrow.compute as pc
import pyarrow.dataset as ds
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from common.paths import CURATED
from ingest.tracker_scraper import DEFAULT_TRACKERS, TrackerError, UDPTracker
from portal.accounts import AccountStore, Actor, current_actor, now, require_roles

log = logging.getLogger(__name__)
TITLE_COLUMNS = ["infohash", "display_title", "category", "imdb_id", "steam_app_id"]
CHECK_CAPACITY = threading.BoundedSemaphore(2)


@lru_cache(maxsize=1)
def title_dataset():
    path = CURATED / "torrents_enriched"
    if not path.is_dir():
        raise HTTPException(503, "Enriched title catalogue is unavailable. Run the catalogue ETL first.")
    return ds.dataset(path, format="parquet", partitioning="hive")


def title_key(row: dict) -> str:
    if row["imdb_id"]:
        return f"{row['category']}|imdb|{row['imdb_id']}"
    if row["steam_app_id"]:
        return f"{row['category']}|steam|{row['steam_app_id']}"
    return f"{row['category']}|name|{row['display_title']}"


@lru_cache(maxsize=512)
def resolve_title(key: str) -> dict:
    parts = key.split("|", 2)
    if len(parts) != 3 or parts[0] not in ("movie", "tv", "game") or len(key) > 600:
        raise HTTPException(400, "Invalid catalogue title key.")
    category, kind, value = parts
    condition = ds.field("category") == category
    if kind == "imdb" and re.fullmatch(r"tt\d+", value):
        condition &= ds.field("imdb_id") == value
    elif kind == "steam" and value.isdigit() and len(value) <= 12:
        condition &= ds.field("steam_app_id") == int(value)
    elif kind == "name" and value.strip():
        condition &= ds.field("display_title") == value
    else:
        raise HTTPException(400, "Invalid catalogue identifier.")
    data = title_dataset()
    total = data.count_rows(filter=condition)
    rows = data.head(20, columns=TITLE_COLUMNS, filter=condition).to_pylist()
    if not rows:
        raise HTTPException(404, "Title is not present in the available historical catalogue.")
    first = rows[0]
    return {
        "key": key, "title": first["display_title"], "category": category,
        "imdb_id": first["imdb_id"], "steam_app_id": first["steam_app_id"],
        "matched_hashes": total, "hashes": [row["infohash"] for row in rows],
        "limited": total > len(rows),
    }


def search_titles(q: str, category: str = "all") -> list[dict]:
    q = q.strip()
    if len(q) < 2:
        raise HTTPException(400, "Enter at least two characters or a catalogue identifier.")
    if len(q) > 150 or category not in ("all", "movie", "tv", "game"):
        raise HTTPException(400, "Invalid title search.")
    condition = ds.field("category").isin(["movie", "tv", "game"])
    if category != "all":
        condition &= ds.field("category") == category
    match = pc.match_substring(ds.field("display_title"), q, ignore_case=True)
    if re.fullmatch(r"tt\d+", q):
        match |= ds.field("imdb_id") == q
    if q.isdigit() and len(q) <= 12:
        match |= ds.field("steam_app_id") == int(q)
    rows = title_dataset().head(500, columns=TITLE_COLUMNS, filter=condition & match).to_pylist()
    rows.sort(key=lambda row: (str(row["display_title"]).casefold() != q.casefold(), str(row["display_title"]).casefold()))
    keys = list(dict.fromkeys(title_key(row) for row in rows if row["display_title"]))[:20]
    return [resolve_title(key) for key in keys]


def check_title(title: dict) -> dict:
    def tracker_check(tracker: str):
        client = None
        try:
            client = UDPTracker(tracker, timeout=2, retries=0)
            rows = client.scrape(title["hashes"])
            observed = now()
            return [{
                "infohash": hash_value, "tracker": tracker, "seeders": seeders,
                "completed": completed, "leechers": leechers, "checked_at": observed,
            } for hash_value, seeders, completed, leechers in rows], None
        except (OSError, TrackerError, struct.error) as error:
            log.warning("Title check failed on %s: %s", tracker, error)
            return [], {"tracker": tracker, "message": str(error)}
        finally:
            if client:
                client.sock.close()

    observations, errors = [], []
    with ThreadPoolExecutor(max_workers=len(DEFAULT_TRACKERS)) as pool:
        for rows, error in pool.map(tracker_check, DEFAULT_TRACKERS):
            observations.extend(rows)
            if error:
                errors.append(error)
    per_hash = []
    for hash_value in title["hashes"]:
        rows = [row for row in observations if row["infohash"] == hash_value]
        per_hash.append({
            "infohash": hash_value, "max_seeders": max((row["seeders"] for row in rows), default=None),
            "max_leechers": max((row["leechers"] for row in rows), default=None),
            "responding_trackers": len(rows),
        })
    return {
        "title": title["title"], "key": title["key"], "category": title["category"],
        "checked_at": now(), "fresh": bool(observations),
        "max_seeders": max((row["seeders"] for row in observations), default=None),
        "matched_hashes": title["matched_hashes"], "sampled_hashes": len(title["hashes"]),
        "responding_trackers": len({row["tracker"] for row in observations}),
        "expected_trackers": len(DEFAULT_TRACKERS), "observations": observations,
        "per_hash": per_hash, "errors": errors,
        "notice": "Largest tracker-reported swarm count, not unique people or a total across torrents. Up to 20 known hashes checked; this does not discover new releases.",
    }


class TitleCheck(BaseModel):
    key: str | None = Field(None, max_length=600)
    portfolio_id: int | None = Field(None, ge=1)


def titles_router(store: AccountStore):
    router = APIRouter(prefix="/api/titles")

    @router.get("")
    def search(actor: Annotated[Actor, Depends(current_actor)],
               q: str = Query(min_length=2, max_length=150), category: str = "all"):
        store.limit(f"title-search:{actor.user_id}", 60)
        return {"rows": search_titles(q, category), "notice": "Up to 20 candidate titles from 500 matching records. Refine searches and verify editions/episodes."}

    @router.post("/check")
    def check(body: TitleCheck, actor: Annotated[Actor, Depends(current_actor)]):
        require_roles(actor, "admin", "analyst")
        store.limit(f"title-check:{actor.user_id}", 10)
        if body.portfolio_id:
            from portal.company import portfolio_item

            item = portfolio_item(store, actor, body.portfolio_id)
            title = resolve_title(item["key"])
        elif body.key:
            title = resolve_title(body.key)
        else:
            raise HTTPException(400, "Choose a matched title or an approved portfolio record.")
        if not CHECK_CAPACITY.acquire(blocking=False):
            raise HTTPException(429, "Two live checks are already running. Try again shortly.")
        try:
            result = check_title(title)
        finally:
            CHECK_CAPACITY.release()
        if body.portfolio_id:
            from portal.company import record_check

            record_check(store, actor, body.portfolio_id, result)
        else:
            with store.connect() as con:
                store.audit(con, actor, "title.checked", title["key"], f"{len(result['observations'])} observations")
        return result

    return router
