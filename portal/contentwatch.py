"""Read-only catalogue serving for the local ExposureIQ prototype."""

from datetime import UTC
from typing import Literal

import pyarrow.compute as pc
import pyarrow.dataset as ds
from fastapi import APIRouter, HTTPException, Query

from common.paths import CURATED

router = APIRouter(prefix="/api/contentwatch")
COLUMNS = [
    "infohash", "display_title", "category", "added_year", "seeders_median",
    "leechers_median", "first_slot", "last_slot", "original_language",
    "imdb_id", "steam_app_id", "imdb_rating", "price_usd",
]


@router.get("/catalogue")
def catalogue(
    q: str = Query("", max_length=150),
    category: Literal["all", "movie", "tv", "game"] = "all",
    year: int | None = Query(None, ge=1900, le=2100),
    min_seeders: int = Query(0, ge=0, le=100_000_000),
    active: bool = False,
    offset: int = Query(0, ge=0, le=10_000),
    limit: int = Query(25, ge=1, le=100),
):
    path = CURATED / "swarm_torrent_day"
    if not path.is_dir():
        raise HTTPException(503, "Curated observations are unavailable. Run the batch analytics pipeline first.")
    data = ds.dataset(path, format="parquet", partitioning="hive")
    condition = ds.field("category").isin(["movie", "tv", "game"])
    if category != "all":
        condition &= ds.field("category") == category
    if q.strip():
        condition &= pc.match_substring(ds.field("display_title"), q.strip(), ignore_case=True)
    if year is not None:
        condition &= ds.field("added_year") == year
    if min_seeders or active:
        condition &= ds.field("seeders_median") >= max(min_seeders, 1 if active else 0)
    total = data.count_rows(filter=condition)
    rows = data.head(offset + limit, columns=COLUMNS, filter=condition).slice(offset, limit).to_pylist()
    for row in rows:
        for field in ("first_slot", "last_slot"):
            if row[field] is not None:
                row[field] = row[field].replace(tzinfo=UTC).isoformat()
    return {
        "rows": rows,
        "total": total,
        "offset": offset,
        "limit": limit,
        "source": "curated/swarm_torrent_day",
        "mode": "historical",
        "notice": "Historical tracker observations, not live detection. Rows represent torrents, not unique titles or people.",
    }
