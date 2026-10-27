"""Focused tests for the read-only company catalogue."""

import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import HTTPException

from portal.contentwatch import catalogue


class CatalogueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        base = Path(self.temp.name)
        path = base / "swarm_torrent_day"
        path.mkdir()
        rows = []
        for index, (title, category, year, seeders) in enumerate([
            ("A Movie", "movie", 2020, 7),
            ("Another MOVIE", "movie", 2021, 0),
            ("A Series", "tv", 2020, 3),
            ("A Game", "game", 2019, 12),
        ]):
            rows.append({
                "infohash": f"{index:040x}", "display_title": title,
                "category": category, "added_year": year, "seeders_median": seeders,
                "leechers_median": 1, "first_slot": datetime(2026, 10, 10, 5, tzinfo=UTC),
                "last_slot": datetime(2026, 10, 10, 15, tzinfo=UTC), "original_language": None,
                "imdb_id": None, "steam_app_id": None, "imdb_rating": None,
                "price_usd": None,
            })
        pq.write_table(pa.Table.from_pylist(rows), path / "part.parquet")
        patcher = patch("portal.contentwatch.CURATED", base)
        patcher.start()
        self.addCleanup(patcher.stop)

    def query(self, **overrides):
        args = {
            "q": "", "category": "all", "year": None, "min_seeders": 0,
            "active": False, "offset": 0, "limit": 25,
        }
        return catalogue(**(args | overrides))

    def test_returns_all_categories_and_explicit_provenance(self):
        result = self.query()
        self.assertEqual(result["total"], 4)
        self.assertEqual({row["category"] for row in result["rows"]}, {"movie", "tv", "game"})
        self.assertEqual(result["source"], "curated/swarm_torrent_day")
        self.assertEqual(result["mode"], "historical")
        self.assertTrue(result["rows"][0]["last_slot"].endswith("+00:00"))

    def test_combines_case_insensitive_search_category_year_and_activity(self):
        result = self.query(q="movie", category="movie", year=2020, active=True, min_seeders=5)
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["rows"][0]["display_title"], "A Movie")
        self.assertEqual(self.query(q="MOVIE")["total"], 2)

    def test_pagination_preserves_filtered_total(self):
        result = self.query(offset=1, limit=2)
        self.assertEqual(result["total"], 4)
        self.assertEqual([row["display_title"] for row in result["rows"]], ["Another MOVIE", "A Series"])
        self.assertEqual(self.query(offset=10)["rows"], [])

    def test_active_excludes_zero_and_minimum_is_inclusive(self):
        self.assertEqual(self.query(active=True)["total"], 3)
        self.assertEqual(self.query(min_seeders=12)["total"], 1)

    def test_search_is_literal_and_no_match_is_empty(self):
        self.assertEqual(self.query(q="%")["total"], 0)
        self.assertEqual(self.query(q="not in dataset")["rows"], [])
        self.assertEqual(self.query(q="  A Game  ")["total"], 1)

    def test_missing_dataset_is_an_explicit_service_error(self):
        with (
            patch("portal.contentwatch.CURATED", Path(self.temp.name) / "missing"),
            self.assertRaises(HTTPException) as raised,
        ):
            self.query()
        self.assertEqual(raised.exception.status_code, 503)


if __name__ == "__main__":
    unittest.main()
