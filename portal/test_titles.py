"""Title resolution and tracker failure semantics without network access."""

import unittest
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

from portal.titles import check_title, resolve_title, search_titles


class TitleTests(unittest.TestCase):
    def setUp(self):
        resolve_title.cache_clear()

    def test_invalid_queries_and_keys_are_explicit_errors(self):
        for key in ("invalid", "movie|sql|anything", "game|steam|not-numeric"):
            with self.subTest(key=key), self.assertRaises(HTTPException):
                resolve_title(key)
        with self.assertRaises(HTTPException):
            search_titles("a")

    def test_failed_trackers_are_not_zero_seeders(self):
        title = {"title": "Example", "key": "movie|name|Example", "category": "movie",
                 "hashes": ["a" * 40], "matched_hashes": 1}
        with (
            patch("portal.titles.DEFAULT_TRACKERS", ["unavailable.example:80"]),
            patch("portal.titles.UDPTracker", side_effect=OSError("unreachable")),
            self.assertLogs("portal.titles", level="WARNING"),
        ):
            result = check_title(title)
        self.assertIsNone(result["max_seeders"])
        self.assertFalse(result["fresh"])
        self.assertEqual(len(result["errors"]), 1)

    def test_counts_use_maximum_not_sum_across_trackers(self):
        title = {"title": "Example", "key": "movie|name|Example", "category": "movie",
                 "hashes": ["a" * 40], "matched_hashes": 3}
        first, second = MagicMock(), MagicMock()
        first.scrape.return_value = [("a" * 40, 10, 100, 2)]
        second.scrape.return_value = [("a" * 40, 15, 200, 3)]
        with (
            patch("portal.titles.DEFAULT_TRACKERS", ["first:80", "second:80"]),
            patch("portal.titles.UDPTracker", side_effect=[first, second]),
        ):
            result = check_title(title)
        self.assertEqual(result["max_seeders"], 15)
        self.assertEqual(result["responding_trackers"], 2)
        self.assertEqual(result["sampled_hashes"], 1)
        self.assertEqual(result["matched_hashes"], 3)
        self.assertTrue(result["fresh"])
        first.sock.close.assert_called_once()
        second.sock.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
