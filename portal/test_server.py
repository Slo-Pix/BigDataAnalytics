"""Focused middleware checks without launching the shared streaming services."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import Request
from fastapi.responses import JSONResponse

from portal.accounts import AccountStore
from portal.server import protect_workspace


class MiddlewareTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = AccountStore(Path(self.temp.name) / "test.sqlite3")
        self.account_patch = patch("portal.server.accounts", self.store)
        self.account_patch.start()
        self.addCleanup(self.account_patch.stop)

    async def request(self, path, headers=None, method="POST"):
        request = Request({
            "type": "http", "method": method, "path": path, "scheme": "http",
            "server": ("127.0.0.1", 8800), "query_string": b"",
            "headers": [(key.encode(), value.encode()) for key, value in (headers or {}).items()],
        })

        async def next_handler(request):
            return JSONResponse({"passed": True})

        return await protect_workspace(request, next_handler)

    async def test_mutations_require_same_origin_custom_header(self):
        response = await self.request("/api/auth/login")
        self.assertEqual(response.status_code, 403)
        response = await self.request("/api/auth/login", {
            "x-cwatch-request": "1", "origin": "http://foreign.example",
        })
        self.assertEqual(response.status_code, 403)

    async def test_malformed_and_oversized_lengths_have_explicit_errors(self):
        for length, status in (("bad", 400), ("-1", 400), ("2000001", 413), ("20", 200)):
            with self.subTest(length=length):
                response = await self.request("/api/auth/login", {
                    "x-cwatch-request": "1", "content-length": length,
                })
                self.assertEqual(response.status_code, status)

    async def test_private_api_requires_authentication_and_research_is_disabled(self):
        response = await self.request("/api/company/portfolio", method="GET")
        self.assertEqual(response.status_code, 401)
        with patch.dict("os.environ", {"CWATCH_ENABLE_RESEARCH": "0"}):
            response = await self.request("/research.html", method="GET")
            self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
