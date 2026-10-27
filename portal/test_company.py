"""Company workflow tests: real isolated SQLite, no tracker or outbound traffic."""

import csv
import io
import os
import socket
import unittest
import uuid
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from portal import company
from portal.accounts import AccountStore, Actor


class SQLiteStore(AccountStore):
    """Actual account store with isolated test tenants; no password hashing needed."""

    def __init__(self):
        self.path = Path(f".company-test-{uuid.uuid4().hex}.sqlite3")
        super().__init__(self.path)
        with self.connect() as con:
            con.executemany("INSERT INTO organisations VALUES(?,?,?)",
                            [(10, "First", company._now()), (20, "Second", company._now())])
            for ident, org, role in (
                (1, 10, "admin"), (2, 10, "analyst"), (3, 10, "viewer"), (4, 20, "admin")
            ):
                con.execute(
                    "INSERT INTO users VALUES(?,?,?,?,?,?)",
                    (ident, org, f"User {ident}", f"user{ident}@example.org", "unused", role),
                )

    def close(self):
        self.path.unlink(missing_ok=True)
        Path(str(self.path) + "-wal").unlink(missing_ok=True)
        Path(str(self.path) + "-shm").unlink(missing_ok=True)


class CompanyTests(unittest.TestCase):
    def setUp(self):
        self.store = SQLiteStore()
        self.addCleanup(self.store.close)
        self.admin = Actor(10, 1, "admin", "User 1", "user1@example.org")
        self.analyst = Actor(10, 2, "analyst", "User 2", "user2@example.org")
        self.viewer = Actor(10, 3, "viewer", "User 3", "user3@example.org")
        self.other = Actor(20, 4, "admin", "User 4", "user4@example.org")
        self.router = company.company_router(self.store)
        self.routes = {(next(iter(r.methods)), r.path): r.endpoint for r in self.router.routes}
        self.title = {
            "key": "movie:one", "title": "Example", "category": "movie", "imdb_id": "tt123",
            "steam_app_id": None, "matched_hashes": 1, "hashes": ["a" * 40], "limited": False,
        }
        resolve = patch("portal.company.resolve_title", side_effect=self.resolve)
        search = patch("portal.company.search_titles", return_value=[self.title])
        resolve.start()
        self.search = search.start()
        self.addCleanup(resolve.stop)
        self.addCleanup(search.stop)

    def resolve(self, key):
        if key != self.title["key"]:
            raise HTTPException(404, "Title not found")
        return dict(self.title)

    def call(self, method, path, *args, actor=None, **kwargs):
        return self.routes[(method, "/api/company" + path)](
            *args, actor=actor or self.admin, **kwargs
        )

    def add_title(self, actor=None):
        return self.call("POST", "/portfolio", company.Selection(key="movie:one"), actor=actor)

    def check(self, seeders=5, fresh=True, observations=None):
        return {
            "max_seeders": seeders, "fresh": fresh,
            "checked_at": datetime.now(UTC).isoformat(),
            "observations": observations or [], "errors": [],
        }

    def new_rule(self, threshold=5, portfolio_id=None, actor=None):
        return self.call("POST", "/rules", company.RuleCreate(
            name="Seeders", threshold=threshold, portfolio_id=portfolio_id
        ), actor=actor)

    def assert_http(self, status, function, *args, **kwargs):
        with self.assertRaises(HTTPException) as raised:
            function(*args, **kwargs)
        self.assertEqual(raised.exception.status_code, status)
        return raised.exception

    def test_tenant_lists_and_scoped_ids(self):
        owned = self.add_title()
        other = self.add_title(self.other)
        self.assertNotEqual(owned["id"], other["id"])
        result = self.call("GET", "/portfolio", limit=200, offset=0, actor=self.viewer)
        self.assertEqual([r["id"] for r in result["rows"]], [owned["id"]])
        self.assert_http(404, self.call, "POST", "/portfolio/{ident}/approve",
                         other["id"], company.Selection(key="movie:one"))
        self.assert_http(404, self.call, "GET", "/checks", portfolio_id=other["id"])
        self.assert_http(404, self.call, "DELETE", "/portfolio/{ident}", other["id"])
        self.assert_http(404, company.record_check, self.store, self.admin, other["id"], self.check())

    def test_viewer_cannot_mutate_any_workflow(self):
        title = self.add_title()
        requests = [
            ("POST", "/portfolio", (company.Selection(key="movie:one"),)),
            ("POST", "/portfolio/import", (company.ImportPortfolio(csv_text="title,category\nX,movie"),)),
            ("POST", "/portfolio/{ident}/approve", (title["id"], company.Selection(key="movie:one"))),
            ("DELETE", "/portfolio/{ident}", (title["id"],)),
            ("POST", "/cases", (company.CaseCreate(portfolio_id=title["id"], title="Case"),)),
            ("PATCH", "/cases/{ident}", (1, company.CaseUpdate(status="open"))),
            ("POST", "/cases/{ident}/notes", (1, company.NoteCreate(body="Note"))),
            ("POST", "/rules", (company.RuleCreate(name="Rule", threshold=0),)),
            ("PATCH", "/rules/{ident}", (1, company.RuleUpdate(enabled=False))),
            ("PATCH", "/alerts/{ident}", (1, company.AlertUpdate(status="reviewed"))),
            ("POST", "/alerts/{ident}/retry-delivery", (1,)),
            ("POST", "/reports", (company.ReportCreate(name="Daily", frequency="daily"),)),
        ]
        for method, path, args in requests:
            with self.subTest(path=path):
                self.assert_http(403, self.call, method, path, *args, actor=self.viewer)
        self.assert_http(403, self.call, "GET", "/audit", actor=self.viewer)
        self.assert_http(403, self.call, "GET", "/audit", actor=self.analyst)
        self.assert_http(403, company.record_check, self.store, self.viewer, title["id"], self.check())
        self.assertEqual(self.call("GET", "/cases", actor=self.viewer), {"rows": []})

    def test_import_exact_still_pending_and_approval_authoritative(self):
        response = self.call("POST", "/portfolio/import",
                             company.ImportPortfolio(csv_text="title,category,external_id\nExample,movie,foo"))
        row = response["rows"][0]
        self.assertEqual((row["status"], row["confidence"]), ("pending", "exact"))
        self.assertIsNone(row["title_key"])
        self.assertEqual(row["candidates"], [self.title])
        self.assert_http(409, company.record_check, self.store, self.admin, row["id"], self.check())
        approved = self.call("POST", "/portfolio/{ident}/approve",
                             row["id"], company.Selection(key="movie:one"), actor=self.analyst)
        self.assertEqual(approved["title"], self.title["title"])
        self.assertEqual(approved["metadata"]["hashes"], self.title["hashes"])
        self.assertEqual(approved["status"], "approved")

    def test_import_all_rows_validate_before_any_write(self):
        bad = "title,category\nGood,movie\nBad,unknown\n,movie\n"
        error = self.assert_http(400, self.call, "POST", "/portfolio/import",
                                 company.ImportPortfolio(csv_text=bad))
        self.assertEqual([e["row"] for e in error.detail["rows"]], [3, 4])
        self.assertEqual(self.call("GET", "/portfolio", limit=200, offset=0)["rows"], [])
        self.search.assert_not_called()
        for text in (
            "", "title,title\nX,X", "title\nX", "title,category\nX,movie,extra",
            "title,category\nX", 'title,category\n"unterminated,movie',
            "title,category\n" + "X,movie\n" * 201,
        ):
            with self.subTest(csv=text[:60]):
                self.assert_http(400, self.call, "POST", "/portfolio/import",
                                 company.ImportPortfolio(csv_text=text))

    def test_import_limits_candidates_and_no_partial_search_failure(self):
        self.search.return_value = [self.title] * 5
        result = self.call("POST", "/portfolio/import",
                           company.ImportPortfolio(csv_text="\ufefftitle,category\nXX,movie"))
        self.assertEqual(len(result["rows"][0]["candidates"]), 3)
        self.search.side_effect = [[self.title], RuntimeError("Search unavailable")]
        with self.assertRaises(RuntimeError):
            self.call("POST", "/portfolio/import",
                      company.ImportPortfolio(csv_text="title,category\nAA,movie\nBB,tv"))
        self.assertEqual(len(self.call("GET", "/portfolio", limit=200, offset=0)["rows"]), 1)

    def test_import_byte_limit_and_whitespace(self):
        self.assert_http(400, company._parse_import, "title,category\n" + "é" * 524288)
        self.assertEqual(company._parse_import("title,category\n  Example  , MOVIE  "),
                         [("Example", "movie", "")])
        self.search.reset_mock()
        row = self.call("POST", "/portfolio/import",
                        company.ImportPortfolio(csv_text="title,category\nX,movie"))["rows"][0]
        self.assertEqual(row["confidence"], "unmatched")
        self.search.assert_not_called()
        self.call("POST", "/portfolio/import",
                  company.ImportPortfolio(csv_text="title,category,external_id\nX,movie,tt123"))
        self.search.assert_called_once_with("tt123", "movie")

    def test_manual_duplicate_and_bad_key(self):
        self.add_title()
        self.assert_http(409, self.add_title)
        self.assert_http(404, self.call, "POST", "/portfolio", company.Selection(key="invalid"))

    def test_portfolio_item_prevents_unapproved_or_cross_tenant_checks(self):
        title = self.add_title()
        item = company.portfolio_item(self.store, self.analyst, title["id"])
        self.assertEqual(item["key"], "movie:one")
        self.assertEqual(item["id"], title["id"])
        self.assert_http(404, company.portfolio_item, self.store, self.other, title["id"])
        self.assert_http(403, company.portfolio_item, self.store, self.viewer, title["id"])
        pending = self.call("POST", "/portfolio/import",
                            company.ImportPortfolio(csv_text="title,category\nX,movie"))["rows"][0]
        self.assert_http(409, company.portfolio_item, self.store, self.admin, pending["id"])

    def test_cases_assignees_notes_history_and_tenant_reads(self):
        portfolio = self.add_title()
        other = self.add_title(self.other)
        self.assert_http(404, self.call, "POST", "/cases",
                         company.CaseCreate(portfolio_id=other["id"], title="Wrong"))
        self.assert_http(400, self.call, "POST", "/cases", company.CaseCreate(
            portfolio_id=portfolio["id"], title="Case", assignee_id=4
        ))
        case = self.call("POST", "/cases", company.CaseCreate(
            portfolio_id=portfolio["id"], title="Case", description="Evidence", assignee_id=2
        ))
        self.assertEqual(self.call("GET", "/cases")["rows"][0]["assignee_name"], "User 2")
        self.assertEqual(self.call("GET", "/cases/{ident}", case["id"])["assignee_name"], "User 2")
        note = self.call("POST", "/cases/{ident}/notes", case["id"], company.NoteCreate(body="Observed"))
        self.assertEqual(note["user_id"], 1)
        for method, path, args in (
            ("GET", "/cases/{ident}", (case["id"],)),
            ("PATCH", "/cases/{ident}", (case["id"], company.CaseUpdate(status="closed"))),
            ("POST", "/cases/{ident}/notes", (case["id"], company.NoteCreate(body="Wrong"))),
        ):
            self.assert_http(404, self.call, method, path, *args, actor=self.other)
        self.assert_http(400, self.call, "PATCH", "/cases/{ident}", case["id"],
                         company.CaseUpdate(status="open", assignee_id=4))
        updated = self.call("PATCH", "/cases/{ident}", case["id"], company.CaseUpdate(status="investigating"))
        self.assertEqual(updated["assignee_id"], 2)
        updated = self.call("PATCH", "/cases/{ident}", case["id"],
                            company.CaseUpdate(status="actioned", assignee_id=None))
        self.assertIsNone(updated["assignee_id"])
        check = company.record_check(self.store, self.admin, portfolio["id"], self.check())
        detail = self.call("GET", "/cases/{ident}", case["id"], actor=self.viewer)
        self.assertEqual(len(detail["notes"]), 1)
        self.assertEqual(detail["check_summary"]["id"], check["check"]["id"])
        self.assert_http(409, self.call, "DELETE", "/portfolio/{ident}", portfolio["id"])
        self.assert_http(409, self.call, "POST", "/portfolio/{ident}/approve",
                         portfolio["id"], company.Selection(key="movie:one"))

    def test_delete_unreferenced_is_safe_and_audited(self):
        portfolio = self.add_title()
        self.assertEqual(self.call("DELETE", "/portfolio/{ident}", portfolio["id"]),
                         {"deleted": portfolio["id"]})
        audit = self.call("GET", "/audit")["rows"]
        self.assertEqual(audit[0]["action"], "portfolio.delete")
        self.assertEqual(self.call("GET", "/audit", actor=self.other)["rows"], [])

    def test_alert_inclusive_threshold_freshness_and_dedup(self):
        title = self.add_title()
        self.new_rule()
        self.new_rule(actor=self.other, threshold=0)
        for maximum, fresh in ((100, False), (None, True), (4, True)):
            result = company.record_check(self.store, self.admin, title["id"],
                                          self.check(maximum, fresh))
            self.assertEqual(result["alerts"], [])
        first = company.record_check(self.store, self.admin, title["id"], self.check())
        self.assertEqual(len(first["alerts"]), 1)
        original_check = first["check"]["id"]
        second = company.record_check(self.store, self.admin, title["id"], self.check(50))
        self.assertEqual(second["alerts"], [])
        alert = self.call("GET", "/alerts")["rows"][0]
        self.assertEqual((alert["title"], alert["rule_name"]), ("Example", "Seeders"))
        self.assertEqual(alert["delivery_status"], "pending")
        self.assertIn("maximum tracker-reported seeders 5", alert["message"])
        self.assertEqual(alert["check_id"], original_check)
        self.assertEqual([d["status"] for d in alert["deliveries"]], ["pending", "pending"])
        self.assertEqual(self.call("GET", "/alerts", actor=self.other)["rows"], [])
        self.assert_http(404, self.call, "PATCH", "/alerts/{ident}", alert["id"],
                         company.AlertUpdate(status="reviewed"), actor=self.other)
        self.call("PATCH", "/alerts/{ident}", alert["id"], company.AlertUpdate(status="reviewed"))
        self.assertEqual(company.record_check(self.store, self.admin, title["id"], self.check())["alerts"], [])
        self.call("PATCH", "/alerts/{ident}", alert["id"], company.AlertUpdate(status="resolved"))
        latest = company.record_check(self.store, self.admin, title["id"], self.check())
        self.assertEqual(len(latest["alerts"]), 1)
        self.assertNotEqual(latest["alerts"][0]["id"], alert["id"])
        self.assert_http(409, self.call, "PATCH", "/alerts/{ident}", alert["id"],
                         company.AlertUpdate(status="new"))
        history = self.call("GET", "/checks", portfolio_id=title["id"])["rows"]
        self.assertEqual(len(history), 7)
        self.assertEqual(history[-1]["result"]["max_seeders"], 100)

    def test_maximum_is_not_sum_and_title_rules_are_scoped(self):
        title = self.add_title()
        self.new_rule(threshold=8, portfolio_id=title["id"])
        rule = self.new_rule(threshold=7)
        observations = [
            {"infohash": "a" * 40, "tracker": tracker, "seeders": seeders, "leechers": 1,
             "checked_at": company._now()}
            for tracker, seeders in (("udp://one", 4), ("udp://two", 7))
        ]
        check = company.record_check(self.store, self.admin, title["id"],
                                     self.check(11, observations=observations))
        self.assertEqual(check["check"]["result"]["max_seeders"], 7)
        self.assertEqual([a["rule_id"] for a in check["alerts"]], [rule["id"]])
        self.assert_http(404, self.call, "PATCH", "/rules/{ident}", rule["id"],
                         company.RuleUpdate(enabled=False), actor=self.other)
        self.call("PATCH", "/rules/{ident}", rule["id"], company.RuleUpdate(enabled=False))
        self.assertFalse(next(r for r in self.call("GET", "/rules")["rows"] if r["id"] == rule["id"])["enabled"])

    def test_check_result_validation_and_check_history_cap(self):
        title = self.add_title()
        for value in (True, -1, "10"):
            self.assert_http(400, company.record_check, self.store, self.admin, title["id"], self.check(value))
        invalid = self.check()
        invalid["checked_at"] = "yesterday"
        self.assert_http(400, company.record_check, self.store, self.admin, title["id"], invalid)
        invalid = {**self.check(), "key": "another:title"}
        self.assert_http(400, company.record_check, self.store, self.admin, title["id"], invalid)
        for _ in range(102):
            company.record_check(self.store, self.admin, title["id"], self.check())
        self.assertEqual(len(self.call("GET", "/checks", portfolio_id=None)["rows"]), 100)

    def test_reports_are_real_scoped_and_formula_safe(self):
        self.call("POST", "/portfolio/import", company.ImportPortfolio(
            csv_text='title,category,external_id\n"=HYPERLINK(""bad"")",movie,+123'
        ))
        self.add_title(self.other)
        response = self.call("GET", "/reports/portfolio.csv", actor=self.viewer)
        rows = list(csv.DictReader(io.StringIO(response.body.decode())))
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["title"].startswith("'="))
        self.assertEqual(rows[0]["external_id"], "'+123")
        self.assertEqual(rows[0]["source"], "csv")
        self.assertIn("not summed", rows[0]["methodology"])
        for text in ("=X", "+X", "-X", "@X", "  =X", "\tX", "\rX", "\nX"):
            self.assertTrue(company._csv_safe(text).startswith("'"))
        summary = self.call("GET", "/reports/summary", actor=self.viewer)
        self.assertEqual(summary["portfolio"], {"pending": 1})
        self.assertEqual(summary["checks"], 0)
        self.assertTrue(summary["limitations"])
        self.assertEqual(len(self.call("GET", "/audit")["rows"]), 3)

    def test_schedules_persist_snapshots_and_deduplicate_utc_periods(self):
        self.add_title()
        daily = self.call("POST", "/reports", company.ReportCreate(name="Daily", frequency="daily"))
        weekly = self.call("POST", "/reports", company.ReportCreate(name="Weekly", frequency="weekly"))
        self.assertEqual(company.run_schedules(self.store), {"generated": 0})
        self.assertEqual(company.run_schedules(self.store), {"generated": 0})
        self.assert_http(404, self.call, "GET", "/reports/{ident}", daily["id"], actor=self.other)
        detail = self.call("GET", "/reports/{ident}", daily["id"], actor=self.viewer)
        self.assertEqual(detail["summary"]["portfolio"], {"approved": 1})
        self.assertEqual(len(detail["runs"]), 1)
        self.assertEqual(self.call("GET", "/reports", actor=self.other)["rows"], [])
        with self.store.connect() as con:
            con.execute("UPDATE company_report_runs SET period='old'")
        self.assertEqual(company.run_schedules(self.store), {"generated": 2})
        self.assertEqual(company.run_schedules(self.store), {"generated": 0})
        self.assertEqual(len(self.call("GET", "/reports/{ident}", weekly["id"])["runs"]), 2)
        self.assertEqual(company._period("weekly", datetime(2021, 1, 1, tzinfo=UTC)), "2020-W53")

    def test_delivery_unconfigured_is_persisted_without_sending(self):
        title = self.add_title()
        self.new_rule()
        company.record_check(self.store, self.admin, title["id"], self.check())
        with patch.dict(os.environ, {}, clear=True), patch("portal.company.smtplib.SMTP") as smtp, \
                patch("portal.company.socket.getaddrinfo") as dns, self.assertLogs("portal.company", level="WARNING"):
            self.assertEqual(company.deliver_pending(self.store), {"processed": 2})
            smtp.assert_not_called()
            dns.assert_not_called()
        statuses = {d["status"] for d in self.call("GET", "/alerts")["rows"][0]["deliveries"]}
        self.assertEqual(statuses, {"unconfigured"})
        self.assertEqual(company.deliver_pending(self.store), {"processed": 0})

    def test_delivery_failure_is_not_sent(self):
        title = self.add_title()
        self.new_rule()
        company.record_check(self.store, self.admin, title["id"], self.check())
        with patch("portal.company._email_delivery", side_effect=OSError("secret")), \
                patch("portal.company._webhook_delivery", side_effect=OSError("secret")), \
                self.assertLogs("portal.company", level="ERROR") as logs:
            company.deliver_pending(self.store)
        self.assertNotIn("secret", " ".join(logs.output))
        statuses = {d["status"] for d in self.call("GET", "/alerts")["rows"][0]["deliveries"]}
        self.assertEqual(statuses, {"error"})

    def test_delivery_sent_only_after_successful_mock_transport(self):
        title = self.add_title()
        self.new_rule()
        company.record_check(self.store, self.admin, title["id"], self.check())
        with patch("portal.company._email_delivery") as email, \
                patch("portal.company._webhook_delivery") as webhook:
            company.deliver_pending(self.store)
            email.assert_called_once()
            webhook.assert_called_once()
            self.assertEqual(email.call_args.args[0]["check"]["max_seeders"], 5)
        self.assertEqual({d["status"] for d in self.call("GET", "/alerts")["rows"][0]["deliveries"]},
                         {"sent"})

    def test_explicit_delivery_retry_is_scoped_and_never_resends_success(self):
        title = self.add_title()
        self.new_rule()
        alert = company.record_check(self.store, self.admin, title["id"], self.check())["alerts"][0]
        self.assert_http(409, self.call, "POST", "/alerts/{ident}/retry-delivery", alert["id"])
        self.assert_http(404, self.call, "POST", "/alerts/{ident}/retry-delivery",
                         alert["id"], actor=self.other)
        with patch("portal.company._email_delivery"), \
                patch("portal.company._webhook_delivery", side_effect=company._Unconfigured("Not configured")), \
                self.assertLogs("portal.company", level="WARNING"):
            company.deliver_pending(self.store)
        self.assertEqual(self.call("GET", "/alerts")["rows"][0]["delivery_status"], "partial")
        response = self.call("POST", "/alerts/{ident}/retry-delivery",
                             alert["id"], actor=self.analyst)
        self.assertEqual((response["alert_id"], response["queued"]), (alert["id"], 1))
        statuses = {d["channel"]: d["status"] for d in response["deliveries"]}
        self.assertEqual(statuses, {"email": "sent", "webhook": "pending"})
        with patch("portal.company._email_delivery") as email, \
                patch("portal.company._webhook_delivery", side_effect=OSError("offline")), \
                self.assertLogs("portal.company", level="ERROR"):
            self.assertEqual(company.deliver_pending(self.store), {"processed": 1})
            email.assert_not_called()
        self.assertEqual(self.call("POST", "/alerts/{ident}/retry-delivery", alert["id"])["queued"], 1)
        with patch("portal.company._webhook_delivery"):
            company.deliver_pending(self.store)
        self.assertEqual(self.call("GET", "/alerts")["rows"][0]["delivery_status"], "sent")
        self.assert_http(409, self.call, "POST", "/alerts/{ident}/retry-delivery", alert["id"])
        with self.store.connect() as con:
            con.execute(
                "UPDATE company_deliveries SET status='error' WHERE alert_id=? AND channel='webhook'",
                (alert["id"],),
            )
        self.call("PATCH", "/alerts/{ident}", alert["id"], company.AlertUpdate(status="resolved"))
        self.assert_http(409, self.call, "POST", "/alerts/{ident}/retry-delivery", alert["id"])
        self.assertTrue(any(r["action"] == "delivery.retry" for r in self.call("GET", "/audit")["rows"]))

    def test_smtp_requires_tls_and_valid_operator_config(self):
        config = {
            "CWATCH_SMTP_HOST": "smtp.example.org", "CWATCH_SMTP_PORT": "587",
            "CWATCH_SMTP_FROM": "cwatch@example.org", "CWATCH_ALERT_EMAIL": "ops@example.org",
            "CWATCH_SMTP_USER": "pilot", "CWATCH_SMTP_PASSWORD": "fake-test-password",
        }
        with patch.dict(os.environ, config, clear=True), patch("portal.company._BoundedSMTP") as transport:
            smtp = transport.return_value.__enter__.return_value
            smtp.send_message.return_value = {}
            company._email_delivery({"alert_id": 1})
            smtp.starttls.assert_called_once()
            smtp.login.assert_called_once_with("pilot", "fake-test-password")
            smtp.send_message.assert_called_once()
        config["CWATCH_ALERT_EMAIL"] = "not-an-email"
        with patch.dict(os.environ, config, clear=True), patch("portal.company._BoundedSMTP") as transport, \
                self.assertRaises(company._Unconfigured):
            company._email_delivery({"alert_id": 1})
        transport.assert_not_called()

    def test_webhook_blocks_private_redirect_schemes_and_mixed_dns(self):
        for url in ("http://example.org/hook", "https://user:pass@example.org/hook",
                    "https://example.org:8443/hook", "https://example.org/#fragment"):
            with self.subTest(url=url), self.assertRaises(company._Unconfigured):
                company._public_webhook(url)
        for ip in ("127.0.0.1", "10.0.0.1", "::1", "169.254.169.254", "192.168.1.1",
                   "100.64.0.1", "224.0.0.1", "ff02::1"):
            answer = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 443))]
            with self.subTest(ip=ip), patch("portal.company.socket.getaddrinfo", return_value=answer), \
                    self.assertRaises(company._Unconfigured):
                company._public_webhook("https://example.org/hook")

    def test_webhook_pins_address_verifies_host_and_does_not_follow_redirect(self):
        address = (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))
        with patch.dict(os.environ, {"CWATCH_ALERT_WEBHOOK": "https://example.org?token=fake"}, clear=True), \
                    patch("portal.company._resolve", return_value=[address]), \
                    patch("portal.company.socket.socket") as sockets, \
                    patch("portal.company.ssl.create_default_context") as tls, \
                    patch("portal.company.http.client.HTTPSConnection") as https:
                connection = https.return_value
                connection.getresponse.return_value.status = 302
                with self.assertRaises(RuntimeError):
                    company._webhook_delivery({"alert_id": 1})
                sockets.return_value.connect.assert_called_once_with(("8.8.8.8", 443))
                tls.return_value.wrap_socket.assert_called_once_with(
                    sockets.return_value, server_hostname="example.org"
                )
                self.assertEqual(connection.request.call_args.args[:2], ("POST", "/?token=fake"))
                connection.request.assert_called_once()
                connection.close.assert_called_once()
        mixed = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 443))
                 for ip in ("8.8.8.8", "127.0.0.1")]
        with patch("portal.company.socket.getaddrinfo", return_value=mixed), \
                self.assertRaises(company._Unconfigured):
            company._public_webhook("https://example.org/hook")

    def test_audit_is_atomic_with_workflow(self):
        with patch.object(self.store, "audit", side_effect=RuntimeError("Audit unavailable")), \
                self.assertRaises(RuntimeError):
            self.add_title()
        self.assertEqual(self.call("GET", "/portfolio", limit=200, offset=0)["rows"], [])


if __name__ == "__main__":
    unittest.main()
