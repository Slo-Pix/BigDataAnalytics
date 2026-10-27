"""Focused lifecycle and read-only query tests; no Docker or tracker access."""

import os
import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import psycopg
from fastapi import HTTPException

from portal.live import LiveMonitor, query_text


class LiveTests(unittest.TestCase):
    def setUp(self):
        self.monitor = LiveMonitor("host=localhost dbname=test user=owner")

    def test_query_requires_select_or_with(self):
        self.assertEqual(query_text(" SELECT 1; "), "SELECT 1")
        self.assertTrue(query_text("WITH a AS (SELECT 1) SELECT * FROM a").startswith("WITH"))
        for text in ("", "DELETE FROM live_latest", "DROP TABLE live_latest", "COPY live_latest TO STDOUT"):
            with self.subTest(text=text), self.assertRaises(HTTPException) as raised:
                query_text(text)
            self.assertEqual(raised.exception.status_code, 400)

    def test_duplicate_start_is_rejected_and_stop_is_cancellable(self):
        with patch("portal.live.threading.Thread") as thread:
            thread.return_value.is_alive.return_value = True
            result = self.monitor.start(100)
            self.assertEqual(result["phase"], "starting")
            with self.assertRaises(HTTPException) as raised:
                self.monitor.start(500)
            self.assertEqual(raised.exception.status_code, 409)
            self.monitor.stop()
            self.assertTrue(self.monitor.cancel.is_set())
            self.assertEqual(self.monitor.phase, "stopping")

    def test_cancelled_start_never_spawns_processes(self):
        self.monitor.cancel.set()
        with patch.object(self.monitor, "spawn") as spawn:
            self.monitor.run()
        spawn.assert_not_called()
        self.assertEqual(self.monitor.phase, "stopped")

    def test_startup_error_is_visible(self):
        with (
            patch.object(self.monitor, "command", side_effect=RuntimeError("Docker unavailable")),
            self.assertLogs("portal.live", level="ERROR"),
        ):
            self.monitor.run()
        self.assertEqual(self.monitor.phase, "error")
        self.assertEqual(self.monitor.error, "Docker unavailable")

    def test_database_failure_is_not_reported_as_empty_success(self):
        with (
            patch.object(self.monitor, "connect", side_effect=psycopg.OperationalError("offline")),
            self.assertLogs("portal.live", level="WARNING"),
        ):
            result = self.monitor.status()
        self.assertIsNone(result["data"])
        self.assertIn("unavailable", result["data_error"])

    def test_freshness_requires_an_observation_from_the_current_run(self):
        now = datetime.now(UTC)
        self.monitor.started_at = (now - timedelta(seconds=10)).isoformat()
        for age, expected in ((20, False), (5, True), (121, False)):
            connection = MagicMock()
            connection.execute.side_effect = [
                MagicMock(),
                MagicMock(fetchone=lambda: ("cwatch.live_latest",)),
                MagicMock(fetchone=lambda age=age: (1, 1, now - timedelta(seconds=age))),
                MagicMock(fetchone=lambda: (1, 100)),
                MagicMock(fetchall=list),
                MagicMock(fetchall=list),
            ]
            with self.subTest(age=age), patch.object(self.monitor, "connect") as connect:
                connect.return_value.__enter__.return_value = connection
                result = self.monitor.status()
            self.assertEqual(result["data"]["fresh"], expected)

    def test_queries_use_reader_and_read_only_bounded_transaction(self):
        self.monitor.reader_ready = True
        connection = MagicMock()
        cursor = MagicMock()
        cursor.description = [MagicMock(name="value")]
        cursor.description[0].name = "value"
        cursor.fetchall.return_value = [[index] for index in range(201)]
        connection.execute.return_value = cursor
        with patch("portal.live.psycopg.connect") as connect:
            connect.return_value.__enter__.return_value = connection
            result = self.monitor.query("SELECT 1")
        self.assertIn("user=cwatch_query_reader", connect.call_args.args[0])
        self.assertEqual(result["columns"], ["value"])
        self.assertEqual(len(result["rows"]), 200)
        self.assertTrue(result["truncated"])
        commands = [str(call.args[0]) for call in connection.execute.call_args_list]
        self.assertIn("SET TRANSACTION READ ONLY", commands)
        self.assertIn("SET LOCAL statement_timeout = '5s'", commands)
        self.assertIn("LIMIT 201", commands[-1])

    def test_database_query_errors_are_returned_explicitly(self):
        self.monitor.reader_ready = True
        with (
            patch("portal.live.psycopg.connect", side_effect=psycopg.OperationalError("offline")),
            self.assertLogs("portal.live", level="WARNING"),
            self.assertRaises(HTTPException) as raised,
        ):
            self.monitor.query("SELECT 1")
        self.assertEqual(raised.exception.status_code, 400)

    def test_cleanup_signals_only_owned_process_groups(self):
        child = MagicMock()
        child.pid = 123456
        self.monitor.processes["stream"] = child
        with patch("portal.live.os.killpg") as kill:
            self.monitor.terminate_children()
        self.assertEqual(kill.call_args.args[0], child.pid)
        child.wait.assert_called_once_with(timeout=15)
        self.assertEqual(self.monitor.processes, {})

    def test_stream_database_environment_override_preserves_cli_default(self):
        from streaming.swarm_stream import main

        for environment, expected in (({}, "cli-dsn"), ({"BDA_PG_DSN": "env-dsn"}, "env-dsn")):
            with (
                self.subTest(environment=environment),
                patch.dict(os.environ, environment, clear=True),
                patch("sys.argv", ["swarm_stream", "--pg", "cli-dsn", "--once"]),
                patch("streaming.swarm_stream.get_spark"),
                patch("streaming.swarm_stream.parse"),
                patch("streaming.swarm_stream.make_serving_writer") as writer,
                patch("builtins.print"),
            ):
                main()
            self.assertEqual(writer.call_args.args[0], expected)


if __name__ == "__main__":
    unittest.main()
