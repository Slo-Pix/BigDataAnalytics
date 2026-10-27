"""Isolated live collection and bounded PostgreSQL exploration for ExposureIQ."""

import logging
import os
import re
import secrets
import signal
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from typing import Literal

import psycopg
from fastapi import APIRouter, HTTPException
from psycopg import sql
from psycopg.conninfo import make_conninfo
from pydantic import BaseModel, Field

from common.paths import RAW, ROOT, STREAM

log = logging.getLogger(__name__)
TOPIC = "tracker.scrapes.cwatch"
READER = "cwatch_query_reader"
VIEWS = ("live_latest", "live_minutes", "live_batches")


class StartRequest(BaseModel):
    targets: Literal[100, 500, 2000] = 500


class LiveQuery(BaseModel):
    sql: str = Field(min_length=1, max_length=10_000)


def query_text(text: str) -> str:
    text = text.strip().rstrip(";").strip()
    if not re.match(r"^(SELECT|WITH)\b", text, re.IGNORECASE):
        raise HTTPException(400, "Use a single SELECT or WITH query over the live views.")
    return text


def stamp(value):
    return value.isoformat() if isinstance(value, datetime) else value


class LiveMonitor:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.reader_dsn = make_conninfo(dsn, user=READER, password=secrets.token_urlsafe(32))
        self.lock = threading.Lock()
        self.db_lock = threading.Lock()
        self.cancel = threading.Event()
        self.worker: threading.Thread | None = None
        self.processes: dict[str, subprocess.Popen] = {}
        self.phase = "stopped"
        self.message = "Start monitoring to collect fresh observations of known torrent hashes."
        self.error: str | None = None
        self.started_at: str | None = None
        self.targets = 0
        self.reader_ready = False

    def update(self, phase: str, message: str, error: str | None = None):
        with self.lock:
            self.phase, self.message, self.error = phase, message, error

    def connect(self):
        return psycopg.connect(self.dsn, connect_timeout=3)

    def command(self, args: list[str], timeout: int = 60):
        if self.cancel.is_set():
            raise InterruptedError("Monitoring startup was cancelled.")
        result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=timeout, check=False)
        if result.returncode:
            log.error("Live prerequisite failed: %s", result.stderr[-2000:])
            raise RuntimeError(f"{args[0]} prerequisite failed (exit {result.returncode}). See the server log.")

    def initialise(self):
        with self.connect() as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS cwatch")
            for table in ("swarm_latest", "swarm_minute", "stream_batches"):
                con.execute(sql.SQL("CREATE TABLE IF NOT EXISTS cwatch.{} (LIKE public.{} INCLUDING ALL)")
                            .format(sql.Identifier(table), sql.Identifier(table)))
            con.execute("""CREATE OR REPLACE VIEW cwatch.live_latest AS
                SELECT s.*, coalesce(t.title, s.infohash) AS title, t.category
                FROM cwatch.swarm_latest s LEFT JOIN public.torrents t USING (infohash)""")
            con.execute("CREATE OR REPLACE VIEW cwatch.live_minutes AS SELECT * FROM cwatch.swarm_minute")
            con.execute("CREATE OR REPLACE VIEW cwatch.live_batches AS SELECT * FROM cwatch.stream_batches")
        self.ensure_reader()

    def ensure_reader(self):
        with self.db_lock:
            if self.reader_ready:
                return
            with self.connect() as con:
                if con.execute("SELECT to_regclass('cwatch.live_latest')").fetchone()[0] is None:
                    raise HTTPException(409, "Start live monitoring once to initialise its query views.")
                if not con.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (READER,)).fetchone():
                    con.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(READER)))
                password = psycopg.conninfo.conninfo_to_dict(self.reader_dsn)["password"]
                con.execute(sql.SQL("""ALTER ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                    NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 4 PASSWORD {}""")
                            .format(sql.Identifier(READER), sql.Literal(password)))
                con.execute(sql.SQL("ALTER ROLE {} SET default_transaction_read_only TO on").format(sql.Identifier(READER)))
                con.execute(sql.SQL("GRANT USAGE ON SCHEMA cwatch TO {}").format(sql.Identifier(READER)))
                for view in VIEWS:
                    con.execute(sql.SQL("GRANT SELECT ON cwatch.{} TO {}")
                                .format(sql.Identifier(view), sql.Identifier(READER)))
            self.reader_ready = True

    def spawn(self, name: str, args: list[str], env: dict[str, str] | None = None):
        path = ROOT / "logs" / f"cwatch-{name}.log"
        path.parent.mkdir(exist_ok=True)
        with path.open("a") as output:
            child = subprocess.Popen(
                args, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT,
                start_new_session=True, env=env,
            )
        self.processes[name] = child

    def start(self, targets: int):
        with self.lock:
            if self.worker and self.worker.is_alive():
                raise HTTPException(409, "Monitoring is already active or changing state.")
            self.cancel.clear()
            self.phase = "starting"
            self.message = "Checking Kafka and PostgreSQL."
            self.error = None
            self.targets = targets
            self.started_at = datetime.now(UTC).isoformat()
            self.worker = threading.Thread(target=self.run, name="cwatch-live", daemon=True)
            self.worker.start()
        return {"accepted": True, "phase": "starting"}

    def run(self):
        try:
            self.command(["docker", "compose", "-f", "infra/docker-compose.yml", "up", "-d", "kafka", "postgres"], 120)
            deadline = time.monotonic() + 90
            while True:
                if self.cancel.is_set():
                    raise InterruptedError("Monitoring startup was cancelled.")
                ready = subprocess.run(
                    ["docker", "exec", "kafka", "/opt/kafka/bin/kafka-topics.sh",
                     "--bootstrap-server", "localhost:9092", "--list"],
                    capture_output=True, timeout=15, check=False,
                )
                if ready.returncode == 0:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError("Kafka did not become ready within 90 seconds.")
                self.cancel.wait(2)
            self.update("starting", "Preparing isolated tables and the monitored hash sample.")
            self.initialise()
            with self.connect() as con:
                rows = con.execute("""SELECT infohash FROM public.swarm_latest GROUP BY 1
                    ORDER BY max(seeders) DESC, infohash LIMIT %s""", (self.targets,)).fetchall()
            hashes = [row[0].strip() for row in rows]
            if not hashes or any(not re.fullmatch(r"[a-fA-F0-9]{40}", value) for value in hashes):
                raise RuntimeError("No valid historical tracker targets are available. Load the base catalogue first.")
            with self.lock:
                self.targets = len(hashes)
            targets = RAW / "scrape_targets" / "cwatch.txt"
            targets.parent.mkdir(parents=True, exist_ok=True)
            targets.write_text("\n".join(hashes) + "\n")
            self.command(["docker", "exec", "kafka", "/opt/kafka/bin/kafka-topics.sh",
                          "--bootstrap-server", "localhost:9092", "--create", "--if-not-exists",
                          "--topic", TOPIC, "--partitions", "3", "--replication-factor", "1"])
            stream_dsn = make_conninfo(self.dsn, options="-csearch_path=cwatch,public")
            stream_env = os.environ | {"BDA_PG_DSN": stream_dsn}
            self.spawn("stream", [sys.executable, "-u", "-m", "streaming.swarm_stream",
                                 "--topic", TOPIC, "--out", str(STREAM / "cwatch"),
                                 "--checkpoint", str(STREAM / "_checkpoints_cwatch"),
                                 "--trigger", "10 seconds"], env=stream_env)
            self.spawn("scraper", [sys.executable, "-u", "-m", "ingest.tracker_scraper",
                                  "--hashes", str(targets), "--kafka", "localhost:9092",
                                  "--topic", TOPIC, "--interval", "30",
                                  "--jsonl-dir", str(STREAM / "jsonl_cwatch")])
            self.update("collecting", "Collector and stream processor started. First committed observations can take 1–2 minutes.")
            while not self.cancel.wait(2):
                for name, child in self.processes.items():
                    if child.poll() is not None:
                        raise RuntimeError(f"{name.capitalize()} exited (code {child.returncode}). Check logs/cwatch-{name}.log.")
        except InterruptedError:
            self.update("stopping", "Stopping collection; retained data will remain queryable.")
        except (OSError, RuntimeError, subprocess.SubprocessError, psycopg.Error, HTTPException) as error:
            log.exception("ExposureIQ live monitoring failed")
            message = error.detail if isinstance(error, HTTPException) else str(error)
            self.update("error", "Live monitoring failed.", message)
        finally:
            self.terminate_children()
            with self.lock:
                if self.phase != "error":
                    self.phase = "stopped"
                    self.message = "Collection stopped. Retained observations can still be queried."

    def terminate_children(self):
        # Each child owns its process group, including Spark's JVM.
        for child in self.processes.values():
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                log.info("Live child %s already exited", child.pid)
        for child in self.processes.values():
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                log.warning("Force-stopping live process group %s", child.pid)
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    log.info("Live child %s already exited", child.pid)
                child.wait(timeout=5)
        self.processes.clear()

    def stop(self):
        with self.lock:
            if self.worker and self.worker.is_alive():
                self.phase = "stopping"
                self.message = "Stopping collector and stream processor. Retained data is not deleted."
                self.cancel.set()
            else:
                self.phase = "stopped"
                self.message = "Collection is stopped. Retained observations are still available."
        return {"accepted": True, "phase": self.phase}

    def shutdown(self):
        self.stop()
        if self.worker:
            self.worker.join(timeout=180)

    def status(self):
        with self.lock:
            result = {
                "phase": self.phase, "message": self.message, "error": self.error,
                "started_at": self.started_at, "targets": self.targets, "data": None,
                "data_error": None,
            }
        try:
            with self.connect() as con:
                con.execute("SET LOCAL statement_timeout = '5s'")
                if con.execute("SELECT to_regclass('cwatch.live_latest')").fetchone()[0] is None:
                    return result
                totals = con.execute("""SELECT count(DISTINCT infohash), count(*), max(scraped_at)
                    FROM cwatch.swarm_latest""").fetchone()
                ledger = con.execute("SELECT count(*), coalesce(sum(rows), 0) FROM cwatch.stream_batches").fetchone()
                top = con.execute("""SELECT title, category, infohash, max(seeders) AS seeders,
                    max(leechers) AS leechers, max(scraped_at) AS scraped_at FROM cwatch.live_latest
                    GROUP BY title, category, infohash ORDER BY seeders DESC LIMIT 10""").fetchall()
                minutes = con.execute("""SELECT minute, sum(rows) FROM cwatch.swarm_minute
                    WHERE minute > now() - interval '20 minutes' GROUP BY minute ORDER BY minute""").fetchall()
                latest = totals[2]
                started = datetime.fromisoformat(result["started_at"]) if result["started_at"] else None
                fresh = (
                    latest is not None
                    and 0 <= (datetime.now(UTC) - latest).total_seconds() < 120
                    and (started is None or latest >= started)
                )
                result["data"] = {
                    "torrents": totals[0], "tracker_records": totals[1], "last_observed": stamp(latest),
                    "fresh": fresh,
                    "batches": ledger[0], "observations": ledger[1],
                    "top": [dict(zip(("title", "category", "infohash", "seeders", "leechers", "scraped_at"),
                                     (stamp(value) for value in row))) for row in top],
                    "minutes": [{"minute": stamp(row[0]), "rows": row[1]} for row in minutes],
                }
        except psycopg.Error as error:
            log.warning("Live status database unavailable: %s", error)
            result["data_error"] = "Live database unavailable. Check PostgreSQL and the server log."
        return result

    def query(self, text: str):
        text = query_text(text)
        started = time.monotonic()
        try:
            self.ensure_reader()
            with psycopg.connect(self.reader_dsn, connect_timeout=3) as con:
                con.execute("SET TRANSACTION READ ONLY")
                con.execute("SET LOCAL statement_timeout = '5s'")
                con.execute("SET LOCAL lock_timeout = '1s'")
                con.execute("SET LOCAL search_path = cwatch, pg_catalog")
                cursor = con.execute(
                    sql.SQL("SELECT * FROM ({}) AS cwatch_result LIMIT 201").format(sql.SQL(text)),
                    prepare=True,
                )
                columns = [column.name for column in cursor.description]
                rows = [[stamp(value) for value in row] for row in cursor.fetchall()]
        except psycopg.Error as error:
            log.warning("Live query rejected: %s", error.diag.message_primary or type(error).__name__)
            raise HTTPException(400, error.diag.message_primary or "Live query failed. Check the database connection.") from None
        return {
            "columns": columns, "rows": rows[:200], "truncated": len(rows) > 200,
            "seconds": round(time.monotonic() - started, 3), "source": "cwatch live serving views",
            "queried_at": datetime.now(UTC).isoformat(),
        }


def live_router(monitor: LiveMonitor) -> APIRouter:
    router = APIRouter(prefix="/api/contentwatch/live")

    @router.get("")
    def status():
        return monitor.status()

    @router.post("/start", status_code=202)
    def start(request: StartRequest):
        return monitor.start(request.targets)

    @router.post("/stop", status_code=202)
    def stop():
        return monitor.stop()

    @router.post("/query")
    def query(request: LiveQuery):
        return monitor.query(request.sql)

    return router
