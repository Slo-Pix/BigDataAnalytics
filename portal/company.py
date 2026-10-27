"""Tenant-scoped company workflows.

The host calls ``company_router(accounts)`` once, ``record_check`` after an actual
tracker check, ``deliver_pending`` in a background job, and ``run_schedules``
once a minute. Delivery never runs in an HTTP handler. Printable reports use the
summary JSON; the frontend should offer browser Print, not claim to create PDFs.

HTTP contract (all paths under /api/company):
  GET portfolio?limit=100&offset=0 -> {rows: [Portfolio]}, maximum limit 200.
  POST portfolio/import {csv_text} -> {rows: [Portfolio]}, all pending.
  POST portfolio {key}; POST portfolio/{id}/approve {key} -> Portfolio.
  DELETE portfolio/{id} -> {deleted: id}; referenced history returns 409.
  GET cases -> {rows: [Case]}; POST cases
    {portfolio_id,title,description?,assignee_id?} -> Case.
  PATCH cases/{id} {status,assignee_id?} -> Case; omission preserves assignee,
    explicit null clears it. GET cases/{id} -> Case + {notes,check_summary}.
  POST cases/{id}/notes {body} -> {id,org_id,case_id,user_id,body,created_at}.
  GET rules -> {rows: [Rule]}; POST rules {name,threshold,portfolio_id?} -> Rule;
    null portfolio_id is organization-wide. PATCH rules/{id} {enabled} -> Rule.
  GET alerts -> {rows: [Alert + {deliveries: [{channel,status,error,updated_at}]}]}.
  PATCH alerts/{id} {status: new|reviewed|resolved} -> Alert.
  POST alerts/{id}/retry-delivery -> {alert_id,queued,deliveries}; requeues only
    error/unconfigured channels, never sent/sending channels or resolved alerts.
  GET checks?portfolio_id= -> {rows: [Check]}, newest first, maximum 100.
  GET reports/portfolio.csv -> CSV attachment; GET reports/summary -> Summary.
  GET reports -> {rows: [Schedule + {last_generated_at}]}.
  POST reports {name,frequency: daily|weekly} -> Schedule, creates first snapshot.
  GET reports/{id} -> Schedule + {runs: [Run],summary: latest saved Summary}.
  GET audit -> {rows: [org-scoped audit_log records]}, admin only.

Portfolio fields: id,org_id,title,category,external_id,title_key,status,
  confidence,candidates,metadata,source,created_at. JSON candidates/metadata
  contain authoritative catalogue metadata; pending title_key is null.
Case fields: id,org_id,portfolio_id,title,description,status,assignee_id,
  created_by,created_at,updated_at.
Rule fields: id,org_id,name,threshold,portfolio_id,enabled,created_at.
Alert fields: id,org_id,rule_id,portfolio_id,check_id,status,created_at,updated_at.
  Alert responses also include title,rule_name,threshold,max_seeders,message,
  delivery_status and deliveries. delivery_status is the common channel status,
  "pending" while queued/sending, or "partial" if final channel states differ.
Check fields: id,org_id,portfolio_id,max_seeders,checked_at,fresh,result,created_at.
Summary fields: org_id,generated_at,portfolio,cases,alerts,checks,counts,
  methodology,limitations; portfolio/cases/alerts are status-to-count maps.
Schedule fields: id,org_id,name,frequency,created_by,created_at.
Run fields: id,org_id,report_id,period,generated_at,summary.

Only admin/analyst may mutate. Viewers may read/export; exports still record
audit entries. Delivery states are pending/sending/sent/error/unconfigured;
failed/unconfigured delivery is not retried automatically. Schedules use UTC
periods, generate once per current period, and do not backfill missed periods.
"""

import csv
import http.client
import io
import ipaddress
import json
import logging
import os
import queue
import re
import smtplib
import socket
import ssl
import threading
from datetime import UTC, datetime
from email.message import EmailMessage
from typing import Annotated, Literal
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

from portal.accounts import AccountStore, Actor, current_actor, require_roles
from portal.titles import resolve_title, search_titles

LOG = logging.getLogger(__name__)
LIMITATIONS = [
    "Tracker seeders are reported observations, not unique people or verified infringement.",
    "Counts use the maximum tracker-reported seeders per record, never a sum.",
    "Unavailable or stale checks do not trigger alerts; coverage may be incomplete.",
    "Imported titles need explicit review; candidate matches are not approvals.",
]


def _now():
    return datetime.now(UTC).isoformat()


def _dump(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def _write(actor):
    require_roles(actor, "admin", "analyst")


def _audit(store, con, actor, action, entity, detail=""):
    store.audit(con, actor, action, entity, detail)


def _owned(con, table, ident, actor):
    # Table names are fixed internal literals, never request inputs.
    row = con.execute(
        f"SELECT * FROM {table} WHERE id=? AND org_id=?", (ident, actor.org_id)
    ).fetchone()
    if row is None:
        raise HTTPException(404, "Record not found")
    return row


def _assignee(con, actor, ident):
    if ident is not None and not con.execute(
        "SELECT 1 FROM users WHERE id=? AND org_id=?", (ident, actor.org_id)
    ).fetchone():
        raise HTTPException(400, "Assignee must belong to this organization")


def _row(row):
    result = dict(row)
    for column in ("candidates", "metadata", "result", "summary"):
        if column in result:
            result[column] = json.loads(result[column])
    if "enabled" in result:
        result["enabled"] = bool(result["enabled"])
    if "fresh" in result:
        result["fresh"] = bool(result["fresh"])
    return result


def _deliveries(con, actor, alert_id):
    return [dict(row) for row in con.execute(
        """SELECT channel,status,error,updated_at FROM company_deliveries
           WHERE alert_id=? AND org_id=? ORDER BY id""", (alert_id, actor.org_id),
    )]


def _alert_response(con, actor, alert_id):
    _owned(con, "company_alerts", alert_id, actor)
    alert = dict(con.execute(
        """SELECT a.*,p.title,r.name AS rule_name,r.threshold,c.max_seeders
        FROM company_alerts a
        JOIN company_portfolio p ON p.id=a.portfolio_id AND p.org_id=a.org_id
        JOIN company_rules r ON r.id=a.rule_id AND r.org_id=a.org_id
        JOIN company_checks c ON c.id=a.check_id AND c.org_id=a.org_id
        WHERE a.id=? AND a.org_id=?""", (alert_id, actor.org_id),
    ).fetchone())
    alert["message"] = (
        f"{alert['title']}: maximum tracker-reported seeders {alert['max_seeders']} "
        f"met threshold {alert['threshold']} ({alert['rule_name']})."
    )
    alert["deliveries"] = _deliveries(con, actor, alert_id)
    statuses = {delivery["status"] for delivery in alert["deliveries"]}
    alert["delivery_status"] = (
        "pending" if statuses & {"pending", "sending"}
        else next(iter(statuses)) if len(statuses) == 1
        else "partial" if statuses else "unconfigured"
    )
    return alert


def initialise_company(store: AccountStore):
    with store.connect() as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS company_portfolio(
          id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL,
          title TEXT NOT NULL, category TEXT NOT NULL, external_id TEXT NOT NULL DEFAULT '',
          title_key TEXT, status TEXT NOT NULL CHECK(status IN ('pending','approved')),
          confidence TEXT NOT NULL, candidates TEXT NOT NULL DEFAULT '[]',
          metadata TEXT NOT NULL DEFAULT '{}', source TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS company_portfolio_title
          ON company_portfolio(org_id,title_key) WHERE title_key IS NOT NULL;
        CREATE TABLE IF NOT EXISTS company_cases(
          id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL,
          portfolio_id INTEGER NOT NULL REFERENCES company_portfolio(id),
          title TEXT NOT NULL, description TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'open', assignee_id INTEGER REFERENCES users(id),
          created_by INTEGER NOT NULL REFERENCES users(id),
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS company_notes(
          id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL,
          case_id INTEGER NOT NULL REFERENCES company_cases(id),
          user_id INTEGER NOT NULL REFERENCES users(id), body TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS company_rules(
          id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL, name TEXT NOT NULL,
          threshold INTEGER NOT NULL CHECK(threshold>=0),
          portfolio_id INTEGER REFERENCES company_portfolio(id),
          enabled INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS company_checks(
          id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL,
          portfolio_id INTEGER NOT NULL REFERENCES company_portfolio(id),
          max_seeders INTEGER, checked_at TEXT NOT NULL, fresh INTEGER NOT NULL,
          result TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS company_alerts(
          id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL,
          rule_id INTEGER NOT NULL REFERENCES company_rules(id),
          portfolio_id INTEGER NOT NULL REFERENCES company_portfolio(id),
          check_id INTEGER NOT NULL REFERENCES company_checks(id),
          status TEXT NOT NULL DEFAULT 'new', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS company_alert_unresolved
          ON company_alerts(org_id,rule_id,portfolio_id) WHERE status!='resolved';
        CREATE TABLE IF NOT EXISTS company_deliveries(
          id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL,
          alert_id INTEGER NOT NULL REFERENCES company_alerts(id),
          created_by INTEGER NOT NULL REFERENCES users(id),
          channel TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
          error TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL,
          UNIQUE(alert_id,channel));
        CREATE TABLE IF NOT EXISTS company_reports(
          id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL, name TEXT NOT NULL,
          frequency TEXT NOT NULL, created_by INTEGER NOT NULL REFERENCES users(id),
          created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS company_report_runs(
          id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL,
          report_id INTEGER NOT NULL REFERENCES company_reports(id),
          period TEXT NOT NULL, generated_at TEXT NOT NULL, summary TEXT NOT NULL,
          UNIQUE(report_id,period));
        CREATE INDEX IF NOT EXISTS company_checks_scope
          ON company_checks(org_id,portfolio_id,id);
        """)


class ImportPortfolio(BaseModel):
    csv_text: str = Field(max_length=1_048_576)


class Selection(BaseModel):
    key: str = Field(min_length=1, max_length=600)


class CaseCreate(BaseModel):
    portfolio_id: int = Field(gt=0)
    title: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=10_000)
    assignee_id: int | None = Field(default=None, gt=0)


class CaseUpdate(BaseModel):
    status: Literal["open", "investigating", "actioned", "closed"]
    assignee_id: int | None = Field(default=None, gt=0)


class NoteCreate(BaseModel):
    body: str = Field(min_length=1, max_length=10_000)


class RuleCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    threshold: int = Field(ge=0, strict=True)
    portfolio_id: int | None = Field(default=None, gt=0)


class RuleUpdate(BaseModel):
    enabled: bool = Field(strict=True)


class AlertUpdate(BaseModel):
    status: Literal["new", "reviewed", "resolved"]


class ReportCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    frequency: Literal["daily", "weekly"]


def _text(value, label):
    value = value.strip()
    if not value:
        raise HTTPException(400, f"{label} cannot be blank")
    if "\x00" in value:
        raise HTTPException(400, f"{label} cannot contain NUL")
    return value


def _parse_import(text):
    if len(text.encode("utf-8")) > 1_048_576:
        raise HTTPException(400, "CSV exceeds 1 MB")
    errors, parsed = [], []
    try:
        reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")), strict=True)
        headers = reader.fieldnames
        if not headers or len(headers) != len(set(headers)):
            raise HTTPException(400, "CSV requires unique headers")
        if not {"title", "category"}.issubset(headers) or set(headers) - {
            "title", "category", "external_id"
        }:
            raise HTTPException(400, "CSV headers: title,category[,external_id]")
        for index, row in enumerate(reader, 2):
            if len(parsed) + len(errors) >= 200:
                raise HTTPException(400, "CSV exceeds 200 rows")
            error = None
            title = (row.get("title") or "").strip()
            category = (row.get("category") or "").strip().lower()
            external = (row.get("external_id") or "").strip()
            if None in row or any(value is None for value in row.values()):
                error = "Row column count does not match header"
            elif not title or len(title) > 500 or "\x00" in title:
                error = "title must contain 1–500 characters without NUL"
            elif category not in ("movie", "tv", "game"):
                error = "category must be movie, tv, or game"
            elif len(external) > 200 or "\x00" in external:
                error = "external_id must be at most 200 characters without NUL"
            if error:
                errors.append({"row": index, "error": error})
            else:
                parsed.append((title, category, external))
    except csv.Error as exc:
        raise HTTPException(400, f"Malformed CSV: {exc}") from exc
    if errors:
        raise HTTPException(400, {"message": "CSV validation failed; nothing imported", "rows": errors})
    if not parsed:
        raise HTTPException(400, "CSV contains no data rows")
    return parsed


def _summary(con, org_id):
    def counts(table, column):
        return {
            row[0]: row[1]
            for row in con.execute(
                f"SELECT {column},COUNT(*) FROM {table} WHERE org_id=? GROUP BY {column}",
                (org_id,),
            )
        }
    result = {
        "org_id": org_id,
        "generated_at": _now(),
        "portfolio": counts("company_portfolio", "status"),
        "cases": counts("company_cases", "status"),
        "alerts": counts("company_alerts", "status"),
        "checks": con.execute(
            "SELECT COUNT(*) FROM company_checks WHERE org_id=?", (org_id,)
        ).fetchone()[0],
        "methodology": "Maximum tracker-reported seeders per record; never summed.",
        "limitations": LIMITATIONS,
    }
    result["counts"] = {
        "portfolio": sum(result["portfolio"].values()),
        "cases": sum(result["cases"].values()),
        "alerts": sum(result["alerts"].values()),
        "checks": result["checks"],
    }
    return result


def _period(frequency, now):
    if frequency == "daily":
        return now.strftime("%Y-%m-%d")
    year, week, _ = now.isocalendar()
    return f"{year}-W{week:02d}"


def _generate_report(store, con, actor, report, now):
    period = _period(report["frequency"], now)
    cur = con.execute(
        """INSERT OR IGNORE INTO company_report_runs
           (org_id,report_id,period,generated_at,summary) VALUES(?,?,?,?,?)""",
        (actor.org_id, report["id"], period, now.isoformat(), _dump(_summary(con, actor.org_id))),
    )
    if cur.rowcount:
        _audit(store, con, actor, "report.generate", str(report["id"]), period)
    return cur.rowcount


def run_schedules(store: AccountStore):
    """Generate the current UTC day/week once; no backfilling missed periods."""
    now, generated = datetime.now(UTC), 0
    with store.connect() as con:
        con.execute("BEGIN IMMEDIATE")
        reports = con.execute(
            """SELECT r.*,u.name AS creator_name,u.email AS creator_email,u.role AS creator_role
               FROM company_reports r JOIN users u ON u.id=r.created_by AND u.org_id=r.org_id"""
        ).fetchall()
        for report in reports:
            actor = Actor(report["org_id"], report["created_by"], report["creator_role"],
                          report["creator_name"], report["creator_email"])
            generated += _generate_report(store, con, actor, report, now)
    return {"generated": generated}


def company_router(store: AccountStore) -> APIRouter:
    initialise_company(store)
    router = APIRouter(prefix="/api/company", tags=["company"])

    @router.get("/portfolio")
    def portfolio(actor: Annotated[Actor, Depends(current_actor)],
                  limit: int = Query(100, ge=1, le=200), offset: int = Query(0, ge=0)):
        with store.connect() as con:
            rows = con.execute(
                "SELECT * FROM company_portfolio WHERE org_id=? ORDER BY id DESC LIMIT ? OFFSET ?",
                (actor.org_id, limit, offset),
            ).fetchall()
            return {"rows": [_row(row) for row in rows]}

    @router.post("/portfolio/import")
    def import_portfolio(payload: ImportPortfolio, actor: Annotated[Actor, Depends(current_actor)]):
        _write(actor)
        parsed = _parse_import(payload.csv_text)
        prepared = []
        # Resolve all candidates before the transaction: failure never causes a partial import.
        for title, category, external in parsed:
            query = external if (
                re.fullmatch(r"tt\d+", external)
                or category == "game" and external.isdigit() and len(external) <= 12
            ) else title
            # The catalogue search accepts 2–150 characters. Titles outside that
            # range remain explicit unmatched review rows instead of being truncated.
            candidates = search_titles(query, category)[:3] if 2 <= len(query) <= 150 else []
            exact = any(
                item["title"].casefold() == title.casefold()
                and item["category"] == category for item in candidates
            )
            prepared.append((title, category, external, candidates,
                             "exact" if exact else "candidate" if candidates else "unmatched"))
        with store.connect() as con:
            rows = []
            for title, category, external, candidates, confidence in prepared:
                cur = con.execute(
                    """INSERT INTO company_portfolio
                    (org_id,title,category,external_id,status,confidence,candidates,source,created_at)
                    VALUES(?,?,?,?,'pending',?,?,'csv',?)""",
                    (actor.org_id, title, category, external, confidence, _dump(candidates), _now()),
                )
                rows.append(_row(_owned(con, "company_portfolio", cur.lastrowid, actor)))
            _audit(store, con, actor, "portfolio.import", "portfolio", str(len(rows)))
            return {"rows": rows}

    @router.post("/portfolio")
    def add_portfolio(payload: Selection, actor: Annotated[Actor, Depends(current_actor)]):
        _write(actor)
        title = resolve_title(payload.key)
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            if con.execute(
                "SELECT 1 FROM company_portfolio WHERE org_id=? AND title_key=?",
                (actor.org_id, title["key"]),
            ).fetchone():
                raise HTTPException(409, "Title already in portfolio")
            cur = con.execute(
                """INSERT INTO company_portfolio
                (org_id,title,category,title_key,status,confidence,metadata,source,created_at)
                VALUES(?,?,?,?,'approved','manual',?,'manual',?)""",
                (actor.org_id, title["title"], title["category"], title["key"], _dump(title), _now()),
            )
            _audit(store, con, actor, "portfolio.add", str(cur.lastrowid))
            return _row(_owned(con, "company_portfolio", cur.lastrowid, actor))

    @router.post("/portfolio/{ident}/approve")
    def approve_portfolio(ident: int, payload: Selection, actor: Annotated[Actor, Depends(current_actor)]):
        _write(actor)
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            _owned(con, "company_portfolio", ident, actor)
            title = resolve_title(payload.key)
            if con.execute(
                "SELECT 1 FROM company_portfolio WHERE org_id=? AND title_key=? AND id!=?",
                (actor.org_id, title["key"], ident),
            ).fetchone():
                raise HTTPException(409, "Title already in portfolio")
            if con.execute(
                "SELECT 1 FROM company_checks WHERE portfolio_id=? UNION ALL "
                "SELECT 1 FROM company_cases WHERE portfolio_id=?",
                (ident, ident),
            ).fetchone():
                raise HTTPException(409, "A title with case/check history cannot be rematched")
            con.execute(
                """UPDATE company_portfolio SET title=?,category=?,title_key=?,status='approved',
                confidence='manual',metadata=? WHERE id=? AND org_id=?""",
                (title["title"], title["category"], title["key"], _dump(title), ident, actor.org_id),
            )
            _audit(store, con, actor, "portfolio.approve", str(ident))
            return _row(_owned(con, "company_portfolio", ident, actor))

    @router.delete("/portfolio/{ident}")
    def delete_portfolio(ident: int, actor: Annotated[Actor, Depends(current_actor)]):
        _write(actor)
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            _owned(con, "company_portfolio", ident, actor)
            for table in ("company_cases", "company_rules", "company_checks", "company_alerts"):
                if con.execute(f"SELECT 1 FROM {table} WHERE portfolio_id=?", (ident,)).fetchone():
                    raise HTTPException(409, "Portfolio title has referenced history")
            con.execute("DELETE FROM company_portfolio WHERE id=? AND org_id=?", (ident, actor.org_id))
            _audit(store, con, actor, "portfolio.delete", str(ident))
            return {"deleted": ident}

    @router.get("/cases")
    def cases(actor: Annotated[Actor, Depends(current_actor)]):
        with store.connect() as con:
            return {"rows": [_row(r) for r in con.execute(
                """SELECT c.*,u.name AS assignee_name FROM company_cases c
                LEFT JOIN users u ON u.id=c.assignee_id AND u.org_id=c.org_id
                WHERE c.org_id=? ORDER BY c.id DESC LIMIT 200""",
                (actor.org_id,),
            )]}

    @router.post("/cases")
    def create_case(payload: CaseCreate, actor: Annotated[Actor, Depends(current_actor)]):
        _write(actor)
        title = _text(payload.title, "Title")
        with store.connect() as con:
            _owned(con, "company_portfolio", payload.portfolio_id, actor)
            _assignee(con, actor, payload.assignee_id)
            now = _now()
            cur = con.execute(
                """INSERT INTO company_cases(org_id,portfolio_id,title,description,assignee_id,
                   created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)""",
                (actor.org_id, payload.portfolio_id, title, payload.description,
                 payload.assignee_id, actor.user_id, now, now),
            )
            _audit(store, con, actor, "case.create", str(cur.lastrowid))
            return _row(_owned(con, "company_cases", cur.lastrowid, actor))

    @router.get("/cases/{ident}")
    def case_detail(ident: int, actor: Annotated[Actor, Depends(current_actor)]):
        with store.connect() as con:
            result = _row(_owned(con, "company_cases", ident, actor))
            assignee = con.execute(
                "SELECT name FROM users WHERE id=? AND org_id=?",
                (result["assignee_id"], actor.org_id),
            ).fetchone()
            result["assignee_name"] = assignee["name"] if assignee else None
            result["notes"] = [_row(r) for r in con.execute(
                "SELECT * FROM company_notes WHERE org_id=? AND case_id=? ORDER BY id",
                (actor.org_id, ident),
            )]
            latest = con.execute(
                "SELECT * FROM company_checks WHERE org_id=? AND portfolio_id=? ORDER BY id DESC LIMIT 1",
                (actor.org_id, result["portfolio_id"]),
            ).fetchone()
            result["check_summary"] = _row(latest) if latest else None
            return result

    @router.patch("/cases/{ident}")
    def update_case(ident: int, payload: CaseUpdate, actor: Annotated[Actor, Depends(current_actor)]):
        _write(actor)
        with store.connect() as con:
            previous = _owned(con, "company_cases", ident, actor)
            supplied = payload.model_fields_set
            assignee = payload.assignee_id if "assignee_id" in supplied else previous["assignee_id"]
            _assignee(con, actor, assignee)
            con.execute(
                "UPDATE company_cases SET status=?,assignee_id=?,updated_at=? WHERE id=? AND org_id=?",
                (payload.status, assignee, _now(), ident, actor.org_id),
            )
            _audit(store, con, actor, "case.update", str(ident), payload.status)
            return _row(_owned(con, "company_cases", ident, actor))

    @router.post("/cases/{ident}/notes")
    def add_note(ident: int, payload: NoteCreate, actor: Annotated[Actor, Depends(current_actor)]):
        _write(actor)
        body = _text(payload.body, "Note")
        with store.connect() as con:
            _owned(con, "company_cases", ident, actor)
            cur = con.execute(
                "INSERT INTO company_notes(org_id,case_id,user_id,body,created_at) VALUES(?,?,?,?,?)",
                (actor.org_id, ident, actor.user_id, body, _now()),
            )
            _audit(store, con, actor, "case.note", str(ident))
            return dict(con.execute("SELECT * FROM company_notes WHERE id=?", (cur.lastrowid,)).fetchone())

    @router.get("/rules")
    def rules(actor: Annotated[Actor, Depends(current_actor)]):
        with store.connect() as con:
            return {"rows": [_row(r) for r in con.execute(
                "SELECT * FROM company_rules WHERE org_id=? ORDER BY id DESC LIMIT 200", (actor.org_id,)
            )]}

    @router.post("/rules")
    def create_rule(payload: RuleCreate, actor: Annotated[Actor, Depends(current_actor)]):
        _write(actor)
        name = _text(payload.name, "Name")
        with store.connect() as con:
            if payload.portfolio_id is not None:
                _owned(con, "company_portfolio", payload.portfolio_id, actor)
            cur = con.execute(
                "INSERT INTO company_rules(org_id,name,threshold,portfolio_id,created_at) VALUES(?,?,?,?,?)",
                (actor.org_id, name, payload.threshold, payload.portfolio_id, _now()),
            )
            _audit(store, con, actor, "rule.create", str(cur.lastrowid))
            return _row(_owned(con, "company_rules", cur.lastrowid, actor))

    @router.patch("/rules/{ident}")
    def update_rule(ident: int, payload: RuleUpdate, actor: Annotated[Actor, Depends(current_actor)]):
        _write(actor)
        with store.connect() as con:
            _owned(con, "company_rules", ident, actor)
            con.execute("UPDATE company_rules SET enabled=? WHERE id=? AND org_id=?",
                        (payload.enabled, ident, actor.org_id))
            _audit(store, con, actor, "rule.update", str(ident), str(payload.enabled))
            return _row(_owned(con, "company_rules", ident, actor))

    @router.get("/alerts")
    def alerts(actor: Annotated[Actor, Depends(current_actor)]):
        with store.connect() as con:
            identifiers = con.execute(
                "SELECT id FROM company_alerts WHERE org_id=? ORDER BY id DESC LIMIT 200",
                (actor.org_id,),
            ).fetchall()
            return {"rows": [_alert_response(con, actor, row["id"]) for row in identifiers]}

    @router.patch("/alerts/{ident}")
    def update_alert(ident: int, payload: AlertUpdate, actor: Annotated[Actor, Depends(current_actor)]):
        _write(actor)
        with store.connect() as con:
            alert = _owned(con, "company_alerts", ident, actor)
            if alert["status"] == "resolved" and payload.status != "resolved":
                raise HTTPException(409, "Resolved alerts are immutable; a fresh check may create a new alert")
            con.execute(
                "UPDATE company_alerts SET status=?,updated_at=? WHERE id=? AND org_id=?",
                (payload.status, _now(), ident, actor.org_id),
            )
            _audit(store, con, actor, "alert.update", str(ident), payload.status)
            return _alert_response(con, actor, ident)

    @router.post("/alerts/{ident}/retry-delivery")
    def retry_delivery(ident: int, actor: Annotated[Actor, Depends(current_actor)]):
        _write(actor)
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            alert = _owned(con, "company_alerts", ident, actor)
            if alert["status"] == "resolved":
                raise HTTPException(409, "Resolved alerts cannot be redelivered")
            queued = con.execute(
                """UPDATE company_deliveries SET status='pending',error='',updated_at=?
                WHERE alert_id=? AND org_id=? AND status IN ('error','unconfigured')""",
                (_now(), ident, actor.org_id),
            ).rowcount
            if not queued:
                raise HTTPException(409, "No failed or unconfigured deliveries to retry")
            _audit(store, con, actor, "delivery.retry", str(ident), str(queued))
            return {"alert_id": ident, "queued": queued,
                    "deliveries": _deliveries(con, actor, ident)}

    @router.get("/checks")
    def checks(actor: Annotated[Actor, Depends(current_actor)],
               portfolio_id: int | None = Query(None, gt=0)):
        with store.connect() as con:
            if portfolio_id is not None:
                _owned(con, "company_portfolio", portfolio_id, actor)
            rows = con.execute(
                """SELECT * FROM company_checks WHERE org_id=?
                AND (? IS NULL OR portfolio_id=?) ORDER BY id DESC LIMIT 100""",
                (actor.org_id, portfolio_id, portfolio_id),
            )
            return {"rows": [_row(r) for r in rows]}

    @router.get("/reports/portfolio.csv")
    def export_portfolio(actor: Annotated[Actor, Depends(current_actor)]):
        with store.connect() as con:
            rows = con.execute(
                "SELECT * FROM company_portfolio WHERE org_id=? ORDER BY id", (actor.org_id,)
            ).fetchall()
            output = io.StringIO(newline="")
            writer = csv.writer(output)
            writer.writerow(["id", "title", "category", "external_id", "title_key", "status",
                             "confidence", "source", "methodology", "exported_at"])
            exported = _now()
            for row in rows:
                values = [row[key] for key in ("id", "title", "category", "external_id",
                                               "title_key", "status", "confidence", "source")]
                writer.writerow([_csv_safe(v) for v in values] + [
                    "Maximum tracker-reported seeders, not summed; pending titles unverified", exported
                ])
            _audit(store, con, actor, "portfolio.export", "portfolio", str(len(rows)))
        return Response(output.getvalue(), media_type="text/csv",
                        headers={"Content-Disposition": 'attachment; filename="portfolio.csv"'})

    @router.get("/reports/summary")
    def summary(actor: Annotated[Actor, Depends(current_actor)]):
        with store.connect() as con:
            result = _summary(con, actor.org_id)
            _audit(store, con, actor, "report.export", "summary")
            return result

    @router.get("/reports")
    def reports(actor: Annotated[Actor, Depends(current_actor)]):
        with store.connect() as con:
            return {"rows": [dict(row) for row in con.execute(
                """SELECT r.*,(SELECT MAX(generated_at) FROM company_report_runs s
                WHERE s.report_id=r.id AND s.org_id=r.org_id) AS last_generated_at
                FROM company_reports r WHERE r.org_id=? ORDER BY id DESC LIMIT 200""",
                (actor.org_id,),
            )]}

    @router.post("/reports")
    def create_report(payload: ReportCreate, actor: Annotated[Actor, Depends(current_actor)]):
        _write(actor)
        name = _text(payload.name, "Name")
        with store.connect() as con:
            cur = con.execute(
                "INSERT INTO company_reports(org_id,name,frequency,created_by,created_at) VALUES(?,?,?,?,?)",
                (actor.org_id, name, payload.frequency, actor.user_id, _now()),
            )
            report = _owned(con, "company_reports", cur.lastrowid, actor)
            _audit(store, con, actor, "report.create", str(cur.lastrowid))
            _generate_report(store, con, actor, report, datetime.now(UTC))
            return dict(report)

    @router.get("/reports/{ident}")
    def report_detail(ident: int, actor: Annotated[Actor, Depends(current_actor)]):
        with store.connect() as con:
            result = dict(_owned(con, "company_reports", ident, actor))
            result["runs"] = [_row(r) for r in con.execute(
                "SELECT * FROM company_report_runs WHERE org_id=? AND report_id=? ORDER BY id DESC LIMIT 100",
                (actor.org_id, ident),
            )]
            result["summary"] = result["runs"][0]["summary"] if result["runs"] else None
            _audit(store, con, actor, "report.export", str(ident))
            return result

    @router.get("/audit")
    def audit(actor: Annotated[Actor, Depends(current_actor)]):
        require_roles(actor, "admin")
        with store.connect() as con:
            return {"rows": [dict(r) for r in con.execute(
                "SELECT * FROM audit_log WHERE org_id=? ORDER BY id DESC LIMIT 200", (actor.org_id,)
            )]}

    return router


def _csv_safe(value):
    text = "" if value is None else str(value)
    if text.lstrip().startswith(("=", "+", "-", "@")) or text.startswith(("\t", "\r", "\n")):
        return "'" + text
    return text


def portfolio_item(store: AccountStore, actor: Actor, portfolio_id: int) -> dict:
    """Resolve the approved, same-tenant title before performing a live check."""
    _write(actor)
    with store.connect() as con:
        row = _owned(con, "company_portfolio", portfolio_id, actor)
        if row["status"] != "approved" or not row["title_key"]:
            raise HTTPException(409, "Approve the portfolio title before checking")
        return {**_row(row), "key": row["title_key"]}


def record_check(store: AccountStore, actor: Actor, portfolio_id: int, result: dict) -> dict:
    """Persist an actual check and return its snapshot plus newly created alerts."""
    _write(actor)
    if not isinstance(result, dict):
        raise HTTPException(400, "Check result must be an object")
    maximum = result.get("max_seeders")
    observations = result.get("observations")
    if (maximum is not None and (type(maximum) is not int or maximum < 0)
            or type(result.get("fresh")) is not bool
            or not isinstance(observations, list)
            or not isinstance(result.get("errors"), list)):
        raise HTTPException(400, "Invalid check result")
    checked_at = result.get("checked_at")
    try:
        stamp = datetime.fromisoformat(checked_at)
        if stamp.tzinfo is None:
            raise ValueError("Timezone required")
    except (AttributeError, TypeError, ValueError) as exc:
        raise HTTPException(400, "checked_at must be an ISO timestamp with timezone") from exc
    reported = []
    for observation in observations:
        if not isinstance(observation, dict) or not all(
            key in observation for key in ("infohash", "tracker", "seeders", "leechers", "checked_at")
        ):
            raise HTTPException(400, "Invalid tracker observation")
        for field in ("seeders", "leechers"):
            count = observation[field]
            if count is not None and (type(count) is not int or count < 0):
                raise HTTPException(400, f"Invalid {field} observation")
        if observation["seeders"] is not None:
            reported.append(observation["seeders"])
    # Never permit upstream aggregation to turn tracker reports into a summed count.
    if observations:
        maximum = max(reported) if reported else None
    snapshot = {**result, "max_seeders": maximum}
    try:
        encoded = _dump(snapshot)
    except (TypeError, ValueError) as exc:
        raise HTTPException(400, "Check result must be JSON serializable") from exc
    with store.connect() as con:
        con.execute("BEGIN IMMEDIATE")
        portfolio = _owned(con, "company_portfolio", portfolio_id, actor)
        if portfolio["status"] != "approved":
            raise HTTPException(409, "Approve the portfolio title before checking")
        if result.get("key") is not None and result["key"] != portfolio["title_key"]:
            raise HTTPException(400, "Check result does not match the portfolio title")
        cur = con.execute(
            """INSERT INTO company_checks(org_id,portfolio_id,max_seeders,checked_at,fresh,result,created_at)
               VALUES(?,?,?,?,?,?,?)""",
            (actor.org_id, portfolio_id, maximum, checked_at, result["fresh"], encoded, _now()),
        )
        check_id, created = cur.lastrowid, []
        if result["fresh"] and maximum is not None:
            rules = con.execute(
                """SELECT * FROM company_rules WHERE org_id=? AND enabled=1
                AND (portfolio_id IS NULL OR portfolio_id=?) AND threshold<=?""",
                (actor.org_id, portfolio_id, maximum),
            ).fetchall()
            for rule in rules:
                now = _now()
                inserted = con.execute(
                    """INSERT OR IGNORE INTO company_alerts
                    (org_id,rule_id,portfolio_id,check_id,created_at,updated_at) VALUES(?,?,?,?,?,?)""",
                    (actor.org_id, rule["id"], portfolio_id, check_id, now, now),
                )
                if not inserted.rowcount:
                    continue
                alert_id = inserted.lastrowid
                for channel in ("email", "webhook"):
                    con.execute(
                        """INSERT INTO company_deliveries(org_id,alert_id,created_by,channel,updated_at)
                        VALUES(?,?,?,?,?)""", (actor.org_id, alert_id, actor.user_id, channel, now),
                    )
                _audit(store, con, actor, "alert.create", str(alert_id), f"check:{check_id}")
                created.append(_alert_response(con, actor, alert_id))
        _audit(store, con, actor, "check.record", str(check_id), f"portfolio:{portfolio_id}")
        return {"check": _row(_owned(con, "company_checks", check_id, actor)), "alerts": created}


class _Unconfigured(ValueError):
    pass


def _resolve(host, port):
    """Bound OS resolver latency without leaving non-daemon pool workers."""
    answers = queue.Queue(maxsize=1)

    def resolve():
        try:
            answers.put((socket.getaddrinfo(host, port, type=socket.SOCK_STREAM), None))
        except OSError as exc:
            answers.put((None, exc))

    threading.Thread(target=resolve, daemon=True, name="cwatch-delivery-dns").start()
    try:
        addresses, error = answers.get(timeout=4)
    except queue.Empty as exc:
        raise TimeoutError("Delivery DNS timed out") from exc
    if error is not None:
        raise error
    return addresses


class _BoundedSMTP(smtplib.SMTP):
    def _get_socket(self, host, port, timeout):
        addresses = _resolve(host, port)
        if not addresses:
            raise OSError("SMTP hostname has no addresses")
        family, kind, protocol, _, address = addresses[0]
        sock = socket.socket(family, kind, protocol)
        try:
            sock.settimeout(timeout)
            sock.connect(address)
            return sock
        except OSError:
            sock.close()
            raise


def _email_delivery(payload):
    host = os.environ.get("CWATCH_SMTP_HOST", "")
    sender = os.environ.get("CWATCH_SMTP_FROM", "")
    recipient = os.environ.get("CWATCH_ALERT_EMAIL", "")
    address = re.compile(r"[^@\s<>,;\r\n]+@[^@\s<>,;\r\n]+\.[^@\s<>,;\r\n]+")
    if not host or not address.fullmatch(sender) or not address.fullmatch(recipient):
        raise _Unconfigured("SMTP host/from or alert email absent or invalid")
    try:
        port = int(os.environ.get("CWATCH_SMTP_PORT", "587"))
        if not 1 <= port <= 65535:
            raise ValueError("port")
    except ValueError as exc:
        raise _Unconfigured("Invalid SMTP port") from exc
    user, password = os.environ.get("CWATCH_SMTP_USER"), os.environ.get("CWATCH_SMTP_PASSWORD")
    if bool(user) != bool(password):
        raise _Unconfigured("SMTP user/password must both be configured")
    message = EmailMessage()
    message["From"], message["To"] = sender, recipient
    message["Subject"] = f"ExposureIQ alert {payload['alert_id']}"
    message.set_content(_dump(payload))
    # Credentials and alert data are never sent on an unencrypted SMTP session.
    with _BoundedSMTP(host, port, timeout=8) as smtp:
        smtp.ehlo()
        smtp.starttls(context=ssl.create_default_context())
        smtp.ehlo()
        if user:
            smtp.login(user, password)
        refused = smtp.send_message(message)
        if refused:
            raise RuntimeError("SMTP recipient refused")


def _public_webhook(url):
    try:
        target = urlsplit(url)
        port = target.port or 443
        host = target.hostname
        if (target.scheme != "https" or not host or target.username or target.password
                or target.fragment or port != 443 or "\r" in url or "\n" in url):
            raise ValueError("HTTPS on port 443 without credentials required")
        addresses = _resolve(host, port)
        if not addresses or any(
            not ipaddress.ip_address(item[4][0]).is_global
            or ipaddress.ip_address(item[4][0]).is_multicast for item in addresses
        ):
            raise ValueError("Webhook must resolve exclusively to public addresses")
        return target, addresses[0]
    except (ValueError, socket.gaierror) as exc:
        raise _Unconfigured("Webhook must be a public HTTPS URL") from exc


def _webhook_delivery(payload):
    url = os.environ.get("CWATCH_ALERT_WEBHOOK", "")
    if not url:
        raise _Unconfigured("Webhook not configured")
    target, address = _public_webhook(url)
    host = target.hostname
    connection = http.client.HTTPSConnection(host, 443, timeout=8)
    # Pin the validated address, but retain the hostname for certificate/SNI checks.
    raw = socket.socket(address[0], address[1], address[2])
    try:
        raw.settimeout(8)
        raw.connect(address[4])
        connection.sock = ssl.create_default_context().wrap_socket(raw, server_hostname=host)
        connection.request("POST", (target.path or "/") + ("?" + target.query if target.query else ""),
                           body=_dump(payload).encode("utf-8"),
                           headers={"Content-Type": "application/json"})
        response = connection.getresponse()
        if not 200 <= response.status < 300:
            raise RuntimeError(f"Webhook returned HTTP {response.status}")
        # Redirects are deliberately not followed.
    finally:
        connection.close()
        raw.close()


def deliver_pending(store: AccountStore):
    """Explicit, bounded background delivery. Errors are durable, never marked sent.

    Processes up to 20 queued channels per call, at most eight seconds per network
    operation. Failed/unconfigured channels remain visible and are not auto-retried.
    A process crash after a claim leaves 'sending' for operator inspection.
    """
    completed = 0
    with store.connect() as con:
        identifiers = [r[0] for r in con.execute(
            "SELECT id FROM company_deliveries WHERE status='pending' ORDER BY id LIMIT 20"
        )]
    for ident in identifiers:
        with store.connect() as con:
            claimed = con.execute(
                "UPDATE company_deliveries SET status='sending',updated_at=? WHERE id=? AND status='pending'",
                (_now(), ident),
            )
            if not claimed.rowcount:
                continue
            row = con.execute(
                """SELECT d.*,a.rule_id,a.portfolio_id,c.result,p.title,
                u.name AS creator_name,u.email AS creator_email,u.role AS creator_role
                FROM company_deliveries d
                JOIN company_alerts a ON a.id=d.alert_id AND a.org_id=d.org_id
                JOIN company_checks c ON c.id=a.check_id AND c.org_id=a.org_id
                JOIN company_portfolio p ON p.id=a.portfolio_id AND p.org_id=a.org_id
                JOIN users u ON u.id=d.created_by AND u.org_id=d.org_id WHERE d.id=?""",
                (ident,),
            ).fetchone()
            actor = Actor(row["org_id"], row["created_by"], row["creator_role"],
                          row["creator_name"], row["creator_email"])
            _audit(store, con, actor, "delivery.claim", str(ident))
            payload = {"alert_id": row["alert_id"], "org_id": row["org_id"],
                       "rule_id": row["rule_id"], "portfolio_id": row["portfolio_id"],
                       "title": row["title"], "check": json.loads(row["result"])}
        status, error = "sent", ""
        try:
            if row["channel"] == "email":
                _email_delivery(payload)
            else:
                _webhook_delivery(payload)
        except _Unconfigured as exc:
            status, error = "unconfigured", str(exc)
            LOG.warning("Alert delivery %s unconfigured: %s", ident, error)
        except Exception as exc:  # noqa: BLE001 - supervise each provider; persist visible failure and continue
            status, error = "error", f"{type(exc).__name__}: delivery failed"
            # Do not log exception text: providers can include credentials or destinations.
            LOG.error("Alert delivery %s failed (%s)", ident, type(exc).__name__)
        with store.connect() as con:
            con.execute("UPDATE company_deliveries SET status=?,error=?,updated_at=? WHERE id=?",
                        (status, error, _now(), ident))
            _audit(store, con, actor, "delivery.complete", str(ident), status)
        completed += 1
    return {"processed": completed}
