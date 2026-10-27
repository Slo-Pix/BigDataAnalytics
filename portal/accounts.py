"""Local-pilot company identities, sessions, permissions and audit storage."""

import hashlib
import hmac
import re
import secrets
import sqlite3
import threading
import time
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

SESSION_COOKIE = "cwatch_session"
SESSION_SECONDS = 8 * 60 * 60


def now() -> str:
    return datetime.now(UTC).isoformat()


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def password_hash(password: str) -> str:
    salt = secrets.token_hex(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 600_000).hex()
    return f"{salt}:{derived}"


def password_matches(password: str, stored: str) -> bool:
    salt, expected = stored.split(":", 1)
    actual = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 600_000).hex()
    return hmac.compare_digest(actual, expected)


DUMMY_PASSWORD_HASH = password_hash("invalid-account-placeholder")


@dataclass(frozen=True)
class Actor:
    org_id: int
    user_id: int
    role: str
    name: str
    email: str
    api_key: bool = False


def require_roles(actor: Actor, *roles: str):
    if actor.role not in roles:
        raise HTTPException(403, "Your company role does not permit this action.")


class AccountStore:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self.rate_lock = threading.Lock()
        self.rates: dict[str, deque] = defaultdict(deque)
        with self.connect() as con:
            con.execute("PRAGMA journal_mode=WAL")
            con.executescript("""
                CREATE TABLE IF NOT EXISTS organisations (
                    id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL REFERENCES organisations(id),
                    name TEXT NOT NULL, email TEXT NOT NULL UNIQUE, password_hash TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('admin','analyst','viewer')));
                CREATE TABLE IF NOT EXISTS sessions (
                    token_hash TEXT PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id),
                    expires_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS invitations (
                    id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL REFERENCES organisations(id),
                    token_hash TEXT NOT NULL UNIQUE, role TEXT NOT NULL,
                    expires_at REAL NOT NULL, used INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS api_keys (
                    id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL REFERENCES organisations(id),
                    user_id INTEGER NOT NULL REFERENCES users(id), name TEXT NOT NULL,
                    token_hash TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL,
                    revoked INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
                    action TEXT NOT NULL, entity TEXT NOT NULL, detail TEXT NOT NULL,
                    created_at TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS audit_org ON audit_log(org_id,id);
            """)
            con.execute("DELETE FROM sessions WHERE expires_at < ?", (time.time(),))
        path.chmod(0o600)

    @contextmanager
    def connect(self):
        con = sqlite3.connect(self.path, timeout=10)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys=ON")
        try:
            with con:
                yield con
        finally:
            con.close()

    def audit(self, con, actor: Actor, action: str, entity: str, detail: str = ""):
        con.execute("INSERT INTO audit_log(org_id,user_id,action,entity,detail,created_at) VALUES (?,?,?,?,?,?)",
                    (actor.org_id, actor.user_id, action, entity, detail[:2000], now()))

    def limit(self, key: str, count: int = 120, seconds: int = 60):
        with self.rate_lock:
            current = time.monotonic()
            if len(self.rates) > 4000:
                self.rates = defaultdict(deque, {k: v for k, v in self.rates.items() if v and current - v[-1] < 3600})
                if len(self.rates) > 4000:
                    raise HTTPException(429, "Request capacity reached. Try again later.")
            history = self.rates[key]
            while history and current - history[0] > seconds:
                history.popleft()
            if len(history) >= count:
                raise HTTPException(429, "Too many requests. Try again after the rate-limit window.")
            history.append(current)

    def session(self, user_id: int) -> str:
        token = secrets.token_urlsafe(32)
        with self.connect() as con:
            con.execute("INSERT INTO sessions VALUES (?,?,?)", (digest(token), user_id, time.time() + SESSION_SECONDS))
        return token

    def actor(self, request: Request) -> Actor:
        authorization = request.headers.get("authorization", "")
        token = request.cookies.get(SESSION_COOKIE)
        with self.connect() as con:
            if authorization:
                if not authorization.startswith("Bearer "):
                    raise HTTPException(401, "Use a session cookie or a Bearer API key.")
                key = authorization.removeprefix("Bearer ").strip()
                row = con.execute("""SELECT u.*, k.id AS key_id FROM users u JOIN api_keys k ON k.user_id=u.id
                    WHERE k.token_hash=? AND k.revoked=0 AND k.org_id=u.org_id""", (digest(key),)).fetchone()
                if row:
                    self.limit(f"key:{row['key_id']}")
            else:
                row = con.execute("""SELECT u.* FROM users u JOIN sessions s ON s.user_id=u.id
                    WHERE s.token_hash=? AND s.expires_at>?""", (digest(token or ""), time.time())).fetchone()
        if row is None:
            raise HTTPException(401, "Sign in to your company workspace.")
        return Actor(row["org_id"], row["id"], "viewer" if authorization else row["role"],
                     row["name"], row["email"], bool(authorization))

    def register(self, company: str, name: str, email: str, password: str, invite: str | None = None) -> Actor:
        if not company.strip() and not invite:
            raise HTTPException(400, "A company name is required.")
        email = email.strip().lower()
        if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email):
            raise HTTPException(400, "Enter a valid email address.")
        if len(password) < 12:
            raise HTTPException(400, "Use at least 12 characters for your password.")
        if not name.strip():
            raise HTTPException(400, "A non-empty name is required.")
        encoded = password_hash(password)
        try:
            with self.connect() as con:
                con.execute("BEGIN IMMEDIATE")
                if invite:
                    invitation = con.execute("""SELECT * FROM invitations
                        WHERE token_hash=? AND used=0 AND expires_at>?""", (digest(invite), time.time())).fetchone()
                    if invitation is None:
                        raise HTTPException(400, "Invitation is invalid, expired or already used.")
                    org_id, role = invitation["org_id"], invitation["role"]
                    con.execute("UPDATE invitations SET used=1 WHERE id=?", (invitation["id"],))
                else:
                    org_id = con.execute("INSERT INTO organisations(name,created_at) VALUES (?,?)",
                                         (company.strip(), now())).lastrowid
                    role = "admin"
                user_id = con.execute("""INSERT INTO users(org_id,name,email,password_hash,role)
                    VALUES (?,?,?,?,?)""", (org_id, name.strip(), email, encoded, role)).lastrowid
                actor = Actor(org_id, user_id, role, name.strip(), email)
                self.audit(con, actor, "account.created", "user", role)
            return actor
        except sqlite3.IntegrityError:
            raise HTTPException(409, "That email already has an account. Sign in instead.") from None


def current_actor(request: Request) -> Actor:
    return getattr(request.state, "actor", None) or request.app.state.accounts.actor(request)


class Registration(BaseModel):
    company: str = Field("", max_length=150)
    name: str = Field(min_length=2, max_length=100)
    email: str = Field(min_length=3, max_length=200)
    password: str = Field(min_length=12, max_length=256)
    invite: str | None = Field(None, max_length=100)


class Login(BaseModel):
    email: str = Field(max_length=200)
    password: str = Field(max_length=256)


class Invitation(BaseModel):
    role: Literal["admin", "analyst", "viewer"]


class KeyRequest(BaseModel):
    name: str = Field(min_length=2, max_length=80)


def accounts_router(store: AccountStore):
    router = APIRouter(prefix="/api/auth")

    def set_session(response: Response, request: Request, user_id: int):
        response.set_cookie(SESSION_COOKIE, store.session(user_id), httponly=True,
                            samesite="strict", secure=request.url.scheme == "https",
                            max_age=SESSION_SECONDS, path="/")

    @router.post("/register", status_code=201)
    def register(body: Registration, request: Request, response: Response):
        store.limit(f"register:{request.client.host}", 10, 3600)
        actor = store.register(body.company, body.name, body.email, body.password, body.invite)
        set_session(response, request, actor.user_id)
        return asdict(actor)

    @router.post("/login")
    def login(body: Login, request: Request, response: Response):
        store.limit(f"login:{request.client.host}", 20, 60)
        with store.connect() as con:
            row = con.execute("SELECT * FROM users WHERE email=?", (body.email.strip().lower(),)).fetchone()
        stored = row["password_hash"] if row else DUMMY_PASSWORD_HASH
        if not password_matches(body.password, stored) or row is None:
            raise HTTPException(401, "Email or password is incorrect.")
        actor = Actor(row["org_id"], row["id"], row["role"], row["name"], row["email"])
        with store.connect() as con:
            store.audit(con, actor, "account.login", "user")
        set_session(response, request, actor.user_id)
        return asdict(actor)

    @router.get("/me")
    def me(request: Request):
        actor = current_actor(request)
        with store.connect() as con:
            company = con.execute("SELECT name FROM organisations WHERE id=?", (actor.org_id,)).fetchone()["name"]
        return asdict(actor) | {"company": company}

    @router.post("/logout")
    def logout(request: Request, response: Response):
        actor = current_actor(request)
        with store.connect() as con:
            con.execute("DELETE FROM sessions WHERE token_hash=?", (digest(request.cookies.get(SESSION_COOKIE, "")),))
            store.audit(con, actor, "account.logout", "user")
        response.delete_cookie(SESSION_COOKIE)
        return {"logged_out": True}

    @router.get("/members")
    def members(request: Request):
        actor = current_actor(request)
        with store.connect() as con:
            return {"rows": [dict(row) for row in con.execute(
                "SELECT id,name,email,role FROM users WHERE org_id=? ORDER BY id", (actor.org_id,))]}

    @router.post("/invitations", status_code=201)
    def invite(body: Invitation, request: Request):
        actor = current_actor(request)
        require_roles(actor, "admin")
        token = secrets.token_urlsafe(32)
        with store.connect() as con:
            con.execute("INSERT INTO invitations(org_id,token_hash,role,expires_at) VALUES (?,?,?,?)",
                        (actor.org_id, digest(token), body.role, time.time() + 86400))
            store.audit(con, actor, "member.invited", "invitation", body.role)
        return {"invite": token, "role": body.role, "expires_in_hours": 24}

    @router.patch("/members/{user_id}")
    def change_role(user_id: int, body: Invitation, request: Request):
        actor = current_actor(request)
        require_roles(actor, "admin")
        with store.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            member = con.execute("SELECT * FROM users WHERE id=? AND org_id=?", (user_id, actor.org_id)).fetchone()
            if member is None:
                raise HTTPException(404, "Company member not found.")
            admins = con.execute("SELECT count(*) FROM users WHERE org_id=? AND role='admin'", (actor.org_id,)).fetchone()[0]
            if member["role"] == "admin" and body.role != "admin" and admins <= 1:
                raise HTTPException(409, "The company must retain at least one administrator.")
            con.execute("UPDATE users SET role=? WHERE id=? AND org_id=?", (body.role, user_id, actor.org_id))
            store.audit(con, actor, "member.role_changed", str(user_id), body.role)
        return {"updated": True}

    @router.get("/keys")
    def keys(request: Request):
        actor = current_actor(request)
        require_roles(actor, "admin")
        with store.connect() as con:
            return {"rows": [dict(row) for row in con.execute(
                "SELECT id,name,created_at,revoked FROM api_keys WHERE org_id=?", (actor.org_id,))]}

    @router.post("/keys", status_code=201)
    def create_key(body: KeyRequest, request: Request):
        actor = current_actor(request)
        require_roles(actor, "admin")
        token = "cw_" + secrets.token_urlsafe(32)
        with store.connect() as con:
            key_id = con.execute("""INSERT INTO api_keys(org_id,user_id,name,token_hash,created_at)
                VALUES (?,?,?,?,?)""", (actor.org_id, actor.user_id, body.name, digest(token), now())).lastrowid
            store.audit(con, actor, "api_key.created", str(key_id), body.name)
        return {"id": key_id, "token": token, "scope": "read-only", "requests_per_minute": 120}

    @router.delete("/keys/{key_id}")
    def revoke_key(key_id: int, request: Request):
        actor = current_actor(request)
        require_roles(actor, "admin")
        with store.connect() as con:
            if not con.execute("UPDATE api_keys SET revoked=1 WHERE id=? AND org_id=?", (key_id, actor.org_id)).rowcount:
                raise HTTPException(404, "API key not found.")
            store.audit(con, actor, "api_key.revoked", str(key_id))
        return {"revoked": True}

    return router
