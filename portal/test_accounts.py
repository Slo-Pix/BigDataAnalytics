"""Identity, role, credential and backup regression tests."""

import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

from portal.accounts import AccountStore, Actor, digest, password_matches, require_roles
from portal.manage import backup, restore, validate


class AccountTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "company.sqlite3"
        self.store = AccountStore(self.path)

    def user(self, email="owner@example.test"):
        return self.store.register("Example Studio", "Owner", email, "correct horse battery staple")

    def request(self, token="", bearer=""):
        return SimpleNamespace(cookies={"cwatch_session": token}, headers={"authorization": bearer})

    def test_password_and_session_tokens_are_not_stored_in_plaintext(self):
        actor = self.user()
        token = self.store.session(actor.user_id)
        with self.store.connect() as con:
            user = con.execute("SELECT * FROM users").fetchone()
            session = con.execute("SELECT * FROM sessions").fetchone()
        self.assertNotEqual(user["password_hash"], "correct horse battery staple")
        self.assertTrue(password_matches("correct horse battery staple", user["password_hash"]))
        self.assertFalse(password_matches("wrong password", user["password_hash"]))
        self.assertEqual(session["token_hash"], digest(token))
        self.assertEqual(self.store.actor(self.request(token)).org_id, actor.org_id)

    def test_expired_session_is_rejected(self):
        token = self.store.session(self.user().user_id)
        with self.store.connect() as con:
            con.execute("UPDATE sessions SET expires_at=?", (time.time() - 1,))
        with self.assertRaises(HTTPException) as raised:
            self.store.actor(self.request(token))
        self.assertEqual(raised.exception.status_code, 401)

    def test_invite_is_single_use_and_stays_in_its_company(self):
        owner = self.user()
        with self.store.connect() as con:
            con.execute("INSERT INTO invitations(org_id,token_hash,role,expires_at) VALUES (?,?,?,?)",
                        (owner.org_id, digest("invite-token"), "viewer", time.time() + 100))
        viewer = self.store.register("", "Viewer", "viewer@example.test", "viewer password long", "invite-token")
        self.assertEqual(viewer.org_id, owner.org_id)
        self.assertEqual(viewer.role, "viewer")
        with self.assertRaises(HTTPException):
            self.store.register("", "Other", "other@example.test", "another password long", "invite-token")

    def test_company_registration_is_separate_and_email_is_unique(self):
        first = self.user()
        second = self.user("second@example.test")
        self.assertNotEqual(first.org_id, second.org_id)
        with self.assertRaises(HTTPException):
            self.user()
        with self.store.connect() as con:
            self.assertEqual(con.execute("SELECT count(*) FROM organisations").fetchone()[0], 2)

    def test_read_only_api_key_cannot_inherit_admin_mutations(self):
        admin = self.user()
        with self.store.connect() as con:
            con.execute("""INSERT INTO api_keys(org_id,user_id,name,token_hash,created_at)
                VALUES (?,?,?,?,?)""", (admin.org_id, admin.user_id, "Test", digest("read-token"), "now"))
        actor = self.store.actor(self.request(bearer="Bearer read-token"))
        self.assertEqual(actor.role, "viewer")
        with self.assertRaises(HTTPException):
            require_roles(actor, "admin")
        with self.store.connect() as con:
            con.execute("UPDATE api_keys SET revoked=1")
        with self.assertRaises(HTTPException):
            self.store.actor(self.request(bearer="Bearer read-token"))

    def test_roles_and_rate_limit_are_enforced(self):
        with self.assertRaises(HTTPException):
            require_roles(Actor(1, 1, "viewer", "Viewer", "v@example.test"), "admin", "analyst")
        self.store.limit("bounded", 1)
        with self.assertRaises(HTTPException) as raised:
            self.store.limit("bounded", 1)
        self.assertEqual(raised.exception.status_code, 429)

    def test_backup_is_valid_and_does_not_overwrite_existing_files(self):
        self.user()
        destination = Path(self.temp.name) / "backup.sqlite3"
        backup(self.path, destination)
        validate(destination)
        with self.assertRaises(ValueError):
            backup(self.path, destination)
        self.assertEqual(destination.stat().st_mode & 0o777, 0o600)

    def test_restore_invalidates_sessions_keys_and_invitations(self):
        actor = self.user()
        self.store.session(actor.user_id)
        with self.store.connect() as con:
            con.execute("INSERT INTO invitations(org_id,token_hash,role,expires_at) VALUES (?,?,?,?)",
                        (actor.org_id, digest("invite"), "viewer", time.time() + 100))
            con.execute("""INSERT INTO api_keys(org_id,user_id,name,token_hash,created_at)
                VALUES (?,?,?,?,?)""", (actor.org_id, actor.user_id, "Test", digest("token"), "now"))
        target = Path(self.temp.name) / "restored.sqlite3"
        with patch("portal.manage.socket.socket") as connection:
            connection.return_value.__enter__.return_value.connect_ex.return_value = 1
            restore(self.path, target)
        restored = AccountStore(target)
        with restored.connect() as con:
            self.assertEqual(con.execute("SELECT count(*) FROM sessions").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT revoked FROM api_keys").fetchone()[0], 1)
            self.assertEqual(con.execute("SELECT used FROM invitations").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
