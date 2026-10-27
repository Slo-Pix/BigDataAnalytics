"""Explicit local-pilot backup/restore; never deletes research or source datasets."""

import argparse
import os
import socket
import sqlite3
from pathlib import Path

from common.paths import ROOT


def validate(path: Path):
    if not path.is_file():
        raise ValueError(f"Database does not exist: {path}")
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as con:
        if con.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ValueError("Database integrity check failed.")
        tables = {row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"users", "organisations", "sessions", "invitations", "api_keys", "audit_log"}.issubset(tables):
            raise ValueError("This is not a ExposureIQ company database.")


def backup(source: Path, destination: Path):
    validate(source)
    if destination.exists():
        raise ValueError("Destination already exists. Choose a new backup filename.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source) as source_con, sqlite3.connect(destination) as target:
        source_con.backup(target)
    destination.chmod(0o600)
    validate(destination)


def restore(source: Path, destination: Path):
    validate(source)
    with socket.socket() as connection:
        connection.settimeout(0.5)
        if connection.connect_ex(("127.0.0.1", int(os.environ.get("PORTAL_PORT", "8800")))) == 0:
            raise ValueError("Stop the portal server before restoring its company database.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source) as source_con, sqlite3.connect(destination) as target:
        source_con.backup(target)
        target.execute("DELETE FROM sessions")
        target.execute("UPDATE api_keys SET revoked=1")
        target.execute("UPDATE invitations SET used=1")
    destination.chmod(0o600)
    validate(destination)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("backup", "restore"))
    parser.add_argument("--file", type=Path, required=True, help="Backup output or restore input.")
    parser.add_argument("--database", type=Path, default=Path(os.environ.get("CWATCH_APP_DB", ROOT / "dataset" / "app" / "cwatch.sqlite3")))
    args = parser.parse_args()
    try:
        if args.action == "backup":
            backup(args.database, args.file)
        else:
            restore(args.file, args.database)
    except (ValueError, sqlite3.Error, OSError) as error:
        parser.exit(1, f"{args.action} failed: {error}\n")
    print(f"{args.action} completed: {args.file}")


if __name__ == "__main__":
    main()
