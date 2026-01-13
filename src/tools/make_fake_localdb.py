#!/usr/bin/env python3
"""
Create a deterministic fake local.db for PHI testing.
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from src.tools.sqlite_artifact_cleanup import cleanup_sqlite_sidecars


def create_fake_localdb(db_path: Path, case_id: str) -> None:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    cleanup_sqlite_sidecars(db_path)

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=DELETE;")
        conn.executescript(
            """
            DROP TABLE IF EXISTS SessionInformationChangeRecord;
            DROP TABLE IF EXISTS Sessions;

            CREATE TABLE SessionInformationChangeRecord (
                Id INTEGER PRIMARY KEY,
                PatientFirstName TEXT,
                PatientLastName TEXT,
                PatientId TEXT,
                ChangeTimestamp TEXT
            );

            CREATE TABLE Sessions (
                SessionId INTEGER PRIMARY KEY,
                FirstName TEXT,
                LastName TEXT,
                CreatedAt TEXT
            );
            """
        )

        conn.execute(
            """
            INSERT INTO SessionInformationChangeRecord
                (Id, PatientFirstName, PatientLastName, PatientId, ChangeTimestamp)
            VALUES (?, ?, ?, ?, ?);
            """,
            (1, "Nick", "Sisco", "123456789", "2025-11-05 07:05:25"),
        )

        conn.execute(
            """
            INSERT INTO Sessions (SessionId, FirstName, LastName, CreatedAt)
            VALUES (?, ?, ?, ?);
            """,
            (1, "Nick", "Sisco", "2025-11-05 07:05:25"),
        )
        conn.execute(
            """
            INSERT INTO Sessions (SessionId, FirstName, LastName, CreatedAt)
            VALUES (?, ?, ?, ?);
            """,
            (2, case_id, case_id, "2025-11-05 07:10:25"),
        )
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Create a deterministic fake local.db for PHI testing.")
    ap.add_argument("--out", required=True, help="Output path for local.db")
    ap.add_argument("--case-id", default="093_01-098", help="Case ID for clean rows")
    args = ap.parse_args()

    create_fake_localdb(Path(args.out), args.case_id)
    cleanup_sqlite_sidecars(Path(args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
