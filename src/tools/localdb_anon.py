#!/usr/bin/env python3
"""
Minimal local.db anonymizer for pipeline checks.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List

from src.tools.sqlite_artifact_cleanup import cleanup_sqlite_sidecars


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,)
    )
    row = cur.fetchone()
    cur.close()
    return row is not None


def _column_names(conn: sqlite3.Connection, table: str) -> List[str]:
    safe_table = table.replace('"', '""')
    cur = conn.execute(f'PRAGMA table_info("{safe_table}");')
    cols = [row[1] for row in cur.fetchall()]
    cur.close()
    return cols


def anonymize_localdb(db_path: Path, case_id: str) -> Dict[str, Any]:
    """
    Update known PHI fields to case_id. Returns a summary dict.
    """
    db_path = Path(db_path)
    result: Dict[str, Any] = {"ok": False, "db": str(db_path), "counts": {}}
    missing: Dict[str, List[str]] = {}

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=DELETE;")
        conn.execute("BEGIN;")

        tables = {
            "SessionInformationChangeRecord": [
                "PatientFirstName",
                "PatientLastName",
                "PatientId",
            ],
            "Sessions": ["FirstName", "LastName"],
        }

        for table, cols in tables.items():
            if not _table_exists(conn, table):
                missing[table] = cols
                continue
            existing = set(_column_names(conn, table))
            missing_cols = [c for c in cols if c not in existing]
            if missing_cols:
                missing[table] = missing_cols
                continue

            updates = ", ".join([f'{c}=?' for c in cols])
            cur = conn.execute(
                f'UPDATE "{table}" SET {updates};',
                tuple([case_id] * len(cols)),
            )
            result["counts"][table] = {
                "updated_rows": cur.rowcount,
                "columns": {c: cur.rowcount for c in cols},
            }

        if missing:
            conn.rollback()
            result["error"] = "Missing required tables/columns"
            result["missing"] = missing
            return result

        conn.commit()
        result["ok"] = True
        return result
    finally:
        conn.close()
        cleanup_sqlite_sidecars(db_path)
