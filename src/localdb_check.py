#!/usr/bin/env python3
"""
localdb_check.py

Read-only checker for Profound/TDC local.db (SQLite).
- Verifies tables/columns exist ("reachability")
- Checks anonymization targets in:
    * SessionInformationChangeRecord: PatientFirstName/PatientLastName/PatientId (and variants)
    * Sessions: FirstName/LastName should equal <case-id> (configurable)
- Flags suspicious patient-identifying strings (non-null and not expected anon value)

Usage:
  python localdb_check.py --db "E:\case\local.db" --case-id "093_01-098"
  python localdb_check.py --db "E:\case\local.db" --case-id "093_01-098" --json-out report.json

Exit codes:
  0 = pass (no findings)
  2 = findings present (needs attention)
  3 = database/schema error (can't open or missing tables/columns)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple


# -----------------------------
# Helpers
# -----------------------------

def connect_sqlite(db_path: str) -> sqlite3.Connection:
    # URI read-only if possible
    if os.path.exists(db_path):
        uri = f"file:{db_path}?mode=ro"
        return sqlite3.connect(uri, uri=True)
    # Fallback: will raise consistent error
    return sqlite3.connect(db_path)

def fetch_all(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return rows

def list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = fetch_all(conn, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
    return [r["name"] for r in rows]

def list_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = fetch_all(conn, f"PRAGMA table_info({quote_ident(table)});")
    return [r["name"] for r in rows]

def quote_ident(name: str) -> str:
    # SQLite identifier quoting
    return '"' + name.replace('"', '""') + '"'

def safe_select_some(conn: sqlite3.Connection, table: str, columns: List[str], limit: int = 50) -> List[sqlite3.Row]:
    col_sql = ", ".join(quote_ident(c) for c in columns)
    sql = f"SELECT {col_sql} FROM {quote_ident(table)} LIMIT {int(limit)};"
    return fetch_all(conn, sql)

def safe_select_nonnull_distinct(conn: sqlite3.Connection, table: str, column: str, limit: int = 200) -> List[Any]:
    sql = (
        f"SELECT DISTINCT {quote_ident(column)} AS v "
        f"FROM {quote_ident(table)} "
        f"WHERE {quote_ident(column)} IS NOT NULL "
        f"LIMIT {int(limit)};"
    )
    rows = fetch_all(conn, sql)
    return [r["v"] for r in rows]


# -----------------------------
# Finding model
# -----------------------------

@dataclass
class Finding:
    severity: str          # "INFO" | "WARN" | "FAIL"
    category: str          # "SCHEMA" | "ANON" | "SUSPICIOUS"
    table: Optional[str]
    column: Optional[str]
    message: str
    examples: Optional[List[str]] = None


# -----------------------------
# Suspicion heuristics (simple, explainable)
# -----------------------------

NAME_LIKE = re.compile(r"^[A-Z][a-z]+(?:[-'][A-Z][a-z]+)?$")          # "Smith", "O'Neil", "Anne-Marie"
HAS_SPACE = re.compile(r"\s+")
HAS_COMMA = re.compile(r",")
ALNUM_LONG = re.compile(r"^[A-Za-z0-9]{8,}$")                        # long token-ish (IDs)
DIGITS_LONG = re.compile(r"^\d{6,}$")                                # MRN-ish
EMAIL_LIKE = re.compile(r".+@.+\..+")
DATE_LIKE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def is_suspicious_value(v: Any, expected: Optional[str]) -> Tuple[bool, str]:
    """
    Returns (suspicious?, reason).
    expected: value that is considered "safe" (e.g., case-id), if provided.
    """
    if v is None:
        return (False, "null")
    s = str(v).strip()
    if s == "":
        return (False, "empty")
    if expected is not None and s == expected:
        return (False, "matches_expected")

    # Obvious not-PII tokens (common anonymization placeholders)
    if s.lower() in {"anon", "anonymous", "redacted", "unknown", "patient"}:
        return (False, "placeholder")

    # Heuristics that suggest "real" content (not definitive)
    if EMAIL_LIKE.match(s):
        return (True, "email_like")
    if NAME_LIKE.match(s):
        return (True, "name_like_single_token")
    if HAS_SPACE.search(s) or HAS_COMMA.search(s):
        return (True, "contains_whitespace_or_comma")
    if DATE_LIKE.match(s):
        return (True, "date_like")
    if DIGITS_LONG.match(s):
        return (True, "long_numeric_id_like")
    if ALNUM_LONG.match(s):
        return (True, "long_alphanumeric_id_like")

    # Default: if not expected and non-empty, treat as mildly suspicious
    return (True, "nonempty_not_expected")


# -----------------------------
# Main checks
# -----------------------------

def find_matching_columns(columns: List[str], wanted: List[str]) -> List[str]:
    """
    Case-insensitive exact-name match against 'wanted' list.
    """
    lower_map = {c.lower(): c for c in columns}
    hits = []
    for w in wanted:
        if w.lower() in lower_map:
            hits.append(lower_map[w.lower()])
    return hits

def check_schema_reachability(conn: sqlite3.Connection, required_tables: List[str]) -> List[Finding]:
    findings: List[Finding] = []
    tables = set(list_tables(conn))
    for t in required_tables:
        if t not in tables:
            findings.append(Finding(
                severity="FAIL",
                category="SCHEMA",
                table=t,
                column=None,
                message=f"Missing required table: {t}"
            ))
        else:
            findings.append(Finding(
                severity="INFO",
                category="SCHEMA",
                table=t,
                column=None,
                message=f"Found table: {t}"
            ))
    return findings

def check_sessioninfo_change_record(conn: sqlite3.Connection, case_id: str) -> List[Finding]:
    findings: List[Finding] = []
    table = "SessionInformationChangeRecord"
    tables = set(list_tables(conn))
    if table not in tables:
        findings.append(Finding("FAIL", "SCHEMA", table, None, "Table not present; cannot check Patient* fields"))
        return findings

    cols = list_columns(conn, table)

    # Your target patterns, plus common variants/typos
    wanted = [
        "PatientFirstName", "PatienFirstName",
        "PatientLastName",
        "PatientId", "PatientID", "PatientIdentifier"
    ]
    hits = find_matching_columns(cols, wanted)
    if not hits:
        findings.append(Finding(
            severity="FAIL",
            category="SCHEMA",
            table=table,
            column=None,
            message="None of the expected patient-identifying columns were found (PatientFirstName/PatientLastName/PatientId variants)."
        ))
        return findings

    findings.append(Finding(
        severity="INFO",
        category="SCHEMA",
        table=table,
        column=None,
        message=f"Matched columns: {', '.join(hits)}"
    ))

    # For each hit column, look for distinct non-null values and flag those not equal to case-id
    for c in hits:
        vals = safe_select_nonnull_distinct(conn, table, c, limit=200)
        bad_examples: List[str] = []
        reasons: Dict[str, int] = {}
        for v in vals:
            suspicious, reason = is_suspicious_value(v, expected=case_id)
            if suspicious:
                reasons[reason] = reasons.get(reason, 0) + 1
                if len(bad_examples) < 10:
                    bad_examples.append(str(v))

        if bad_examples:
            findings.append(Finding(
                severity="FAIL",
                category="ANON",
                table=table,
                column=c,
                message=f"Non-null values found that do not match expected case-id '{case_id}'. Reasons: {reasons}",
                examples=bad_examples
            ))
        else:
            findings.append(Finding(
                severity="INFO",
                category="ANON",
                table=table,
                column=c,
                message=f"Column appears clean vs expected '{case_id}' (no suspicious non-null distinct values in sample)."
            ))

    return findings

def check_sessions_table(conn: sqlite3.Connection, case_id: str, require_equal_caseid: bool = True) -> List[Finding]:
    findings: List[Finding] = []
    table = "Sessions"
    tables = set(list_tables(conn))
    if table not in tables:
        findings.append(Finding("FAIL", "SCHEMA", table, None, "Table not present; cannot check Sessions.FirstName/LastName"))
        return findings

    cols = list_columns(conn, table)
    hits = find_matching_columns(cols, ["FirstName", "LastName"])
    if len(hits) < 2:
        findings.append(Finding(
            severity="FAIL",
            category="SCHEMA",
            table=table,
            column=None,
            message=f"Expected columns FirstName and LastName not both present. Found: {hits}"
        ))
        return findings

    # Pull distinct values
    for c in hits:
        vals = safe_select_nonnull_distinct(conn, table, c, limit=200)
        bad_examples: List[str] = []
        reasons: Dict[str, int] = {}
        for v in vals:
            expected = case_id if require_equal_caseid else None
            suspicious, reason = is_suspicious_value(v, expected=expected)
            # If require_equal_caseid=True, any non-null != case_id is suspicious by definition
            if require_equal_caseid and str(v).strip() != case_id:
                reasons["not_equal_case_id"] = reasons.get("not_equal_case_id", 0) + 1
                if len(bad_examples) < 10:
                    bad_examples.append(str(v))
            elif (not require_equal_caseid) and suspicious:
                reasons[reason] = reasons.get(reason, 0) + 1
                if len(bad_examples) < 10:
                    bad_examples.append(str(v))

        if bad_examples:
            findings.append(Finding(
                severity="FAIL",
                category="ANON",
                table=table,
                column=c,
                message=f"Sessions.{c} contains values not matching expected '{case_id}'. Reasons: {reasons}",
                examples=bad_examples
            ))
        else:
            findings.append(Finding(
                severity="INFO",
                category="ANON",
                table=table,
                column=c,
                message=f"Sessions.{c} appears clean vs expected '{case_id}' (no mismatches in sampled distinct values)."
            ))

    return findings

def main() -> int:
    ap = argparse.ArgumentParser(description="Read-only local.db anonymization checker (SQLite).")
    ap.add_argument("--db", required=True, help="Path to local.db (SQLite)")
    ap.add_argument("--case-id", required=True, help="Case ID expected to appear in anonymized fields (e.g., 093_01-098)")
    ap.add_argument("--json-out", default=None, help="Optional JSON output path")
    ap.add_argument("--no-caseid-enforce-sessions", action="store_true",
                    help="If set, do not require Sessions.FirstName/LastName == case-id; just flag suspicious patterns.")
    args = ap.parse_args()

    report: Dict[str, Any] = {
        "db": args.db,
        "case_id": args.case_id,
        "tables": [],
        "findings": [],
        "summary": {}
    }

    findings: List[Finding] = []

    try:
        conn = connect_sqlite(args.db)
    except Exception as e:
        findings.append(Finding("FAIL", "SCHEMA", None, None, f"Cannot open database: {e}"))
        report["findings"] = [asdict(f) for f in findings]
        _emit(report, args.json_out)
        print("ERROR: cannot open database")
        return 3

    try:
        tables = list_tables(conn)
        report["tables"] = tables

        # Reachability checks (minimal, focused on your targets)
        findings += check_schema_reachability(conn, ["SessionInformationChangeRecord", "Sessions"])

        # Target checks
        findings += check_sessioninfo_change_record(conn, args.case_id)
        findings += check_sessions_table(conn, args.case_id, require_equal_caseid=not args.no_caseid_enforce_sessions)

    except Exception as e:
        findings.append(Finding("FAIL", "SCHEMA", None, None, f"Error while querying database: {e}"))
        report["findings"] = [asdict(f) for f in findings]
        _emit(report, args.json_out)
        print("ERROR: failed during query/inspection")
        return 3
    finally:
        try:
            conn.close()
        except Exception:
            pass

    report["findings"] = [asdict(f) for f in findings]

    # Summary + exit code
    n_fail = sum(1 for f in findings if f.severity == "FAIL")
    n_warn = sum(1 for f in findings if f.severity == "WARN")
    report["summary"] = {
        "fails": n_fail,
        "warns": n_warn,
        "infos": sum(1 for f in findings if f.severity == "INFO"),
    }

    _emit(report, args.json_out)
    _print_human(report)

    if n_fail > 0 or n_warn > 0:
        return 2
    return 0

def _emit(report: Dict[str, Any], json_out: Optional[str]) -> None:
    if json_out:
        with open(json_out, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

def _print_human(report: Dict[str, Any]) -> None:
    print("\n=== local.db CHECK REPORT ===")
    print(f"DB:      {report['db']}")
    print(f"Case ID: {report['case_id']}")
    print(f"Tables:  {len(report.get('tables', []))}")

    print("\n--- Findings ---")
    for f in report.get("findings", []):
        sev = f["severity"]
        cat = f["category"]
        loc = ""
        if f.get("table"):
            loc += f["table"]
        if f.get("column"):
            loc += f".{f['column']}"
        if loc:
            loc = f" [{loc}]"
        print(f"{sev:<4} {cat:<10}{loc} {f['message']}")
        ex = f.get("examples") or []
        if ex:
            print("     Examples:")
            for e in ex:
                print(f"       - {e}")

    s = report.get("summary", {})
    print("\n--- Summary ---")
    print(f"FAIL: {s.get('fails', 0)}  WARN: {s.get('warns', 0)}  INFO: {s.get('infos', 0)}")
    print("============================\n")

if __name__ == "__main__":
    raise SystemExit(main())
