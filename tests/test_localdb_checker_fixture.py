from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from src.tools.make_fake_localdb import create_fake_localdb
from src.tools.sqlite_artifact_cleanup import cleanup_sqlite_sidecars


def _run_checker(db_path: Path, case_id: str, json_out: Path) -> tuple[int, dict]:
    cmd = [
        sys.executable,
        "-m",
        "src.localdb_check",
        "--db",
        str(db_path),
        "--case-id",
        case_id,
        "--json-out",
        str(json_out),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    summary = {"fails": -1, "warns": -1, "infos": -1}
    if json_out.exists():
        try:
            with json_out.open("r", encoding="utf-8") as f:
                data = json.load(f)
            summary.update(data.get("summary", {}))
        except Exception:
            pass
    return result.returncode, summary


def _anonymize_minimal(db_path: Path, case_id: str) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            UPDATE SessionInformationChangeRecord
            SET PatientFirstName = ?, PatientLastName = ?, PatientId = ?;
            """,
            (case_id, case_id, case_id),
        )
        conn.execute(
            """
            UPDATE Sessions
            SET FirstName = ?, LastName = ?;
            """,
            (case_id, case_id),
        )
        conn.commit()
    finally:
        conn.close()


def test_localdb_checker_fixture() -> None:
    case_id = "093_01-098"
    with TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "local.db"
        json_before = Path(tmp) / "report_before.json"
        json_after = Path(tmp) / "report_after.json"

        try:
            create_fake_localdb(db_path, case_id)
            cleanup_sqlite_sidecars(db_path)

            code_before, summary_before = _run_checker(db_path, case_id, json_before)
            cleanup_sqlite_sidecars(db_path)

            _anonymize_minimal(db_path, case_id)
            cleanup_sqlite_sidecars(db_path)

            code_after, summary_after = _run_checker(db_path, case_id, json_after)
            cleanup_sqlite_sidecars(db_path)

            print(
                "localdb_check status:",
                f"db={db_path}",
                f"before=exit{code_before} FAIL/WARN/INFO={summary_before}",
                f"after=exit{code_after} FAIL/WARN/INFO={summary_after}",
            )

            assert code_before == 2
            assert summary_before.get("fails", 0) >= 1
            assert code_after == 0
            assert summary_after.get("fails", 1) == 0
        finally:
            # Ensure WAL/SHM files are removed even on assertion failure.
            cleanup_sqlite_sidecars(db_path)
