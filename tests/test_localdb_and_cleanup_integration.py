# PURPOSE: Integration-style test for localdb step and cleanup artifacts.
# INPUTS: Fake local.db with PHI in a temp working directory.
# OUTPUTS: localdb check/anon summary and cleanup deletion.
from __future__ import annotations

from pathlib import Path

from src.pipeline_steps.cleanup_artifacts import cleanup_artifacts, DEFAULT_PATTERNS
from src.pipeline_steps.localdb_step import run_localdb_step
from src.tools.make_fake_localdb import create_fake_localdb


def test_localdb_step_and_cleanup(tmp_path: Path) -> None:
    case_id = "093_01-098"
    work = tmp_path / "work"
    db_path = work / "local.db"
    create_fake_localdb(db_path, case_id)

    out_dir = tmp_path / "reports"
    summary = run_localdb_step(
        db_path=db_path,
        case_id=case_id,
        out_dir=out_dir,
        enable_anon=True,
        check_only=False,
        strict=True,
    )

    assert summary["post"]["fails"] == 0

    wal = db_path.with_name(db_path.name + "-wal")
    shm = db_path.with_name(db_path.name + "-shm")
    wal.write_text("wal", encoding="utf-8")
    shm.write_text("shm", encoding="utf-8")

    cleanup = cleanup_artifacts(work, DEFAULT_PATTERNS, dry_run=False)
    assert cleanup["status"] == "ok"
    assert not wal.exists()
    assert not shm.exists()
