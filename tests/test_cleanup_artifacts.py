# PURPOSE: Tests for guarded artifact cleanup.
# INPUTS: Temporary working directory with junk files.
# OUTPUTS: Deletions under the working directory only.
from __future__ import annotations

from pathlib import Path

from src.pipeline_steps.cleanup_artifacts import cleanup_artifacts, DEFAULT_PATTERNS


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_cleanup_deletes_known_patterns(tmp_path: Path) -> None:
    root = tmp_path / "work"
    keep = root / "keep.txt"
    mat = root / "sub" / "junk.mat"
    wal = root / "local.db-wal"
    shm = root / "sub" / "local.db-shm"
    _write(keep, "keep")
    _write(mat, "mat")
    _write(wal, "wal")
    _write(shm, "shm")

    summary = cleanup_artifacts(root, DEFAULT_PATTERNS, dry_run=False)

    assert summary["status"] == "ok"
    assert keep.exists()
    assert not mat.exists()
    assert not wal.exists()
    assert not shm.exists()


def test_cleanup_refuses_home_dir() -> None:
    summary = cleanup_artifacts(Path.home(), DEFAULT_PATTERNS, dry_run=False)
    assert summary["status"] == "refused"
