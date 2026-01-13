# PURPOSE: Validate invariant that forbidden Logs directories are rejected.
# INPUTS: Temporary case directory with a forbidden applog/Logs folder.
# OUTPUTS: RuntimeError is raised with offending paths.
from __future__ import annotations

from pathlib import Path
import pytest

from src.paths import assert_no_forbidden_log_dirs, delete_forbidden_log_dirs


def test_assert_no_tdc_applog_logs_raises(tmp_path: Path) -> None:
    case_dir = tmp_path / "CASE"
    logs_dir = case_dir / "TDC Sessions" / "applog" / "Logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(RuntimeError):
        assert_no_forbidden_log_dirs(case_dir)


def test_assert_no_logs_suffix_raises(tmp_path: Path) -> None:
    case_dir = tmp_path / "CASE"
    logs_dir = case_dir / "Misc" / "Logs__1"
    logs_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(RuntimeError):
        assert_no_forbidden_log_dirs(case_dir)


def test_cleanup_applog_dirs_and_invariant(tmp_path: Path) -> None:
    case_dir = tmp_path / "CASE"
    applog_dir = case_dir / "TDC Sessions" / "applog" / "Logs"
    applog_dir.mkdir(parents=True, exist_ok=True)
    (applog_dir / "dummy.txt").write_text("x", encoding="utf-8")

    removed = delete_forbidden_log_dirs(case_dir)
    assert removed
    assert not (case_dir / "TDC Sessions" / "applog").exists()
    assert_no_forbidden_log_dirs(case_dir)


def test_delete_logs_suffix_dir(tmp_path: Path) -> None:
    case_dir = tmp_path / "CASE"
    logs_dir = case_dir / "Misc" / "Logs__1"
    logs_dir.mkdir(parents=True, exist_ok=True)
    (logs_dir / "dummy.txt").write_text("x", encoding="utf-8")

    removed = delete_forbidden_log_dirs(case_dir)
    assert logs_dir in removed
    assert not logs_dir.exists()
    assert_no_forbidden_log_dirs(case_dir)
