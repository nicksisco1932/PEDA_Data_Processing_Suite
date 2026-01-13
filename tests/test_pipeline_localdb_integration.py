from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from src.tools.make_dummy_case_tree import make_dummy_case_tree
from src.tools.sqlite_artifact_cleanup import cleanup_sqlite_sidecars

ROOT = Path(__file__).resolve().parents[1]


def _run_controller(args: list[str]) -> subprocess.CompletedProcess[str]:
    cmd = [sys.executable, str(ROOT / "src" / "controller.py"), *args]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(ROOT))


def _load_summary(report_path: Path) -> dict:
    with report_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("summary", {})


def test_pipeline_localdb_success() -> None:
    case_id = "093_01-098"
    session_name = "_2025-11-05--07-05-25 122867438"
    with TemporaryDirectory() as tmp:
        paths = make_dummy_case_tree(Path(tmp), case_id, session_name)
        report_dir = paths.case_dir / "TDC Sessions" / "applog" / "Logs"
        report_path = report_dir / "localdb_check_post.json"

        try:
            result = _run_controller(
                [
                    "--root",
                    str(paths.root),
                    "--case",
                    case_id,
                    "--mri-input",
                    str(paths.root / "dummy_mri.zip"),
                    "--tdc-input",
                    str(paths.root / "dummy_tdc.zip"),
                    "--skip-mri",
                    "--skip-tdc",
                    "--localdb-enabled",
                    "--localdb-path",
                    str(paths.db_path),
                ]
            )
            assert result.returncode == 0
            assert report_path.exists()
            summary = _load_summary(report_path)
            assert summary.get("fails", 1) == 0
        finally:
            cleanup_sqlite_sidecars(paths.db_path)


def test_pipeline_localdb_check_only_strict_fails() -> None:
    case_id = "093_01-098"
    session_name = "_2025-11-05--07-05-25 122867438"
    with TemporaryDirectory() as tmp:
        paths = make_dummy_case_tree(Path(tmp), case_id, session_name)
        report_dir = paths.case_dir / "TDC Sessions" / "applog" / "Logs"
        report_path = report_dir / "localdb_check_pre.json"

        try:
            result = _run_controller(
                [
                    "--root",
                    str(paths.root),
                    "--case",
                    case_id,
                    "--mri-input",
                    str(paths.root / "dummy_mri.zip"),
                    "--tdc-input",
                    str(paths.root / "dummy_tdc.zip"),
                    "--skip-mri",
                    "--skip-tdc",
                    "--localdb-enabled",
                    "--localdb-check-only",
                    "--localdb-strict",
                    "--localdb-path",
                    str(paths.db_path),
                ]
            )
            assert result.returncode != 0
            assert report_path.exists()
            summary = _load_summary(report_path)
            assert summary.get("fails", 0) >= 1
        finally:
            cleanup_sqlite_sidecars(paths.db_path)


def test_pipeline_localdb_missing_skips() -> None:
    case_id = "093_01-098"
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        case_dir = root / case_id
        (case_dir / "Misc").mkdir(parents=True, exist_ok=True)
        (case_dir / "MR DICOM").mkdir(parents=True, exist_ok=True)
        (case_dir / "TDC Sessions").mkdir(parents=True, exist_ok=True)

        result = _run_controller(
            [
                "--root",
                str(root),
                "--case",
                case_id,
                "--mri-input",
                str(root / "dummy_mri.zip"),
                "--tdc-input",
                str(root / "dummy_tdc.zip"),
                "--skip-mri",
                "--skip-tdc",
                "--localdb-enabled",
            ]
        )
        assert result.returncode == 0
