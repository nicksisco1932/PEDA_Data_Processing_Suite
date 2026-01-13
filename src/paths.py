# PURPOSE: Canonical path policy helpers for logs and TDC log placement.
# INPUTS: Case directory, case ID, run ID, and TDC date token.
# OUTPUTS: Deterministic Path objects for pipeline artifacts and log invariants.
# NOTES: Callers are responsible for creating directories.
from __future__ import annotations

import logging
import os
import shutil
import stat
from pathlib import Path
from typing import List, Set


def run_logs_dir(case_dir: Path) -> Path:
    return Path(case_dir) / "run_logs"


def case_run_logs_dir(case_dir: Path) -> Path:
    return run_logs_dir(case_dir)


def misc_logs_dir(case_dir: Path) -> Path:
    return Path(case_dir) / "Misc" / "Logs"


def run_manifest_path(case_dir: Path, case_id: str, run_id: str) -> Path:
    return case_run_logs_dir(case_dir) / f"{case_id}__{run_id}__manifest.json"


def run_log_path(case_dir: Path, case_id: str, run_id: str) -> Path:
    return case_run_logs_dir(case_dir) / f"{case_id}__{run_id}.log"


def tdc_log_path(case_dir: Path, case_id: str, tdc_date_token: str) -> Path:
    return misc_logs_dir(case_dir) / f"{case_id} Tdc.{tdc_date_token}.log"


def _collect_forbidden_log_dirs(case_dir: Path) -> List[Path]:
    case_dir = Path(case_dir)
    offenders: Set[Path] = set()

    for path in case_dir.rglob("*"):
        if not path.is_dir():
            continue
        name_lower = path.name.lower()
        if name_lower.startswith("logs__"):
            offenders.add(path)
            continue
    tdc_root = case_dir / "TDC Sessions"
    if tdc_root.exists():
        for path in tdc_root.rglob("*"):
            if not path.is_dir():
                continue
            if path.name.lower() == "applog":
                offenders.add(path)
                continue
            if path.name.lower() == "logs" and path.parent.name.lower() == "applog":
                offenders.add(path)

    misc_logs_glob = list((case_dir / "Misc").glob("Logs__*"))
    offenders.update([p for p in misc_logs_glob if p.is_dir()])

    return sorted(offenders, key=lambda p: str(p).lower())


def assert_no_forbidden_log_dirs(case_dir: Path) -> None:
    offenders = _collect_forbidden_log_dirs(case_dir)
    if offenders:
        msg = "Forbidden Logs directories found: " + ", ".join(str(p) for p in offenders)
        raise RuntimeError(msg)


def assert_no_extra_logs_dirs(case_dir: Path) -> None:
    assert_no_forbidden_log_dirs(case_dir)


def _rmtree_with_retry(path: Path) -> None:
    def _onerror(func, p, exc_info) -> None:
        try:
            os.chmod(p, stat.S_IWRITE)
            func(p)
        except Exception:
            raise

    shutil.rmtree(path, onerror=_onerror)


def delete_forbidden_log_dirs(
    case_dir: Path, logger: logging.Logger | None = None
) -> List[Path]:
    offenders = _collect_forbidden_log_dirs(case_dir)
    offenders = sorted(offenders, key=lambda p: len(p.parts), reverse=True)
    removed: List[Path] = []
    for path in offenders:
        if not path.exists():
            continue
        try:
            _rmtree_with_retry(path)
        except Exception as exc:
            raise RuntimeError(f"Failed to delete forbidden log dir: {path}") from exc
        if path.exists():
            raise RuntimeError(f"Failed to delete forbidden log dir: {path}")
        removed.append(path)
        if logger:
            logger.warning("Deleted forbidden log dir: %s", path)
    return removed


def cleanup_tdc_applog_dirs(case_dir: Path, logger: logging.Logger | None = None) -> List[Path]:
    case_dir = Path(case_dir)
    tdc_root = case_dir / "TDC Sessions"
    if not tdc_root.exists():
        return []

    removed = []
    for path in delete_forbidden_log_dirs(case_dir, logger=logger):
        if path.name.lower() == "applog":
            removed.append(path)
    return removed
