#!/usr/bin/env python3
"""
Pipeline wrapper for local.db check + anonymization.
"""
from __future__ import annotations

import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Tuple

from src.logutil import ProcessingError
from src.tools.localdb_anon import anonymize_localdb
from src.tools.sqlite_artifact_cleanup import cleanup_sqlite_sidecars


def _run_checker(db_path: Path, case_id: str, json_out: Path) -> Tuple[int, Dict[str, Any]]:
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
                report = json.load(f)
            summary.update(report.get("summary", {}))
        except Exception:
            pass
    return result.returncode, summary


def run_localdb_step(
    db_path: Path,
    case_id: str,
    out_dir: Path,
    enable_anon: bool,
    check_only: bool,
    strict: bool,
) -> Dict[str, Any]:
    log = logging.getLogger(__name__)
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = Path(db_path)
    summary: Dict[str, Any] = {
        "db": str(db_path),
        "pre": {},
        "post": {},
        "anon_applied": False,
    }

    pre_json = out_dir / "localdb_check_pre.json"
    post_json = out_dir / "localdb_check_post.json"

    pre_code = 0
    pre_summary: Dict[str, Any] = {}
    post_code = 0
    post_summary: Dict[str, Any] = {}
    anon_failed = False

    try:
        pre_code, pre_summary = _run_checker(db_path, case_id, pre_json)
        summary["pre"] = {"exit_code": pre_code, **pre_summary}
    finally:
        cleanup_sqlite_sidecars(db_path)

    if enable_anon and summary["pre"].get("fails", 0) > 0:
        anon_result = anonymize_localdb(db_path, case_id)
        summary["anon_applied"] = anon_result.get("ok", False)
        summary["anon_result"] = anon_result
        if not anon_result.get("ok", False):
            anon_failed = True
            log.warning("localdb anonymization failed: %s", anon_result)

    try:
        post_code, post_summary = _run_checker(db_path, case_id, post_json)
        summary["post"] = {"exit_code": post_code, **post_summary}
    finally:
        cleanup_sqlite_sidecars(db_path)

    if check_only and summary["pre"].get("fails", 0) > 0:
        msg = f"localdb pre-check failed (check_only): {db_path}"
        if strict:
            raise ProcessingError(msg)
        log.warning(msg)

    if anon_failed and strict:
        raise ProcessingError(f"localdb anonymization failed: {db_path}")

    if summary["post"].get("fails", 0) > 0 and strict:
        raise ProcessingError(f"localdb post-check failed: {db_path}")

    return summary
