# PURPOSE: Stubbed PEDA pipeline step with deterministic artifact generation.
# INPUTS: Case ID, working case dir, output root, and PEDA version.
# OUTPUTS: Stub PEDA output tree, video copy without .mat, and data zip.
# NOTES: Does not invoke MATLAB/MAIN_PEDA; captures intended command in stub log.
from __future__ import annotations

import logging
import shutil
import zipfile
from pathlib import Path
from typing import Dict, Any, List

from src.annon_logs import get_annon_logs_dir
from src.pipeline_steps.cleanup_artifacts import cleanup_artifacts
from src.tools.sqlite_artifact_cleanup import cleanup_sqlite_sidecars


def _intended_matlab_command(log_path: Path, input_path: Path) -> str:
    return (
        f'matlab -logfile "{log_path.name}" -batch '
        f"\"cd('<PEDA_PATH>');MAIN_PEDA('\"\"{input_path}\"\"')\""
    )


def _zip_dir_with_prefix(src_dir: Path, dest_zip: Path, prefix: str) -> None:
    files: List[Path] = [p for p in src_dir.rglob("*") if p.is_file()]
    files.sort(key=lambda p: str(p.relative_to(src_dir)).lower())

    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    if dest_zip.exists():
        dest_zip.unlink()

    with zipfile.ZipFile(dest_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in files:
            rel = path.relative_to(src_dir)
            arc = Path(prefix) / rel
            zf.write(path, arcname=str(arc.as_posix()))


def _cleanup_sqlite_sidecars(root: Path) -> None:
    for db in root.rglob("*.db"):
        cleanup_sqlite_sidecars(db)


def run_peda_step(
    case_id: str,
    case_dir: Path,
    peda_version: str = "v9.1.3",
    enabled: bool = True,
    mode: str = "stub",
) -> Dict[str, Any]:
    log = logging.getLogger(__name__)
    case_dir = Path(case_dir)

    if not enabled:
        log.info("PEDA step skipped (disabled).")
        return {"skipped": True, "reason": "disabled"}

    peda_dir_name = f"PEDA{peda_version}"
    video_dir_name = f"{case_id} PEDA{peda_version}-Video"
    data_zip_name = f"{case_id} PEDA{peda_version}-Data.zip"

    peda_out_dir = case_dir / peda_dir_name
    applog_dir = peda_out_dir / "applog"
    results_dir = peda_out_dir / "Results"
    annon_logs_dir = get_annon_logs_dir(case_dir)
    stub_log_path = annon_logs_dir / "PEDA_run_log.txt"

    if mode == "stub":
        log.warning("PEDA step running in stub mode; MATLAB/MAIN_PEDA not executed.")
        applog_dir.mkdir(parents=True, exist_ok=True)
        results_dir.mkdir(parents=True, exist_ok=True)

        (results_dir / "dummy.mat").write_bytes(b"STUB_MATLAB_DATA")
        (results_dir / "summary.txt").write_text("STUB SUMMARY\n", encoding="utf-8")

        intended_cmd = _intended_matlab_command(stub_log_path, case_dir)
        stub_log_path.write_text(
            "\n".join(
                [
                    "STUB: PEDA execution not run.",
                    f"case_id: {case_id}",
                    f"case_dir: {case_dir}",
                    f"intended_command: {intended_cmd}",
                    "",
                ]
            ),
            encoding="utf-8",
        )
    elif mode == "matlab":
        raise NotImplementedError(
            "PEDA MATLAB execution not implemented. Expected behavior: locate MATLAB, "
            "run with -logfile to peda_out_dir/applog/peda_log.txt, and capture stdout/stderr."
        )
    else:
        raise ValueError(f"Unsupported PEDA mode: {mode}")

    case_dir.mkdir(parents=True, exist_ok=True)
    video_dir = case_dir / video_dir_name
    if video_dir.exists():
        shutil.rmtree(video_dir, ignore_errors=True)
    shutil.copytree(peda_out_dir, video_dir)

    cleanup_summary = cleanup_artifacts(video_dir, patterns=["*.mat"], dry_run=False)
    mat_removed_count = sum(
        1 for p in cleanup_summary.get("deleted", []) if str(p).lower().endswith(".mat")
    )
    log.info("PEDA video cleanup removed %s .mat files.", mat_removed_count)

    data_zip_path = case_dir / data_zip_name
    _zip_dir_with_prefix(peda_out_dir, data_zip_path, peda_dir_name)

    if (
        peda_out_dir.exists()
        and peda_out_dir.name == peda_dir_name
        and peda_out_dir.resolve().parent == case_dir.resolve()
    ):
        shutil.rmtree(peda_out_dir, ignore_errors=True)

    _cleanup_sqlite_sidecars(case_dir)

    return {
        "status": "stub" if mode == "stub" else "unknown",
        "peda_out_dir": str(peda_out_dir),
        "video_dir": str(video_dir),
        "data_zip_path": str(data_zip_path),
        "mat_removed_count": mat_removed_count,
        "stub_log_path": str(stub_log_path),
    }
