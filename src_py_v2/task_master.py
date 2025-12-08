from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional


# --------------------------------------------------------------------
# External dependency: peda_setup()
# Adjust import to your package layout as needed.
# --------------------------------------------------------------------
try:
    from peda_setup import peda_setup
except ImportError:  # placeholder so this file still imports cleanly
    def peda_setup() -> Dict[str, Any]:
        raise RuntimeError("peda_setup() is not available. Ensure it is importable.")


# --------------------------------------------------------------------
# Global configuration (can be overridden via env vars)
# --------------------------------------------------------------------
PEDA_VERSION = os.getenv("PEDA_VERSION", "v9.1.3")

# Default Python implementation root is this file's directory (src_py_v2)
# You can override with PEDA_PY_ROOT if you ever want to.
DEFAULT_PY_ROOT = Path(__file__).resolve().parent
NS_PEDA_ROOT = Path(os.getenv("PEDA_PY_ROOT", DEFAULT_PY_ROOT)).resolve()


# --------------------------------------------------------------------
# Report structure (Python analogue of the MATLAB struct)
# --------------------------------------------------------------------
@dataclass
class TaskReport:
    mode: str
    started: str
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    info: List[str] = field(default_factory=list)


# --------------------------------------------------------------------
# PEDA path scaffolding (Python analogue of pedapaths.m & friends)
# --------------------------------------------------------------------
@dataclass
class PedaPaths:
    patientID: str
    sessionRoot: Path
    case_root: Path
    output_root: Path
    seg_out: Path
    work_seg: Path
    pathSessionFiles: Path
    pathData: Path


def find_segments(case_dir: str | Path) -> List[Path]:
    """
    Return a list of full paths to segment subdirectories under case_dir.

    Segments are defined as directories whose names match:
        YYYY-MM-DD--HH-MM-SS
    e.g., 2025-11-05--07-05-25

    This intentionally excludes container folders such as 'Raw' or 'PEDAv9.1.3'.
    """
    case_dir = Path(case_dir)
    segs: List[Path] = []

    # Segment names: 4-digit year, 2-digit month/day, then '--', then 3x 2-digit fields
    pattern = re.compile(r"^\d{4}-\d{2}-\d{2}--\d{2}-\d{2}-\d{2}$")

    for entry in case_dir.iterdir():
        if entry.is_dir() and pattern.match(entry.name):
            segs.append(entry.resolve())

    return segs


def pedapaths(
    case_dir: str | Path,
    patient_id: Optional[str],
    seg_path: str | Path,
    seg_idx: int,
) -> PedaPaths:
    """
    Build input/work/output paths deterministically.

    Mirrors MATLAB pedapaths.m behavior.
    """
    case_dir = Path(case_dir).resolve()
    seg_path = Path(seg_path).resolve()

    # Locate the real sessionRoot (folder that has Raw + local.db)
    candidates = [seg_path, seg_path.parent, case_dir]
    session_root: Optional[Path] = None
    for candidate in candidates:
        if (candidate / "Raw").is_dir() and (candidate / "local.db").is_file():
            session_root = candidate
            break
    if session_root is None:
        session_root = seg_path.parent

    # Derive patientID if needed
    if not patient_id:
        # Look for \123_45-678\ pattern anywhere in the path
        m = re.search(r"[\\/](\d{3}_\d{2}-\d{3})[\\/]", str(case_dir))
        if not m:
            raise ValueError(f"Cannot derive patientID from case_dir: {case_dir}")
        patient_id = m.group(1)

    # Case and output roots
    case_root = case_dir.parent  # ...\<CASE_ID>\
    output_root = case_root / "output" / f"{patient_id} TDC Sessions"

    # Segment output dir
    seg_name = seg_path.name
    seg_out = output_root / seg_name

    # Temp WORK dir
    ts = datetime.now().strftime("%Y%m%d-%H%M%S%f")  # yyyymmdd-HHMMSSFFF analogue
    work_root = case_root / "work"
    work_seg = work_root / f"seg{seg_idx:02d}_{ts}"

    # PEDA expectations used by legacy bits
    path_session_files = session_root / "stub" / "stub"
    path_data = seg_out / "PEDA"

    # Ensure folders exist
    for p in (work_seg, seg_out, path_data):
        p.mkdir(parents=True, exist_ok=True)

    return PedaPaths(
        patientID=patient_id,
        sessionRoot=session_root,
        case_root=case_root,
        output_root=output_root,
        seg_out=seg_out,
        work_seg=work_seg,
        pathSessionFiles=path_session_files,
        pathData=path_data,
    )


def ensure_raw_present_temp(P: PedaPaths) -> Path:
    """
    Stage a self-contained copy for reading under P.work_seg.

    Never modifies the source container.

    Returns the stagedRoot path.
    """
    staged_root = P.work_seg / "session"
    raw_src = P.sessionRoot / "Raw"
    db_src = P.sessionRoot / "local.db"

    staged_root.mkdir(parents=True, exist_ok=True)

    # Copy local.db (tiny)
    if db_src.is_file():
        shutil.copy2(db_src, staged_root / "local.db")
    else:
        raise FileNotFoundError(f"local.db not found at {db_src}")

    # If Raw is large, prefer a junction (fast) on Windows; else copy.
    raw_dst = staged_root / "Raw"
    if not raw_dst.exists():
        did_link = False

        if sys.platform.startswith("win"):
            try:
                if is_ntfs(raw_src) and is_ntfs(staged_root):
                    # mklink /J "dst" "src"
                    cmd = ["cmd", "/c", "mklink", "/J", str(raw_dst), str(raw_src)]
                    proc = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                    )
                    did_link = proc.returncode == 0 and raw_dst.is_dir()
                    if not did_link and proc.stdout.strip():
                        print(f"[mklink] fallback: {proc.stdout.strip()}")
                    if not did_link and proc.stderr.strip():
                        print(f"[mklink] stderr: {proc.stderr.strip()}")
            except Exception:
                did_link = False  # swallow and fallback

        if not did_link:
            # Fallback: copy (can be slow; but always works)
            shutil.copytree(raw_src, raw_dst)

    return staged_root


def is_ntfs(path: str | Path) -> bool:
    """
    Best-effort NTFS check. On Windows, uses 'wmic logicaldisk'.
    Returns False on error or non-Windows platforms.
    """
    path = Path(path)

    if not sys.platform.startswith("win"):
        return False

    try:
        drive = path.drive
        if not drive:  # e.g., relative path
            drive = str(path.resolve().drive)
        drive_letter = drive.rstrip(":").upper()
        if not drive_letter:
            return False

        cmd = [
            "wmic",
            "logicaldisk",
            "where",
            f"DeviceID='{drive_letter}:'",
            "get",
            "FileSystem",
            "/value",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            return False

        output_upper = proc.stdout.upper()
        return "FILESYSTEM=NTFS" in output_upper
    except Exception:
        return False


# --------------------------------------------------------------------
# Main entry point
# --------------------------------------------------------------------
def task_master(mode: str = "verify") -> Tuple[bool, TaskReport]:
    """
    task_master('verify') — pre-proc accountability: paths, required files, shadowing.
    Writes a log with error codes and returns (ok, report).
    """
    startT = datetime.now()
    report = TaskReport(mode=mode, started=startT.isoformat())

    logger: Optional[logging.Logger] = None
    log_path: Optional[Path] = None

    # ---- Load setup + open log
    try:
        S = peda_setup()  # expected to return a dict-like object with APPLOG_DIR etc.
    except Exception as exc:
        logger, log_path = _open_log(None, "task_master")
        _log(logger, "ERROR", "TM004_SETUP_MISSING", f"peda_setup failed: {exc!r}")
        report.errors.append("TM004_SETUP_MISSING")
        _close_log(logger)
        return False, report

    logger, log_path = _open_log(S, "task_master")
    _log(logger, "INFO", "TM000_BEGIN", f"Task master start: {startT.isoformat()}")

    # ---- Required functions (edit list as needed)
    required_funcs = [
        "CreateTMaxTDose_wrapper",
        "CreateTMaxTDose_orig",
        "ParseRawDataFolder",
        "RetrieveSxParameters",
        "GenerateMovies",
        "AnalyzeHardwareLogs",
        "AdditionalImageMasking",
        "CreateIsotherms",
        "TreatmentControllerSummary",
        "OutputStatistics",
        "PlotTmax",
        "ReadData",
        "CalculateDynamicMasks",
        "UnwindAngle",
    ]

    safe_shadow_names = {"UnwindAngle"}  # extend list later if needed

    ok = True

    # We emulate MATLAB `which -all` by scanning possible roots for .py files
    search_roots = _build_search_roots(S)

    for name in required_funcs:
        hits = _find_function_files(name, search_roots)
        if not hits:
            ok = False
            _log(logger, "ERROR", "TM001_MISSING_FUNC", f"Missing required function: {name}")
            report.errors.append(f"TM001_MISSING_FUNC:{name}")
            continue

        if len(hits) > 1:
            active = hits[0]
            is_ns_active = str(active).lower().startswith(str(NS_PEDA_ROOT).lower())
            is_safe_shadow = is_ns_active or name in safe_shadow_names

            if is_safe_shadow:
                _log(
                    logger,
                    "WARN",
                    "TM002_SHADOWING_OK",
                    f"{name} shadowed ({len(hits)} copies). Active (OK): {active}",
                )
                for ii, path in enumerate(hits[1:], start=2):
                    _log(logger, "INFO", "TM002_DETAIL", f"Shadowed copy #{ii}: {path}")
                report.warnings.append(f"TM002_SHADOWING_OK:{name}")
            else:
                ok = False
                _log(
                    logger,
                    "ERROR",
                    "TM002_SHADOWING",
                    f"Function {name} is shadowed by {len(hits)} locations. Active: {active}",
                )
                for ii, path in enumerate(hits[1:], start=2):
                    _log(logger, "INFO", "TM002_DETAIL", f"Shadowed copy #{ii}: {path}")
                report.errors.append(f"TM002_SHADOWING:{name}")
        else:
            _log(logger, "INFO", "TM_OK_FUNC", f"{name} -> {hits[0]}")
            report.info.append(f"OK:{name}")

    # ---- Sanity: confirm log dir exists
    app_log_dir = Path(S.get("APPLOG_DIR")) if isinstance(S, dict) and "APPLOG_DIR" in S else None
    if not app_log_dir or not app_log_dir.is_dir():
        ok = False
        missing_dir = str(app_log_dir) if app_log_dir else "<None>"
        _log(logger, "ERROR", "TM003_MISSING_DIR", f"APPLOG_DIR missing or invalid: {missing_dir}")
        report.errors.append("TM003_MISSING_DIR:APPLOG_DIR")

    # ---- Versions (optional)
    if PEDA_VERSION:
        _log(logger, "INFO", "TM_VER", f"PEDA_VERSION={PEDA_VERSION}")
    else:
        _log(logger, "WARN", "TM006_VERSION", "PEDA_VERSION not set.")
        report.warnings.append("TM006_VERSION")

    # ---- Summarize & close
    elapsed = (datetime.now() - startT).total_seconds()
    _log(
        logger,
        "INFO",
        "TM999_END",
        f"Done. ok={int(bool(ok))} | elapsed={elapsed:.3f}s | log={log_path}",
    )
    _close_log(logger)

    # Also drop a JSON report next to the log
    if log_path is not None:
        try:
            json_path = log_path.with_suffix(".json")
            with json_path.open("w", encoding="utf-8") as f:
                json.dump(report.__dict__, f, indent=2)
        except Exception:
            # Non-fatal: handlers are closed, so we don't try to log here
            pass

    return ok, report


# --------------------------------------------------------------------
# Helper functions for task_master
# --------------------------------------------------------------------
def _build_search_roots(S: Dict[str, Any]) -> List[Path]:
    """
    Build the ordered list of roots to search for required functions.

    Priority:
      1. NS_PEDA_ROOT (Python impl root, default = src_py_v2)
      2. S['PEDA_ROOT'] or S['ROOT_DIR'], if present
      3. This file's directory
    """
    roots: List[Path] = []

    # Python PEDA root has highest priority
    roots.append(NS_PEDA_ROOT)

    # Include any configured PEDA root
    if isinstance(S, dict):
        raw_root = S.get("PEDA_ROOT") or S.get("ROOT_DIR")
        if raw_root:
            roots.append(Path(raw_root))

    # Fall back to current file's directory
    here = Path(__file__).resolve().parent
    roots.append(here)

    # Deduplicate while preserving order
    seen = set()
    out: List[Path] = []
    for root in roots:
        if root not in seen:
            out.append(root)
            seen.add(root)
    return out


def _find_function_files(name: str, roots: List[Path]) -> List[Path]:
    """
    Return all candidate files matching the given function name.

    We now only consider Python files (.py), not MATLAB .m files.
    """
    hits: List[Path] = []
    for root in roots:
        for ext in (".py",):
            candidate = root / f"{name}{ext}"
            if candidate.is_file():
                hits.append(candidate)
    return hits


def _open_log(S: Optional[Dict[str, Any]], stem: str) -> Tuple[logging.Logger, Path]:
    """
    Open a log file and return a configured logger and the log path.

    If S.APPLOG_DIR is missing/invalid, falls back to ./applog next to this file.
    """
    if (
        not S
        or not isinstance(S, dict)
        or "APPLOG_DIR" not in S
        or not Path(S["APPLOG_DIR"]).is_dir()
    ):
        here = Path(__file__).resolve().parent
        applog = here / "applog"
        applog.mkdir(parents=True, exist_ok=True)
    else:
        applog = Path(S["APPLOG_DIR"])

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = applog / f"{stem}_{stamp}.log"

    logger_name = f"{stem}_{stamp}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # avoid double logging if root has handlers

    # Clear existing handlers if re-used (paranoid but safe)
    if logger.handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    ch = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime)s | %(levelname)-5s | %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger, log_path


def _log(logger: Optional[logging.Logger], level: str, code: str, msg: str) -> None:
    """
    Log a single line with the given level and code.

    This mirrors your MATLAB formatting: CODE column + message text.
    """
    if logger is None:
        return
    text = f"{code:18s} | {msg}"
    level = level.upper()
    if level == "ERROR":
        logger.error(text)
    elif level in ("WARN", "WARNING"):
        logger.warning(text)
    elif level == "INFO":
        logger.info(text)
    else:
        logger.debug(text)


def _close_log(logger: Optional[logging.Logger]) -> None:
    """
    Close all handlers attached to the logger, to release file handles.
    """
    if logger is None:
        return
    for handler in list(logger.handlers):
        handler.flush()
        handler.close()
        logger.removeHandler(handler)


# --------------------------------------------------------------------
# Simple CLI hook for ad-hoc testing
# --------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="PEDA task_master and basic I/O staging test."
    )
    parser.add_argument(
        "--mode",
        default="verify",
        help="Mode passed to task_master (default: verify)",
    )
    parser.add_argument(
        "--case-dir",
        default=None,
        help="TDC case directory for I/O staging test (optional)",
    )
    parser.add_argument(
        "--patient-id",
        default="",
        help="Explicit patientID; if omitted, derived from case_dir when needed.",
    )

    args = parser.parse_args()

    ok_val, rep = task_master(args.mode)
    print(f"ok={ok_val}")
    print(json.dumps(rep.__dict__, indent=2))

    if args.case_dir:
        print("\n[I/O] Running basic segment + staging test...")
        segs = find_segments(args.case_dir)
        print(f"[I/O] Found {len(segs)} segment(s) under {args.case_dir}")
        if segs:
            P = pedapaths(args.case_dir, args.patient_id or None, segs[0], 1)
            staged = ensure_raw_present_temp(P)
            print(f"[I/O] Staged session root: {staged}")
