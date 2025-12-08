from __future__ import annotations

import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional


# ---------------------------------------------------------------------
# Dataclass equivalent of MATLAB P struct
# ---------------------------------------------------------------------
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


# ---------------------------------------------------------------------
# find_segments(caseDir)
# ---------------------------------------------------------------------
def find_segments(case_dir: str | Path) -> List[Path]:
    """
    Return a list of full paths to all immediate subdirectories of case_dir.

    MATLAB:
        d = dir(fullfile(caseDir,'*'));
        d = d([d.isdir]);
        names = {d.name};
        names = names(~ismember(names,{'.','..'}));
        segs = cellfun(@(n) fullfile(caseDir,n), names, 'uni',0);
    """
    case_dir = Path(case_dir)
    segs: List[Path] = []
    for entry in case_dir.iterdir():
        if entry.is_dir() and entry.name not in (".", ".."):
            segs.append(entry.resolve())
    return segs


# ---------------------------------------------------------------------
# pedapaths(caseDir, patientID, segPath, segIdx)
# ---------------------------------------------------------------------
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

    # --- Locate the real sessionRoot (folder that has Raw + local.db) ---
    candidates = [seg_path, seg_path.parent, case_dir]
    session_root: Optional[Path] = None
    for candidate in candidates:
        if (candidate / "Raw").is_dir() and (candidate / "local.db").is_file():
            session_root = candidate
            break
    if session_root is None:
        session_root = seg_path.parent

    # --- Derive patientID if needed ---
    if not patient_id:
        # Look for \123_45-678\ pattern anywhere in the path
        m = re.search(r"[\\/](\d{3}_\d{2}-\d{3})[\\/]", str(case_dir))
        if not m:
            raise ValueError(f"Cannot derive patientID from case_dir: {case_dir}")
        patient_id = m.group(1)

    # --- Case and output roots ---
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


# ---------------------------------------------------------------------
# ensure_raw_present_temp(P)
# ---------------------------------------------------------------------
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


# ---------------------------------------------------------------------
# is_ntfs(pth)
# ---------------------------------------------------------------------
def is_ntfs(path: str | Path) -> bool:
    """
    Best-effort NTFS check. On Windows, uses 'wmic logicaldisk'.
    Returns False on error or non-Windows platforms.
    """
    path = Path(path)

    if not sys.platform.startswith("win"):
        return False

    try:
        # Extract drive letter, e.g., 'C'
        drive = path.drive
        if not drive:  # e.g., relative path
            drive = str(path.resolve().drive)
        # drive is like 'C:'; strip colon
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
