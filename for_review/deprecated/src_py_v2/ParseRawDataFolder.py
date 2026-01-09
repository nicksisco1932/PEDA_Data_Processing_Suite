"""
ParseRawDataFolder.py

Best-effort Python port of ParseRawDataFolder.m.

Purpose:
    - Discover Raw/ acquisitions under a session or case root.
    - Classify each Raw subfolder as Thermometry or TUV by file count.
    - Copy each acquisition into the corresponding segment's modality folder.

Behavior mirrors the MATLAB remake:
    - Accepts either a session folder (contains Raw/) or a case root that has
      child session folders with Raw/.
    - If multiple session folders are found, processes each.
    - If multiple segment folders exist, assigns each acquisition to the most
      recent segment with timestamp <= acquisition timestamp (fallback to last).
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Tuple


@dataclass
class Options:
    DryRun: bool = False
    Verbose: bool = True
    TUVMin: int = 300
    ThermMin: int = 312
    FilePattern: str = "raw*.dat"


def _log(msg: str, verbose: bool = True) -> None:
    if verbose:
        print(msg)


def _parse_ts(name: str) -> Optional[datetime]:
    m = re.search(r"(\d{4}-\d{2}-\d{2}--\d{2}-\d{2}-\d{2})", name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1).replace("--", " "), "%Y-%m-%d %H-%M-%S")
    except Exception:
        return None


def _list_subdirs(path: Path) -> List[Path]:
    return [p for p in path.iterdir() if p.is_dir() and p.name not in (".", "..")]


def _count_files(folder: Path, pattern: str) -> int:
    return sum(1 for _ in folder.glob(pattern) if _.is_file())


def _classify(n_files: int, therm_min: int, tuv_min: int) -> str:
    if n_files >= therm_min:
        return "Thermometry"
    if n_files >= tuv_min:
        return "TUV"
    return "Unknown"


def _detect_sessions(root: Path) -> List[Path]:
    """
    Return list of session folders that contain Raw/.
    """
    sessions: List[Path] = []
    for child in _list_subdirs(root):
        if (child / "Raw").is_dir():
            sessions.append(child)
    return sessions


def _detect_segments(session_dir: Path) -> List[Tuple[Path, Optional[datetime]]]:
    """
    Detect segment folders under session_dir, skipping Raw/, PEDA*, applog, etc.
    """
    segments: List[Tuple[Path, Optional[datetime]]] = []
    skip_names = {"Raw", "applog"}
    for child in _list_subdirs(session_dir):
        if child.name in skip_names or child.name.startswith("PEDA"):
            continue
        ts = _parse_ts(child.name)
        segments.append((child, ts))
    return segments


def _copytree(src: Path, dst: Path, dry_run: bool, verbose: bool) -> None:
    if dry_run:
        _log(f"[copy] {src} -> {dst}", verbose)
        return
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    _log(f"[ok] Copied: {src} -> {dst}", verbose)


def ParseRawDataFolder(dirName: str, **kwargs) -> None:
    opt = Options(**{k: v for k, v in kwargs.items() if k in Options.__annotations__})
    root = Path(dirName)
    if not root.is_dir():
        raise FileNotFoundError(f"Folder not found: {root}")

    # If root has no Raw/, try to discover session folders that do.
    sessions: List[Path]
    if (root / "Raw").is_dir():
        sessions = [root]
    else:
        sessions = _detect_sessions(root)
        if not sessions:
            # Try parent (handles work/ layouts)
            parent = root.parent
            if parent.is_dir():
                sessions = _detect_sessions(parent)
        if not sessions:
            _log(f"[info] No Raw/ folder found under: {root}. Nothing to do.", opt.Verbose)
            return

    # If multiple sessions discovered, process each and return.
    if len(sessions) > 1:
        _log(f"[info] Discovered {len(sessions)} sessions near: {root}", opt.Verbose)
        for idx, sess in enumerate(sessions, 1):
            _log(f"[info] Processing session ({idx}/{len(sessions)}): {sess}", opt.Verbose)
            ParseRawDataFolder(str(sess), **kwargs)
        return

    session = sessions[0]
    raw_dir = session / "Raw"

    # List and classify raw acquisitions
    raw_subs = _list_subdirs(raw_dir)
    if not raw_subs:
        _log(f"[warn] Raw/ exists but has no subfolders: {raw_dir}", opt.Verbose)
        return

    classified = []
    for sub in raw_subs:
        n_files = _count_files(sub, opt.FilePattern)
        dtype = _classify(n_files, opt.ThermMin, opt.TUVMin)
        ts = _parse_ts(sub.name)
        classified.append((sub, n_files, dtype, ts))

    # Drop Unknown
    keep = [(s, n, d, ts) for (s, n, d, ts) in classified if d != "Unknown"]
    if not keep:
        _log("[warn] No Raw acquisitions classified as Thermometry/TUV.", opt.Verbose)
        return

    # Detect segments
    segments = _detect_segments(session)
    if not segments:
        _log(f"[warn] No segment folders found under: {session}", opt.Verbose)
        return

    # Sort segments by timestamp
    segments = sorted(segments, key=lambda x: x[1] or datetime.min)
    seg_times = [ts or datetime.min for (_, ts) in segments]

    for sub, n_files, dtype, ts in keep:
        # Assign to nearest segment (timestamp <= acquisition)
        if ts:
            idx = max(i for i, t in enumerate(seg_times) if t <= ts) if any(t <= ts for t in seg_times) else len(segments) - 1
        else:
            idx = len(segments) - 1
        seg_path = segments[idx][0]
        dest_parent = seg_path / dtype
        dest = dest_parent / sub.name
        if not dest_parent.exists() and not opt.DryRun:
            dest_parent.mkdir(parents=True, exist_ok=True)
        _copytree(sub, dest, opt.DryRun, opt.Verbose)

    _log(f"[done] ParseRawDataFolder completed for: {session}", opt.Verbose)
