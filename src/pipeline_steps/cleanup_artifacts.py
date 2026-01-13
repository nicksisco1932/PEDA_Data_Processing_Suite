# PURPOSE: Remove known artifact files under a guarded working directory.
# INPUTS: Root directory, filename patterns, and dry_run flag.
# OUTPUTS: Summary dict with deleted file list and counts.
# NOTES: Refuses unsafe roots (drive root, home dir); logs each deletion.
from __future__ import annotations

import logging
import os
import stat
from pathlib import Path
from typing import List, Dict, Any, Tuple


DEFAULT_PATTERNS = [
    "*.mat",
    "local.db-wal",
    "local.db-shm",
    "*.db-wal",
    "*.db-shm",
]


def _root_guard(root: Path) -> Tuple[bool, str]:
    if not root.exists():
        return False, "missing_root"
    if not root.is_dir():
        return False, "not_a_directory"

    resolved = root.resolve()
    anchor = Path(resolved.anchor)
    if resolved == anchor:
        return False, "drive_root"

    try:
        home = Path.home().resolve()
    except Exception:
        home = None

    if home and resolved == home:
        return False, "home_dir"
    if home and resolved == home.parent:
        return False, "home_parent"

    return True, "ok"


def _try_unlink(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        path.unlink()
        return True
    except PermissionError:
        try:
            os.chmod(path, stat.S_IWRITE)
            path.unlink()
            return True
        except Exception:
            return False
    except Exception:
        return False


def cleanup_artifacts(root: Path, patterns: List[str], dry_run: bool = False) -> Dict[str, Any]:
    log = logging.getLogger(__name__)
    root = Path(root)
    ok, reason = _root_guard(root)
    if not ok:
        log.warning("Cleanup refused for root=%s reason=%s", root, reason)
        return {
            "status": "refused",
            "reason": reason,
            "root": str(root),
            "patterns": patterns,
            "deleted": [],
            "dry_run": dry_run,
        }

    patterns = patterns or []
    candidates = []
    for pattern in patterns:
        candidates.extend(root.rglob(pattern))

    seen = set()
    deleted: List[str] = []
    failed: List[str] = []

    for path in candidates:
        if not path.is_file():
            continue
        key = str(path.resolve())
        if key in seen:
            continue
        seen.add(key)
        if dry_run:
            log.info("Cleanup dry-run: would delete %s", path)
            deleted.append(str(path))
            continue
        log.info("Cleanup delete: %s", path)
        if _try_unlink(path):
            deleted.append(str(path))
        else:
            failed.append(str(path))

    status = "ok" if not failed else "partial"
    return {
        "status": status,
        "root": str(root),
        "patterns": patterns,
        "deleted": deleted,
        "failed": failed,
        "dry_run": dry_run,
    }
