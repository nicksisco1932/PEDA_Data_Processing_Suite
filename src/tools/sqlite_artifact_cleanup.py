#!/usr/bin/env python3
"""
Utility to remove SQLite sidecar files (-wal/-shm) for a given DB path.
"""
from __future__ import annotations

import os
import stat
from pathlib import Path


def _try_unlink(path: Path) -> None:
    if not path.exists():
        return
    try:
        path.unlink()
        return
    except PermissionError:
        # On Windows, clear read-only and retry once.
        try:
            os.chmod(path, stat.S_IWRITE)
            path.unlink()
        except Exception:
            return
    except Exception:
        return


def cleanup_sqlite_sidecars(db_path: Path) -> None:
    """
    Delete SQLite sidecar files for the given DB path.
    Handles:
      - "<db_path>-wal" / "<db_path>-shm"
      - "<db_path>.db-wal" / "<db_path>.db-shm" (if db_path has a suffix)
    Does not raise if files are missing.
    """
    db_path = Path(db_path)
    candidates = [
        db_path.with_name(db_path.name + "-wal"),
        db_path.with_name(db_path.name + "-shm"),
    ]

    if db_path.suffix:
        candidates.extend(
            [
                db_path.with_suffix(".db-wal"),
                db_path.with_suffix(".db-shm"),
            ]
        )

    for candidate in candidates:
        _try_unlink(candidate)
