#!/usr/bin/env python3
"""
Create a minimal dummy case tree with a PHI-infected local.db.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.tools.make_fake_localdb import create_fake_localdb
from src.tools.sqlite_artifact_cleanup import cleanup_sqlite_sidecars


@dataclass(frozen=True)
class DummyCasePaths:
    root: Path
    case_dir: Path
    session_dir: Path
    db_path: Path


def make_dummy_case_tree(root: Path, case_id: str, session_name: str) -> DummyCasePaths:
    root = Path(root)
    case_dir = root / case_id
    misc_dir = case_dir / "Misc"
    mr_dir = case_dir / "MR DICOM"
    tdc_dir = case_dir / "TDC Sessions"
    session_dir = tdc_dir / session_name
    db_path = session_dir / "local.db"

    misc_dir.mkdir(parents=True, exist_ok=True)
    mr_dir.mkdir(parents=True, exist_ok=True)
    session_dir.mkdir(parents=True, exist_ok=True)

    create_fake_localdb(db_path, case_id)
    cleanup_sqlite_sidecars(db_path)

    return DummyCasePaths(
        root=root,
        case_dir=case_dir,
        session_dir=session_dir,
        db_path=db_path,
    )
