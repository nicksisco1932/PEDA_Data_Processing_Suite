from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

ARCHIVE_EXTS = {".zip", ".7z", ".rar", ".tar", ".gz", ".bz2"}


def validate_pre_peda(
    case_root: Path,
    *,
    forbid_archives: bool = False,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    log = logger or logging.getLogger(__name__)
    errors: List[str] = []
    warnings: List[str] = []

    mr_dir = case_root / "MR DICOM"
    tdc_dir = case_root / "TDC Sessions"
    workspace_dir: Optional[Path] = None

    if not mr_dir.is_dir():
        errors.append(f"Missing MR DICOM folder: {mr_dir}")
    if not tdc_dir.is_dir():
        errors.append(f"Missing TDC Sessions folder: {tdc_dir}")

    underscore_dirs: List[Path] = []
    if tdc_dir.is_dir():
        underscore_dirs = [
            p for p in tdc_dir.iterdir() if p.is_dir() and p.name.startswith("_")
        ]
        if len(underscore_dirs) != 1:
            found = ", ".join(str(p) for p in underscore_dirs) or "none"
            errors.append(
                f"Expected exactly one '_' workspace dir under {tdc_dir}; "
                f"expected={tdc_dir / '<underscore_dir>'}; found: {found}"
            )
        else:
            workspace_dir = underscore_dirs[0]

    expected_local_db = (
        workspace_dir / "local.db"
        if workspace_dir
        else tdc_dir / "<underscore_dir>" / "local.db"
    )
    expected_raw = (
        workspace_dir / "Raw"
        if workspace_dir
        else tdc_dir / "<underscore_dir>" / "Raw"
    )

    if workspace_dir:
        if not expected_local_db.is_file():
            errors.append(f"Missing local.db: {expected_local_db}")
        if not expected_raw.is_dir():
            errors.append(f"Missing Raw dir: {expected_raw}")

        if forbid_archives:
            archives = [
                str(p)
                for p in workspace_dir.rglob("*")
                if p.is_file() and p.suffix.lower() in ARCHIVE_EXTS
            ]
            if archives:
                errors.append(
                    "Archive files not allowed under workspace_dir:\n"
                    + "\n".join(archives)
                )

    result = {
        "pre_peda_ready": not errors,
        "mr_dir": str(mr_dir),
        "tdc_dir": str(tdc_dir),
        "workspace_dir": str(workspace_dir) if workspace_dir else None,
        "errors": errors,
        "warnings": warnings,
        "expected_local_db": str(expected_local_db),
        "expected_raw": str(expected_raw),
    }
    if errors:
        log.error("Pre-PEDA validation failed.")
        for e in errors:
            log.error(" - %s", e)
    return result
