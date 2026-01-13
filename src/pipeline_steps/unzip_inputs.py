# PURPOSE: Deterministic unzip utility for pipeline staging.
# INPUTS: List of archive paths and a destination root.
# OUTPUTS: Extracted folders under dest_root plus summary dict.
# NOTES: Uses archive_utils and guards against writes outside dest_root.
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Dict, Any

from src.archive_utils import extract_archive
from src.logutil import ProcessingError


def _strip_zip_suffix(name: str) -> str:
    base = name
    while base.lower().endswith(".zip"):
        base = base[:-4]
        base = base.rstrip(". ")
    return base or "archive"


def _ensure_under_root(dest_root: Path, candidate: Path) -> Path:
    root_resolved = dest_root.resolve(strict=False)
    cand_resolved = candidate.resolve(strict=False)
    try:
        cand_resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise ProcessingError(
            f"Refusing to extract outside dest_root: dest_root={dest_root} candidate={candidate}"
        ) from exc
    return candidate


def _unique_dest(dest_root: Path, base_name: str) -> Path:
    candidate = _ensure_under_root(dest_root, dest_root / base_name)
    if not candidate.exists():
        return candidate
    n = 1
    while True:
        candidate = _ensure_under_root(dest_root, dest_root / f"{base_name}__{n}")
        if not candidate.exists():
            return candidate
        n += 1


def expand_archives(input_paths: List[Path], dest_root: Path) -> Dict[str, Any]:
    log = logging.getLogger(__name__)
    dest_root = Path(dest_root)
    dest_root.mkdir(parents=True, exist_ok=True)

    expanded = 0
    skipped = 0
    items: List[Dict[str, Any]] = []

    for raw in input_paths:
        if raw is None:
            skipped += 1
            items.append({"status": "skipped", "reason": "none"})
            continue
        path = Path(raw)
        if not path.exists() or not path.is_file():
            skipped += 1
            items.append({"src": str(path), "status": "skipped", "reason": "missing"})
            continue
        if path.suffix.lower() != ".zip":
            skipped += 1
            items.append({"src": str(path), "status": "skipped", "reason": "not_zip"})
            continue

        base_name = _strip_zip_suffix(path.name)
        dest_dir = _unique_dest(dest_root, base_name)
        log.info("Unzip input: %s -> %s", path, dest_dir)
        try:
            extract_archive(path, dest_dir, prefer_7z=True)
        except Exception as exc:
            raise ProcessingError(f"Input unzip failed: {path} -> {dest_dir}: {exc}") from exc
        expanded += 1
        items.append({"src": str(path), "dest": str(dest_dir), "status": "expanded"})

    return {
        "status": "ok",
        "dest_root": str(dest_root),
        "expanded": expanded,
        "skipped": skipped,
        "items": items,
    }
