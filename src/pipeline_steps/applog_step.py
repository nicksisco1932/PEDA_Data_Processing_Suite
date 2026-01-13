# PURPOSE: Locate and consolidate TDC log files into Misc/Logs.
# INPUTS: Case root and case ID.
# OUTPUTS: Copied log file, optional source Logs removal, and summary dict.
# NOTES: Enforces hash-verified copy before removing source Logs folders.
from __future__ import annotations

import hashlib
import re
import shutil
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any, Iterable

from src.logutil import ProcessingError
from src.paths import misc_logs_dir, tdc_log_path


LOG_EXTS = {".log", ".txt"}
NAME_TOKEN = "tdc."
DATE_TOKEN_RE = re.compile(r"tdc\.(\d{4}_\d{2}_\d{2})", re.IGNORECASE)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def verified_copy(src: Path, dst: Path) -> Dict[str, Any]:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    shutil.copy2(src, dst)
    src_hash = sha256_file(src)
    dst_hash = sha256_file(dst) if dst.exists() else ""
    if src_hash != dst_hash:
        if dst.exists():
            try:
                dst.unlink()
            except Exception:
                pass
        raise ProcessingError(
            "TDC log copy hash mismatch: "
            f"src={src} dst={dst} src_sha256={src_hash} dst_sha256={dst_hash}"
        )
    return {"src_sha256": src_hash, "dst_sha256": dst_hash}


def _is_candidate(path: Path) -> bool:
    if not path.is_file():
        return False
    if path.suffix.lower() not in LOG_EXTS:
        return False
    return NAME_TOKEN in path.name.lower()


def _sort_key(path: Path) -> Tuple[int, float, str]:
    ext_rank = 0 if path.suffix.lower() == ".log" else 1
    mtime = path.stat().st_mtime
    return (ext_rank, -mtime, path.name.lower())


def _select_candidate(candidates: List[Path]) -> Path:
    candidates.sort(key=_sort_key)
    return candidates[0]


def _list_candidates(logs_dir: Path) -> List[Path]:
    if not logs_dir.exists() or not logs_dir.is_dir():
        return []
    return [p for p in logs_dir.iterdir() if _is_candidate(p)]


def _has_logs_suffix(path: Path) -> bool:
    for part in path.parts:
        if part.lower().startswith("logs__"):
            return True
    return False


def _walk_candidates(root: Path) -> List[Path]:
    if not root.exists() or not root.is_dir():
        return []
    candidates: List[Path] = []
    for path in root.rglob("*"):
        if not _is_candidate(path):
            continue
        if _has_logs_suffix(path):
            continue
        candidates.append(path)
    return candidates


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _same_path(left: Path, right: Path) -> bool:
    return left.resolve() == right.resolve()


def _find_tdc_log_with_meta(
    case_root: Path,
    search_roots: Optional[Iterable[Path]] = None,
) -> Tuple[Optional[Path], int, Optional[str]]:
    if search_roots:
        for root in search_roots:
            if not root:
                continue
            root = Path(root)
            if not root.exists() or not root.is_dir():
                continue
            logs_dir = root / "Logs"
            candidates = _list_candidates(logs_dir)
            if candidates:
                return _select_candidate(candidates), len(candidates), "tdc_logs_root"
            candidates = _walk_candidates(root)
            if candidates:
                return _select_candidate(candidates), len(candidates), "tdc_root_walk"
        return None, 0, None

    misc_logs = case_root / "Misc" / "Logs"
    tdc_logs = case_root / "TDC Sessions" / "Logs"

    candidates = _list_candidates(misc_logs)
    if candidates:
        return _select_candidate(candidates), len(candidates), "misc_logs"

    candidates = _list_candidates(tdc_logs)
    if candidates:
        return _select_candidate(candidates), len(candidates), "tdc_logs"

    fallback_candidates: List[Path] = []
    for root in (case_root / "Misc", case_root / "TDC Sessions"):
        fallback_candidates.extend(_walk_candidates(root))

    if not fallback_candidates:
        return None, 0, None

    selected = _select_candidate(fallback_candidates)
    if _is_under(selected, case_root / "Misc"):
        reason = "fallback_misc_walk"
    elif _is_under(selected, case_root / "TDC Sessions"):
        reason = "fallback_tdc_walk"
    else:
        reason = "fallback_walk"
    return selected, len(fallback_candidates), reason


def find_tdc_log(case_root: Path, search_roots: Optional[Iterable[Path]] = None) -> Optional[Path]:
    log_path, _, _ = _find_tdc_log_with_meta(case_root, search_roots)
    return log_path


def _extract_date_token(name: str) -> str:
    match = DATE_TOKEN_RE.search(name)
    if match:
        return match.group(1)
    return "unknown"


def install_tdc_log(
    case_root: Path,
    case_id: str,
    search_roots: Optional[Iterable[Path]] = None,
) -> Dict[str, Any]:
    log_path, candidate_count, selection_reason = _find_tdc_log_with_meta(
        case_root,
        search_roots,
    )
    if log_path is None:
        return {
            "status": "skipped",
            "reason": "no_log_found",
            "source_path": None,
            "dest_path": None,
            "source_dir_removed": False,
            "source_dir": None,
            "candidate_count": candidate_count,
            "selection_reason": selection_reason,
            "src_sha256": None,
            "dst_sha256": None,
        }

    token = _extract_date_token(log_path.name)
    dest_path = tdc_log_path(case_root, case_id, token)

    copy_info: Dict[str, Any]
    if _same_path(log_path, dest_path):
        src_hash = sha256_file(log_path)
        copy_info = {"src_sha256": src_hash, "dst_sha256": src_hash}
    else:
        copy_info = verified_copy(log_path, dest_path)

    misc_logs = misc_logs_dir(case_root)
    tdc_logs = case_root / "TDC Sessions" / "Logs"
    source_dir: Optional[Path] = None
    source_dir_removed = False
    warning = None

    if misc_logs.exists():
        for candidate in _list_candidates(misc_logs):
            if not _same_path(candidate, dest_path):
                try:
                    candidate.unlink()
                except Exception:
                    pass

    if _same_path(log_path.parent, misc_logs):
        source_dir = misc_logs
        source_dir_removed = False
    elif _same_path(log_path.parent, tdc_logs):
        source_dir = tdc_logs
        if source_dir.exists():
            shutil.rmtree(source_dir)
        source_dir_removed = not source_dir.exists()
    else:
        parent = log_path.parent
        misc_root = case_root / "Misc"
        tdc_root = case_root / "TDC Sessions"
        if parent.name.lower() == "logs" and (
            _same_path(parent.parent, misc_root) or _same_path(parent.parent, tdc_root)
        ):
            source_dir = parent
            if source_dir.exists():
                shutil.rmtree(source_dir)
            source_dir_removed = not source_dir.exists()
        else:
            source_dir = parent
            warning = "source_dir_not_removed"

    result = {
        "status": "copied",
        "source_path": str(log_path),
        "dest_path": str(dest_path),
        "source_dir_removed": source_dir_removed,
        "source_dir": str(source_dir) if source_dir else None,
        "candidate_count": candidate_count,
        "selection_reason": selection_reason,
        "src_sha256": copy_info.get("src_sha256"),
        "dst_sha256": copy_info.get("dst_sha256"),
        "tdc_date_token": token,
    }
    if warning:
        result["warning"] = warning
    return result
