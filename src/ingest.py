from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from logutil import sha256_file


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    with src.open("rb") as fsrc, dst.open("wb") as fdst:
        for chunk in iter(lambda: fsrc.read(1024 * 1024), b""):
            fdst.write(chunk)
        fdst.flush()
        try:
            os.fsync(fdst.fileno())
        except Exception:
            pass
    try:
        os.utime(dst, (src.stat().st_atime, src.stat().st_mtime))
    except Exception:
        pass


def stage_input_zip(
    source_zip: Path,
    ingest_dir: Path,
    *,
    attempts: int = 3,
    verify: bool = True,
    source_stability_check: bool = False,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    log = logger or logging.getLogger(__name__)
    errors: List[str] = []
    warnings: List[str] = []
    src_sha1 = None
    src_sha2 = None
    src_sha = None
    dst_sha = None
    last_staged: Optional[Path] = None

    if not source_zip.exists():
        return {
            "ok": False,
            "source_zip": str(source_zip),
            "staged_zip": None,
            "src_sha256": None,
            "dst_sha256": None,
            "attempts": 0,
            "errors": [f"Source zip not found: {source_zip}"],
            "warnings": [],
        }

    ingest_dir.mkdir(parents=True, exist_ok=True)

    if source_stability_check:
        src_sha1 = sha256_file(source_zip)
        time.sleep(1.0)
        src_sha2 = sha256_file(source_zip)
        if src_sha1 != src_sha2:
            msg = (
                "Source file is unstable across reads; aborting before staging. "
                f"source={source_zip} sha256_1={src_sha1} sha256_2={src_sha2}"
            )
            log.error("%s", msg)
            errors.append(msg)
            return {
                "ok": False,
                "source_zip": str(source_zip),
                "staged_zip": None,
                "src_sha256": src_sha2,
                "dst_sha256": None,
                "attempts": 0,
                "errors": errors,
                "warnings": warnings,
            }

    for attempt in range(1, max(1, attempts) + 1):
        stem = source_zip.stem
        suffix = source_zip.suffix
        staged_name = f"{stem}.attempt{attempt}{suffix}" if suffix else f"{stem}.attempt{attempt}"
        staged_zip = ingest_dir / staged_name
        last_staged = staged_zip
        try:
            if staged_zip.exists():
                staged_zip.unlink()
        except Exception:
            pass

        try:
            _copy_file(source_zip, staged_zip)
        except Exception as exc:
            errors.append(f"Copy failed on attempt {attempt}: {exc}")
            continue

        if staged_zip.stat().st_size <= 0:
            warnings.append(f"Staged file size is zero: {staged_zip}")
            try:
                staged_zip.unlink()
            except Exception:
                pass
            continue

        if verify:
            src_sha = sha256_file(source_zip)
            dst_sha = sha256_file(staged_zip)
            if src_sha != dst_sha:
                warn_msg = (
                    f"Hash mismatch on attempt {attempt}: src={source_zip} dst={staged_zip} "
                    f"src_sha256={src_sha} dst_sha256={dst_sha}"
                )
                log.warning("%s", warn_msg)
                warnings.append(warn_msg)
                try:
                    staged_zip.unlink()
                except Exception:
                    pass
                continue

        return {
            "ok": True,
            "source_zip": str(source_zip),
            "staged_zip": str(staged_zip),
            "src_sha256": src_sha or src_sha2,
            "dst_sha256": dst_sha,
            "attempts": attempt,
            "errors": errors,
            "warnings": warnings,
        }

    msg = (
        f"Failed to stage input zip after {attempts} attempts; source I/O unstable or file corrupt. "
        f"source={source_zip} last_staged={last_staged} src_sha256={src_sha} dst_sha256={dst_sha}"
    )
    errors.append(msg)
    return {
        "ok": False,
        "source_zip": str(source_zip),
        "staged_zip": str(last_staged) if last_staged else None,
        "src_sha256": src_sha or src_sha2,
        "dst_sha256": dst_sha,
        "attempts": attempts,
        "errors": errors,
        "warnings": warnings,
    }
