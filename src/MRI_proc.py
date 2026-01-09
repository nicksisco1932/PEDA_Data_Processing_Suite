# PURPOSE: Process MRI zip input and produce the canonical MR DICOM unzipped output.
# INPUTS: MRI zip path, scratch/output dirs, and run flags.
# OUTPUTS: <case_dir>/MR DICOM/<input_zip.stem> (UNZIPPED) directory.
# NOTES: Uses archive_utils with optional 7-Zip preference.
from __future__ import annotations

from pathlib import Path
import logging
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.logutil import ValidationError, ProcessingError, copy_with_integrity
from src.archive_utils import extract_archive


def run(
    *,
    root: Path,
    case: str,
    input_zip: Path,
    mr_dir: Path | None = None,
    scratch: Path,
    logger: logging.Logger | None = None,
    dry_run: bool = False,
    legacy_names: bool = False,
) -> dict:
    log = logger or logging.getLogger(__name__)
    case_dir = root / case
    mr_dir = mr_dir or (case_dir / "MR DICOM")

    if not input_zip.exists() or not input_zip.is_file() or input_zip.suffix.lower() != ".zip":
        raise ValidationError(f"MRI input not found or not .zip: {input_zip}")

    bak = scratch / (input_zip.name + ".bak")
    final_dir = mr_dir / f"{input_zip.stem} (UNZIPPED)"

    if dry_run:
        log.info("MRI dry-run: would copy and extract %s", input_zip)
        return {"backup": bak, "final_dir": final_dir}

    try:
        log.info("MRI input: %s", input_zip)

        # 1) backup into scratch with integrity verification
        backup_info = copy_with_integrity(input_zip, bak, retries=2, logger=log)
        log.info(
            "MRI backup verified: attempts=%s src_sha256=%s dst_sha256=%s",
            backup_info.get("attempts"),
            backup_info.get("src_sha256"),
            backup_info.get("dst_sha256"),
        )

        # 2) unzip -> final MR DICOM directory
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        if final_dir.exists():
            shutil.rmtree(final_dir, ignore_errors=True)
        final_dir.mkdir(parents=True, exist_ok=True)
        try:
            extract_archive(bak, final_dir, prefer_7z=True)
        except Exception as exc:
            raise ProcessingError(f"MRI extraction failed: {exc}") from exc
        log.info("MRI extracted to final dir: %s", final_dir)
    except ValidationError:
        raise
    except Exception as exc:
        raise ProcessingError(f"MRI processing failed: {exc}") from exc

    return {
        "backup": bak,
        "backup_info": backup_info if not dry_run else None,
        "final_dir": final_dir,
    }
