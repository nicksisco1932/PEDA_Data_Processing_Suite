# MRI_proc.py
from __future__ import annotations

from pathlib import Path
import logging
import shutil
import tempfile
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from logutil import ValidationError, ProcessingError, copy_with_integrity
from src.archive_utils import create_zip_from_dir, extract_archive


def run(
    *,
    root: Path,
    case: str,
    input_zip: Path,
    scratch: Path,
    logger: logging.Logger | None = None,
    dry_run: bool = False,
) -> dict:
    log = logger or logging.getLogger(__name__)
    case_dir = root / case
    mr_dir = case_dir / "MR DICOM"

    if not input_zip.exists() or not input_zip.is_file() or input_zip.suffix.lower() != ".zip":
        raise ValidationError(f"MRI input not found or not .zip: {input_zip}")

    bak = scratch / (input_zip.name + ".bak")
    out_zip = scratch / "MRI_anonymized.zip"
    final_zip = mr_dir / f"{case}_MRI.zip"

    if dry_run:
        log.info("MRI dry-run: would copy, extract, and re-zip %s", input_zip)
        return {"backup": bak, "scratch_zip": out_zip, "final_zip": final_zip}

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

        # 2) unzip -> (future anonymize) -> rezip into scratch/MRI_anonymized.zip
        with tempfile.TemporaryDirectory(dir=scratch, prefix="mri_unzipped_") as tmpdir:
            tmp = Path(tmpdir)
            try:
                extract_archive(bak, tmp, prefer_7z=True)
            except Exception as exc:
                raise ProcessingError(f"MRI extraction failed: {exc}") from exc
            log.info("MRI extracted: %s", tmp)

            # (anonymization would happen here later, operating on files under `tmp`)

            create_zip_from_dir(tmp, out_zip, prefer_7z=True)
            log.info("MRI repacked: %s", out_zip)

        # 3) copy final to case MR folder as <case>_MRI.zip
        final_zip.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(out_zip, final_zip)
        log.info("MRI final: %s", final_zip)
    except ValidationError:
        raise
    except Exception as exc:
        raise ProcessingError(f"MRI processing failed: {exc}") from exc

    return {
        "backup": bak,
        "backup_info": backup_info if not dry_run else None,
        "scratch_zip": out_zip,
        "final_zip": final_zip,
    }
