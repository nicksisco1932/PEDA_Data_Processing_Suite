# TDC_proc.py
from __future__ import annotations

from pathlib import Path
import logging
import shutil
import tempfile
import zipfile
import os
import zlib
import sys
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from localdb_anon import anonymize_in_place
from logutil import ProcessingError, StepTimer, ValidationError, copy_with_integrity


def _is_local_db(p: Path) -> bool:
    return p.name.lower() == "local.db" and p.is_file()


def _zip_dir(src_dir: Path, dest_zip: Path) -> None:
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dest_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(src_dir):
            rp = Path(root)
            for name in files:
                p = rp / name
                zf.write(p, arcname=str(p.relative_to(src_dir)))


def _first_session_dir(root: Path) -> Path | None:
    candidates = [d for d in root.iterdir() if d.is_dir()]
    for d in candidates:
        if d.name.startswith("_") and any(_is_local_db(p) for p in d.iterdir()):
            return d
    for d in candidates:
        if d.name.startswith("_"):
            return d
    return candidates[0] if candidates else None


def _find_local_db(root: Path) -> Path | None:
    for p in root.rglob("local.db"):
        if p.is_file():
            return p
    return None


def _find_archives(root: Path) -> List[Path]:
    return [p for p in root.rglob("*.zip") if p.is_file()]


def _extract_with_diagnostics(zf: zipfile.ZipFile, dest: Path, logger: logging.Logger) -> None:
    for member in zf.infolist():
        try:
            zf.extract(member, dest)
        except (zipfile.BadZipFile, zlib.error) as exc:
            logger.error(
                "Zip extraction failed for %s (file_size=%s compress_size=%s crc=%s)",
                member.filename,
                member.file_size,
                member.compress_size,
                member.CRC,
            )
            raise ProcessingError(
                f"TDC zip extraction failed for member '{member.filename}': {exc}"
            ) from exc


def run(
    *,
    root: Path,
    case: str,
    input_zip: Path,
    scratch: Path,
    date_shift_days: int = 137,
    allow_archives: bool = False,
    logger: logging.Logger | None = None,
    dry_run: bool = False,
    step_results: Optional[Dict[str, Any]] = None,
    status_mgr: Optional[Any] = None,
) -> dict:
    log = logger or logging.getLogger(__name__)
    case_dir = root / case
    tdc_dir = case_dir / f"{case} TDC Sessions"
    misc_dir = case_dir / f"{case} Misc"

    if not input_zip.exists() or not input_zip.is_file() or input_zip.suffix.lower() != ".zip":
        raise ValidationError(f"TDC input not found or not .zip: {input_zip}")

    bak = scratch / (input_zip.name + ".bak")
    staged_session_root = scratch / "TDC_staged"

    if dry_run:
        log.info("TDC dry-run: would copy, extract, anonymize, and stage %s", input_zip)
        return {
            "backup": bak,
            "backup_info": None,
            "staged_session": staged_session_root / "UNKNOWN_SESSION",
            "final_session": tdc_dir / "UNKNOWN_SESSION",
            "local_db": None,
            "session_zips": [],
        }

    try:
        log.info("TDC input: %s", input_zip)

        # 1) backup in scratch with integrity verification
        backup_info = copy_with_integrity(input_zip, bak, retries=2, logger=log)
        log.info(
            "TDC backup verified: attempts=%s src_sha256=%s dst_sha256=%s",
            backup_info.get("attempts"),
            backup_info.get("src_sha256"),
            backup_info.get("dst_sha256"),
        )

        # 2) unzip -> temp
        with tempfile.TemporaryDirectory(dir=scratch, prefix="tdc_unzipped_") as tmpdir:
            tmp = Path(tmpdir)
            with zipfile.ZipFile(bak, "r") as zf:
                _extract_with_diagnostics(zf, tmp, log)
            log.info("TDC extracted: %s", tmp)

            # 3) copy Logs -> <case> Misc\Logs (if present)
            logs_src = tmp / "Logs"
            if logs_src.exists() and logs_src.is_dir():
                target_logs = misc_dir / "Logs"
                n, final_logs = 1, target_logs
                while final_logs.exists():
                    final_logs = misc_dir / (f"Logs__{n}")
                    n += 1
                final_logs.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(logs_src, final_logs)
                log.info("Copied Logs: %s", final_logs)
            else:
                log.info("No Logs/ folder in TDC input (skipping).")

            # 4) find the session directory
            session_dir = _first_session_dir(tmp)
            if session_dir is None:
                raise ValidationError("No session directory found in TDC archive")
            session_name = session_dir.name
            log.info("Session: %s", session_name)

            # 5) stage destination in scratch
            staged_session = staged_session_root / session_name
            staged_session_root.mkdir(parents=True, exist_ok=True)
            if staged_session.exists():
                shutil.rmtree(staged_session, ignore_errors=True)

            # 6) stage the session as a directory tree (no recompression)
            shutil.copytree(session_dir, staged_session)
            log.info("Staged session: %s", staged_session)

            # 7) ensure local.db exists at workspace root
            staged_db = staged_session / "local.db"
            if not staged_db.is_file():
                alt_db = _find_local_db(staged_session)
                if alt_db is None:
                    raise ValidationError(f"local.db not found in staged session: {staged_session}")
                shutil.copy2(alt_db, staged_db)
                log.info("Copied local.db to workspace root: %s", staged_db)

            # 8) ensure Raw is a directory (repair Raw.zip if needed)
            raw_dir = staged_session / "Raw"
            raw_zip = staged_session / "Raw.zip"
            if raw_zip.exists():
                raw_dir.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(raw_zip, "r") as zf:
                    _extract_with_diagnostics(zf, raw_dir, log)
                raw_zip.unlink()
                log.info("Extracted Raw.zip -> %s", raw_dir)
            if raw_dir.exists() and not raw_dir.is_dir():
                raise ValidationError(f"Expected directory missing: {raw_dir}")
            if not raw_dir.exists():
                raise ValidationError(f"Expected directory missing: {raw_dir}")

            # 9) disallow archives under workspace by default
            if not allow_archives:
                archives = _find_archives(staged_session)
                if archives:
                    archive_list = "\n".join(str(p) for p in archives)
                    raise ValidationError(
                        "Unexpected archive found under workspace:\n" + archive_list
                    )

            step_name = "local.db anonymization"
            if step_results is not None:
                with StepTimer(
                    logger=log, step_name=step_name, results=step_results, status_mgr=status_mgr
                ):
                    summary = anonymize_in_place(
                        staged_db, date_shift_days=date_shift_days, make_temp_proof=False, logger=log
                    )
            else:
                summary = anonymize_in_place(
                    staged_db, date_shift_days=date_shift_days, make_temp_proof=False, logger=log
                )
            log.info("Anonymized local.db (tables: %s)", len(summary.get("tables", [])))

        # 10) copy staged session to final case tree
        target = tdc_dir / session_name
        n = 1
        while target.exists():
            target = tdc_dir / f"{session_name}__{n}"
            n += 1
        shutil.copytree(staged_session, target)
        log.info("TDC final: %s", target)
    except ValidationError:
        raise
    except ProcessingError:
        raise
    except Exception as exc:
        raise ProcessingError(f"TDC processing failed: {exc}") from exc

    return {
        "backup": bak,
        "backup_info": backup_info if not dry_run else None,
        "staged_session": staged_session,
        "final_session": target,
        "local_db": staged_db,
        "session_zips": [],
    }
