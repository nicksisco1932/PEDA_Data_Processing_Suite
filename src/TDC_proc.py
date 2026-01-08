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
            if staged_session.exists():
                shutil.rmtree(staged_session, ignore_errors=True)
            staged_session.mkdir(parents=True, exist_ok=True)

            # 6) process contents of the session
            local_db_src = None
            session_zips: List[Path] = []
            for child in session_dir.iterdir():
                if child.is_dir():
                    dest_zip = staged_session / f"{child.name}.zip"
                    _zip_dir(child, dest_zip)
                    session_zips.append(dest_zip)
                    log.info("Packed %s -> %s", child.name, dest_zip)
                elif child.suffix.lower() == ".zip":
                    dest_zip = staged_session / child.name
                    shutil.copy2(child, dest_zip)
                    session_zips.append(dest_zip)
                    log.info("Copied zip -> %s", dest_zip)
                elif _is_local_db(child):
                    local_db_src = child
                else:
                    pass

            if local_db_src is None:
                local_db_src = next((p for p in session_dir.rglob("*") if _is_local_db(p)), None)
            if local_db_src is None:
                raise ValidationError("local.db not found inside session directory")

            staged_db = staged_session / "local.db"
            shutil.copy2(local_db_src, staged_db)

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

        # 8) copy staged session to final case tree
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
        "session_zips": session_zips,
    }
