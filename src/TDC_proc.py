# TDC_proc.py
from __future__ import annotations

from pathlib import Path
import logging
import shutil
import tempfile
import sys
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from localdb_anon import anonymize_in_place
from logutil import ProcessingError, StepTimer, ValidationError, copy_with_integrity
from src.archive_utils import extract_archive


def _is_local_db(p: Path) -> bool:
    return p.name.lower() == "local.db" and p.is_file()


def resolve_tdc_sessions_dir(unzipped_root: Path) -> Path:
    direct = unzipped_root / "TDC Sessions"
    if direct.is_dir():
        return direct

    if any(p.is_dir() and p.name.startswith("_") for p in unzipped_root.iterdir()):
        return unzipped_root

    matches: List[Path] = []
    max_depth = 2
    for p in unzipped_root.rglob("TDC Sessions"):
        if not p.is_dir():
            continue
        rel = p.relative_to(unzipped_root)
        if len(rel.parts) <= max_depth:
            matches.append(p)

    if len(matches) == 1 and matches[0].is_dir():
        return matches[0]

    children = [p.name for p in unzipped_root.iterdir()]
    children.sort()

    if len(matches) > 1:
        matches.sort(key=lambda p: str(p).lower())
        raise ValidationError(
            "Multiple 'TDC Sessions' folders found under "
            f"{unzipped_root}: {', '.join(str(p) for p in matches)} "
            f"top_level_children={children}"
        )

    raise ValidationError(
        "Missing expected 'TDC Sessions' folder under "
        f"{unzipped_root}. candidates={matches} top_level_children={children}"
    )


def pick_active_session_dir(tdc_root: Path) -> str:
    candidates = [d for d in tdc_root.iterdir() if d.is_dir() and d.name.startswith("_")]
    if len(candidates) == 1:
        return candidates[0].name
    if not candidates:
        raise ValidationError(f"No session directories found under {tdc_root}")
    candidates.sort(key=lambda p: p.name.lower())
    raise ValidationError(
        "Multiple session directories found under "
        f"{tdc_root}: {', '.join(p.name for p in candidates)}"
    )


def _expand_workspace_zips(workspace: Path, log: logging.Logger) -> None:
    zips = [p for p in workspace.rglob("*.zip") if p.is_file()]
    for zip_path in zips:
        if zip_path.name.lower() == "raw.zip":
            dest_dir = workspace / "Raw"
        else:
            dest_dir = zip_path.with_suffix("")
        dest_dir.mkdir(parents=True, exist_ok=True)
        extract_archive(zip_path, dest_dir, prefer_7z=True)
        zip_path.unlink()
        log.info("Expanded zip -> %s", dest_dir)


def run(
    *,
    root: Path,
    case: str,
    input_zip: Path,
    scratch: Path,
    date_shift_days: int = 137,
    logger: logging.Logger | None = None,
    dry_run: bool = False,
    test_mode: bool = False,
    allow_workspace_zips: bool = False,
    step_results: Optional[Dict[str, Any]] = None,
    status_mgr: Optional[Any] = None,
) -> dict:
    log = logger or logging.getLogger(__name__)
    case_dir = root / case
    tdc_dir = case_dir / "TDC Sessions"
    misc_dir = case_dir / "Misc"

    if not input_zip.exists() or not input_zip.is_file() or input_zip.suffix.lower() != ".zip":
        raise ValidationError(f"TDC input not found or not .zip: {input_zip}")

    bak = scratch / (input_zip.name + ".bak")
    staged_session_root = scratch / "TDC_staged"
    session_name: str | None = None
    staged_session: Path | None = None
    staged_db: Path | None = None
    session_zips: List[Path] = []

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

        # 2) unzip -> temp (keep on failure for debugging)
        tmp = Path(tempfile.mkdtemp(dir=scratch, prefix="tdc_unzipped_"))
        try:
            extract_archive(bak, tmp, prefer_7z=True)
        except Exception as exc:
            raise ProcessingError(f"TDC extraction failed: {exc}") from exc
        log.info("TDC extracted: %s", tmp)

        tdc_root = resolve_tdc_sessions_dir(tmp)
        if not tdc_root.is_dir():
            raise ValidationError(f"Resolved TDC root does not exist: {tdc_root}")
        log.info("Resolved TDC root: %s", tdc_root)

        # 3) copy Logs -> Misc\Logs (if present)
        logs_src = tmp / "Logs"
        if not logs_src.exists():
            wrapper_logs = tdc_root.parent / "Logs"
            if wrapper_logs.exists():
                logs_src = wrapper_logs
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
        tdc_children = [p.name for p in tdc_root.iterdir() if p.is_dir()]
        tdc_children.sort()
        log.info("TDC root children: %s", tdc_children)

        session_dir_name = pick_active_session_dir(tdc_root)
        session_path = tdc_root / session_dir_name
        if not session_path.exists():
            raise ValidationError(
                "Session path missing: "
                f"tdc_root={tdc_root} session_dir={session_dir_name} "
                f"session_path={session_path} children={tdc_children}"
            )

        session_name = session_dir_name
        log.info("Session: %s", session_name)

        # 5) stage destination in scratch
        staged_session = staged_session_root / session_name
        if staged_session.exists():
            shutil.rmtree(staged_session, ignore_errors=True)
        staged_session.mkdir(parents=True, exist_ok=True)

        # 6) process contents of the session
        local_db_src = None
        for child in session_path.iterdir():
            if child.is_dir():
                dest_dir = staged_session / child.name
                shutil.copytree(child, dest_dir)
                log.info("Copied dir -> %s", dest_dir)
            elif child.suffix.lower() == ".zip":
                if child.name.lower() == "raw.zip":
                    dest_dir = staged_session / "Raw"
                else:
                    dest_dir = staged_session / child.stem
                dest_dir.mkdir(parents=True, exist_ok=True)
                try:
                    extract_archive(child, dest_dir, prefer_7z=True)
                except Exception as exc:
                    raise ProcessingError(f"Failed to expand {child}: {exc}") from exc
                log.info("Expanded zip -> %s", dest_dir)
            elif _is_local_db(child):
                local_db_src = child
            elif child.is_file():
                dest_file = staged_session / child.name
                shutil.copy2(child, dest_file)

        if local_db_src is None:
            local_db_src = next(
                (p for p in session_path.rglob("*") if _is_local_db(p)), None
            )
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

        _expand_workspace_zips(staged_session, log)

        if staged_session is None or staged_db is None or session_name is None:
            raise ValidationError("TDC staging failed; missing staged outputs")

        raw_dir = staged_session / "Raw"
        if not raw_dir.is_dir():
            raise ValidationError(f"Raw directory missing in workspace: {raw_dir}")
        if not staged_db.is_file():
            raise ValidationError(f"local.db missing in workspace: {staged_db}")
        zipped = list(staged_session.rglob("*.zip"))
        if zipped and not allow_workspace_zips:
            raise ValidationError(
                "Zip archives present in workspace: "
                + ", ".join(str(p) for p in zipped)
            )

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
