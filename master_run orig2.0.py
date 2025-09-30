#!/usr/bin/env python3
r"""
master_run.py  (v2.0.0)

Full pipeline OR archive-only, your choice.

Pipeline stages (when not --archive-only):
  1) clean_tdc_data.py  (TDC)
  2) process_mri_package.py  (MRI)
  3) run_peda (module) OR skip/simulate
  4) Archive newest PEDAv* dir → <NORM_ID> PEDAv*-Data.zip (unless --no-archive)

Archive-only mode:
  - Use --archive-only to skip all stages and just zip a PEDA folder.
  - Accepts an explicit --peda-path (e.g., D:\\PEDAv9.1.3). If omitted, picks newest PEDAv* under the case dir.
  - Never zips folders whose names end with "-Video".

Quality-of-life:
  - Normalizes logs paths to avoid Logs/Logs.
  - Detects swapped-ID MRI output (017-01_XXX) and rehomes into canonical (017_01-XXX).
  - Lets you pass --peda-path even in full pipeline: we will archive that explicit folder at the end (unless --no-archive).
"""

from __future__ import annotations
import argparse
import logging
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from glob import iglob
from pathlib import Path

# Optional import: only needed if you actually run PEDA (not archive-only)
try:
    from run_peda import run_peda, DEFAULT_PEDA_HOME  # type: ignore
except Exception:
    run_peda = None  # type: ignore
    DEFAULT_PEDA_HOME = "C:\\PEDA"

# =============================
# Helpers: PEDA archive / search
# =============================

def _find_latest_pedav_dir(search_dir: Path) -> Path | None:
    """Newest PEDAv* folder (excluding *-Video) under search_dir."""
    cands = [
        p for p in search_dir.iterdir()
        if p.is_dir() and p.name.startswith("PEDAv") and not p.name.lower().endswith("-video")
    ]
    return max(cands, key=lambda p: p.stat().st_mtime) if cands else None


def _archive_pedav_dir(
    pedav_dir: Path, out_case_dir: Path, case_id: str, label_name: str | None,
    logger: logging.Logger, clean: bool = False
) -> Path:
    """Zip the contents of pedav_dir to <case_id> <label>-Data.zip in out_case_dir."""
    base_name = label_name or pedav_dir.name
    zip_path = out_case_dir / f"{case_id} {base_name}-Data.zip"
    out_case_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"ARCHIVE: Zipping '{pedav_dir.name}' → {zip_path.name}")

    with tempfile.TemporaryDirectory(dir=out_case_dir) as tmpd:
        tmp_zip = Path(tmpd) / (zip_path.name + ".tmp")
        with zipfile.ZipFile(tmp_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
            for p in pedav_dir.rglob("*"):
                if p.is_file():
                    zf.write(p, arcname=p.relative_to(pedav_dir))
        if zip_path.exists():
            logger.info(f"ARCHIVE: Overwriting existing archive: {zip_path.name}")
            zip_path.unlink()
        shutil.move(str(tmp_zip), str(zip_path))

    logger.info(f"ARCHIVE: Complete: {zip_path.name} ({zip_path.stat().st_size:,} bytes)")
    if clean:
        logger.info(f"ARCHIVE: Removing source: {pedav_dir}")
        shutil.rmtree(pedav_dir, ignore_errors=False)
    return zip_path


# =============================
# ID normalization
# =============================

_CASE_PAT = re.compile(r"(?P<a>\d{3})[-_](?P<b>\d{2})[-_](?P<c>\d{3})")


def extract_norm_id(text: str) -> str | None:
    m = _CASE_PAT.search(text)
    if not m:
        return None
    return f"{m.group('a')}_{m.group('b')}-{m.group('c')}"


def resolve_norm_id(primary: Path, mri_input: str | None, mri_dir: str | None) -> str:
    candidates = [
        extract_norm_id(str(primary)),
        extract_norm_id(mri_input) if mri_input else None,
        extract_norm_id(mri_dir) if mri_dir else None,
    ]
    for c in candidates:
        if c:
            return c
    raise ValueError("Unable to derive normalized ID (NNN_NN-NNN) from inputs.")


def make_swapped_variant(norm_id: str) -> str | None:
    m = re.match(r"^(\d{3})_(\d{2})-(\d{3})$", norm_id)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}_{m.group(3)}"


# =============================
# Logging
# =============================

def setup_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("master")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    sh = logging.StreamHandler(sys.stdout)
    fh.setFormatter(fmt); sh.setFormatter(fmt)
    logger.addHandler(fh); logger.addHandler(sh)
    return logger


# =============================
# Subprocess utility
# =============================

def run_py(logger: logging.Logger, script_path: Path, args: list[str], dry_run: bool = False) -> int:
    cmd = [sys.executable, str(script_path)] + args
    logger.info(f"RUN: {cmd}")
    if dry_run:
        return 0
    proc = subprocess.run(cmd)
    return proc.returncode


# =============================
# MRI helpers
# =============================

def id_matches(text: str, norm_id: str) -> bool:
    found = extract_norm_id(text)
    return found == norm_id


def _autodetect_mri_input(case_dir: Path, norm_id: str) -> Path | None:
    zips = [Path(p) for p in iglob(str(case_dir / "**" / "*_MRI.zip"), recursive=True)]
    zips_matching = [z for z in zips if id_matches(z.name, norm_id)]
    zpool = zips_matching if zips_matching else zips
    if len(zpool) == 1:
        return zpool[0]
    if len(zpool) > 1:
        return None
    dcm_dir = case_dir / f"{norm_id} MR DICOM"
    if dcm_dir.is_dir():
        try:
            next(dcm_dir.iterdir())
            return dcm_dir
        except StopIteration:
            pass
    return None


def build_mri_args(
    case_dir: Path,
    logs_root: Path,
    norm_id: str,
    explicit_input: str | None,
    explicit_dir: str | None,
    birthdate: str | None,
    apply_flag: bool,
    strict_id: bool,
    logger: logging.Logger,
) -> tuple[list[str] | None, str, dict]:
    meta = {'mri_input': None, 'source': None, 'id_mismatch': False, 'expected': norm_id, 'found': None}
    out_root = case_dir.parent

    # Resolve MRI input
    if explicit_input:
        mri_input = Path(explicit_input)
        meta['source'] = 'explicit_input'
        if not mri_input.exists():
            return None, f"--mri-input does not exist: {mri_input}", meta
        meta['mri_input'] = mri_input
        found = extract_norm_id(mri_input.name); meta['found'] = found
        if found != norm_id:
            meta['id_mismatch'] = True
            msg = f"MRI input ID mismatch: expected {norm_id}, got {mri_input.name}"
            if strict_id: return None, msg, meta
            logger.warning(msg + " (continuing)")
    elif explicit_dir:
        mri_input = Path(explicit_dir)
        meta['source'] = 'explicit_dir'
        if not (mri_input.exists() and mri_input.is_dir()):
            return None, f"--mri-dir not a directory: {mri_input}", meta
        meta['mri_input'] = mri_input
        found = extract_norm_id(mri_input.name); meta['found'] = found
        if found != norm_id:
            meta['id_mismatch'] = True
            msg = f"MRI dir ID mismatch: expected {norm_id}, got {mri_input.name}"
            if strict_id: return None, msg, meta
            logger.warning(msg + " (continuing)")
    else:
        mri_input = _autodetect_mri_input(case_dir, norm_id)
        meta['source'] = 'autodetect'
        if mri_input is None:
            return None, "No unique *_MRI.zip or valid MR DICOM directory detected; skipping MRI.", meta
        meta['mri_input'] = mri_input
        found = extract_norm_id(mri_input.name); meta['found'] = found
        if found != norm_id:
            meta['id_mismatch'] = True
            msg = f"Autodetected MRI input does not match {norm_id}: {mri_input.name}"
            if strict_id: return None, msg, meta
            logger.warning(msg + " (continuing)")

    # Birthdate
    bd = (birthdate or "19000101")
    if len(bd) != 8 or not bd.isdigit():
        logger.warning(f"Invalid birthdate '{birthdate}', using 19000101")
        bd = "19000101"

    # Normalize logs-root so downstream tools that append "Logs" won't make Logs/Logs
    logs_arg = logs_root.parent if logs_root.name.lower() == "logs" else logs_root

    args = ["--input", str(mri_input), "--birthdate", bd, "--out-root", str(out_root), "--logs-root", str(logs_arg)]
    if apply_flag:
        args.append("--apply")
    return args, "", meta


# =============================
# TDC helpers
# =============================

def _autodetect_tdc_input(raw_case: Path, case_dir: Path, norm_id: str) -> Path | None:
    """Pick *_TDC.zip, prefer exact ID match, newest if many."""
    candidates: list[Path] = []
    if raw_case.is_file() and raw_case.suffix.lower() == ".zip" and "tdc" in raw_case.name.lower():
        candidates.append(raw_case)
    candidates += [Path(p) for p in iglob(str(raw_case.parent / "*_TDC.zip"))]
    candidates += [Path(p) for p in iglob(str(case_dir / "**" / "*_TDC.zip"), recursive=True)]
    if not candidates:
        return None
    exact = [c for c in candidates if extract_norm_id(c.name) == norm_id]
    pool = exact if exact else candidates
    pool = list({p.resolve(): p for p in pool}.values())  # unique by path
    if len(pool) == 1:
        return pool[0]
    pool.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return pool[0]


# =============================
# Case/paths
# =============================

def resolve_case_and_logs(raw_case: Path, out_root: str | None, norm_id: str, explicit_log_root: str | None) -> tuple[Path, Path, str | None]:
    warn = None
    base = Path(out_root).resolve() if out_root else raw_case.parent
    case_dir = base / norm_id
    if raw_case.is_dir() and raw_case.resolve() != case_dir.resolve():
        warn = f"Using canonical case directory {case_dir} (source: {raw_case})"
    elif raw_case.is_file():
        warn = f"Derived case directory from file: {case_dir}"
    log_root = Path(explicit_log_root).resolve() if explicit_log_root else (case_dir / "applog" / "Logs")
    return case_dir, log_root, warn


def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def _merge_dir(src: Path, dst: Path):
    """Move contents of src into dst; overwrite files if collide; remove src if empty."""
    _ensure_dir(dst)
    for item in src.iterdir():
        target = dst / item.name
        if item.is_dir():
            _merge_dir(item, target)
        else:
            if target.exists():
                target.unlink()
            shutil.move(str(item), str(target))
    try:
        src.rmdir()
    except OSError:
        pass


def fix_mri_output_variants(case_dir: Path, norm_id: str, logger: logging.Logger):
    """Rehome outputs produced under swapped ID (017-01_XXX) into canonical (017_01-XXX)."""
    swapped = make_swapped_variant(norm_id)
    if not swapped:
        return
    wrong_case_dir = case_dir.parent / swapped
    wrong_mr = wrong_case_dir / f"{swapped} MR DICOM"
    right_mr = case_dir / f"{norm_id} MR DICOM"

    if wrong_mr.exists():
        logger.warning(f"MRI output located under swapped-ID path: {wrong_mr}")
        logger.info(f"Rehoming MRI outputs to canonical: {right_mr}")
        if right_mr.exists():
            _merge_dir(wrong_mr, right_mr)
        else:
            _ensure_dir(right_mr.parent)
            shutil.move(str(wrong_mr), str(right_mr))
        try:
            wrong_case_dir.rmdir()
        except OSError:
            pass

    # Normalize any *_MRI.zip names to <norm_id>_MRI.zip
    if right_mr.exists():
        for z in right_mr.glob("*_MRI.zip"):
            correct = right_mr / f"{norm_id}_MRI.zip"
            if z.name != correct.name:
                if correct.exists():
                    correct.unlink()
                z.rename(correct)
                logger.info(f"Renamed MRI ZIP → {correct.name}")


# =============================
# Main
# =============================

def main():
    ap = argparse.ArgumentParser(description="TDC → MRI → PEDA orchestrator with archive-only shortcut.")

    # Core
    ap.add_argument("case_dir", help="Case directory OR a related ZIP/file (e.g., *_TDC.zip).")
    ap.add_argument("--out-root", default=None, help="Root for canonical <norm_id> directory.")
    ap.add_argument("--allow-id-mismatch", action="store_true",
                    help="Proceed even if naming doesn't match derived norm_id (strict by default).")

    # Archive-only / explicit PEDA
    ap.add_argument("--archive-only", action="store_true",
                    help="Skip all steps; only archive PEDAv output.")
    ap.add_argument("--peda-path", default=None,
                    help="Explicit PEDA folder to zip (e.g., D:\\PEDAv9.1.3).")
    ap.add_argument("--peda-name", default=None,
                    help="Override the label used in the zip name (defaults to folder basename).")

    # TDC wiring
    ap.add_argument("--tdc-input", default=None, help="Path to TDC input (e.g., *_TDC.zip).")

    # MRI wiring
    ap.add_argument("--mri-input", default=None, help="Path to MRI input (e.g., *_MRI.zip).")
    ap.add_argument("--mri-dir", default=None, help="Path to an MR DICOM directory (fallback if no zip).")
    ap.add_argument("--patient-birthdate", default=None, help="YYYYMMDD. If omitted/invalid, uses 19000101.")
    ap.add_argument("--mri-apply", action="store_true", help="Pass --apply to process_mri_package.py")

    # PEDA wiring
    ap.add_argument("--peda-home", default=DEFAULT_PEDA_HOME)
    ap.add_argument("--matlab-exe", default=None)
    ap.add_argument("--simulate-peda", action="store_true")
    ap.add_argument("--force-matlab", action="store_true")

    # Toggles
    ap.add_argument("--skip-tdc", action="store_true")
    ap.add_argument("--skip-mri", action="store_true")
    ap.add_argument("--skip-peda", action="store_true")

    # Logging / control
    ap.add_argument("--log-root", default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-archive", action="store_true", help="Disable automatic PEDA archiving (except in --archive-only).")
    ap.add_argument("--clean-peda", action="store_true", help="After archiving, delete the PEDAv* source folder")

    args = ap.parse_args()

    raw_case = Path(args.case_dir).resolve()

    # 1) Derive norm_id
    try:
        norm_id = resolve_norm_id(raw_case, args.mri_input, args.mri_dir)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)

    # 2) Canonical case dir & logs
    case_dir, log_root, warn = resolve_case_and_logs(raw_case, args.out_root, norm_id, args.log_root)
    case_dir.mkdir(parents=True, exist_ok=True)
    log_root.mkdir(parents=True, exist_ok=True)

    master_log = log_root / "master_run.log"
    logger = setup_logger(master_log)

    logger.info("==== MASTER START ====")
    logger.info(f"NORM ID     : {norm_id}")
    logger.info(f"RAW CASE ARG: {raw_case}")
    logger.info(f"CASE DIR    : {case_dir}")
    logger.info(f"LOG ROOT    : {log_root}")
    if warn:
        logger.warning(warn)

    # ------------------ ARCHIVE-ONLY SHORTCUT ------------------
    if args.archive_only:
        pedav = Path(args.peda_path).resolve() if args.peda_path else _find_latest_pedav_dir(case_dir)
        if pedav and pedav.name.lower().endswith("-video"):
            logger.error(f"Refusing to archive '*-Video' folder: {pedav}")
            sys.exit(2)
        if not pedav:
            logger.warning("ARCHIVE: No PEDAv* folder found (and no --peda-path supplied).")
            logger.info("==== MASTER ARCHIVE-ONLY COMPLETE ====")
            sys.exit(0)

        label = args.peda_name or pedav.name
        if args.dry_run:
            logger.info(f"[DRY-RUN] Would archive '{pedav}' → '{case_dir / (norm_id + ' ' + label + '-Data.zip')}'.")
            logger.info("==== MASTER ARCHIVE-ONLY COMPLETE ====")
            sys.exit(0)

        try:
            _archive_pedav_dir(pedav, case_dir, norm_id, label, logger, clean=args.clean_peda)
        except Exception as e:
            logger.error(f"ARCHIVE: Failed: {e}")
            sys.exit(1)
        logger.info("==== MASTER ARCHIVE-ONLY COMPLETE ====")
        sys.exit(0)

    # ------------------ NORMAL PIPELINE ------------------

    # TDC
    if not args.skip_tdc:
        tdc = Path(__file__).parent / "clean_tdc_data.py"
        if tdc.exists():
            tdc_input = Path(args.tdc_input).resolve() if args.tdc_input else _autodetect_tdc_input(raw_case, case_dir, norm_id)
            tdc_args = [str(case_dir), "--norm-id", norm_id, "--log-root", str(log_root)]
            if tdc_input:
                tdc_args += ["--input", str(tdc_input)]
            if args.allow_id_mismatch:
                tdc_args.append("--allow-id-mismatch")
            rc = run_py(logger, tdc, tdc_args, args.dry_run)
            if rc != 0:
                logger.error("TDC step failed.")
                sys.exit(rc)
        else:
            logger.warning("clean_tdc_data.py not found; skipping TDC.")

    # MRI
    if not args.skip_mri:
        mri = Path(__file__).parent / "process_mri_package.py"
        if mri.exists():
            mri_args, reason, mri_meta = build_mri_args(
                case_dir=case_dir,
                logs_root=log_root,
                norm_id=norm_id,
                explicit_input=args.mri_input,
                explicit_dir=args.mri_dir,
                birthdate=args.patient_birthdate,
                apply_flag=args.mri_apply,
                strict_id=(not args.allow_id_mismatch),
                logger=logger,
            )
            if mri_args is None:
                logger.warning(f"MRI step skipped: {reason}")
            else:
                rc = run_py(logger, mri, mri_args, args.dry_run)
                if rc != 0:
                    logger.error("MRI step failed.")
                    sys.exit(rc)
                if not args.dry_run:
                    fix_mri_output_variants(case_dir, norm_id, logger)
        else:
            logger.warning("process_mri_package.py not found; skipping MRI.")

    # PEDA (optional run or simulate)
    if not args.skip_peda:
        if args.dry_run:
            logger.info("[DRY-RUN] Would call run_peda(...)")
        else:
            if run_peda is None:
                logger.warning("run_peda module unavailable; skipping PEDA execution.")
            else:
                rc, plog = run_peda(
                    case_dir=case_dir,
                    peda_home=Path(args.peda_home),
                    matlab_exe=args.matlab_exe,
                    log_root=log_root,
                    simulate=True if args.simulate_peda else None,
                    force_matlab=args.force_matlab,
                )
                logger.info(f"PEDA log: {plog}")
                if rc != 0:
                    logger.error("PEDA step failed.")
                    sys.exit(rc)

    # Archive at end (unless disabled)
    if not args.no_archive:
        # Prefer explicit --peda-path if provided; else newest under case_dir
        pedav = Path(args.peda_path).resolve() if args.peda_path else _find_latest_pedav_dir(case_dir)
        if pedav and pedav.name.lower().endswith("-video"):
            logger.info("ARCHIVE: Skipping '*-Video' folder.")
            pedav = None
        if pedav:
            try:
                _archive_pedav_dir(pedav, case_dir, norm_id, (args.peda_name or pedav.name), logger, clean=args.clean_peda)
            except Exception as e:
                logger.error(f"ARCHIVE: Failed: {e}")
        else:
            logger.info("ARCHIVE: No PEDAv* folder found; skipping.")

    logger.info("==== MASTER COMPLETE ====")
    logger.info("====================================")
    logger.info("Run Summary:")
    logger.info(f"- Normalized ID : {norm_id}")
    logger.info(f"- Case Dir      : {case_dir}")
    logger.info(f"- Log Root      : {log_root}")
    logger.info(f"- TDC step      : {'SKIPPED' if args.skip_tdc else 'RUN (see logs)'}")
    logger.info(f"- MRI step      : {'SKIPPED' if args.skip_mri else 'RUN (see logs)'}")
    logger.info(f"- PEDA step     : {'SKIPPED' if args.skip_peda else ('SIMULATED' if args.simulate_peda else 'RUN')} ")
    logger.info("====================================")
    sys.exit(0)


if __name__ == "__main__":
    main()
