#!/usr/bin/env python3
r"""
master_run.py  (v2.3.1)

Pipeline orchestrator:
  1) clean_tdc_data.py  (TDC)
  2) process_mri_package.py  (MRI)
  3) normalize/move case PDF into <CASEID> Misc/<CASEID>_TreatmentReport.pdf
  4) anonymize local.db in-place (delegates to localdb_anon.py)
  5) run_peda (simulate or real)
  6) archive PEDAv output

Notes:
- local.db anonymization now lives in localdb_anon.py
- This script only calls into that module (or subprocess fallback).
"""

from __future__ import annotations
import argparse
import logging
import re
import shutil
import sys, os, subprocess
import tempfile
import zipfile
from glob import iglob
from pathlib import Path
from typing import Optional, List
import hashlib, json, time

# -----------------------------------------------------------------------------
# VENV BOOTSTRAP + DEPENDENCY RESOLVER (RELATIVE, CROSS-PLATFORM)
# -----------------------------------------------------------------------------

def _venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"

def _is_in_this_venv(venv_dir: Path) -> bool:
    vpy = _venv_python(venv_dir).resolve()
    return Path(sys.executable).resolve() == vpy or Path(sys.prefix).resolve() == venv_dir.resolve()

def _create_venv(venv_dir: Path) -> None:
    venv_dir.mkdir(parents=True, exist_ok=True)
    subprocess.check_call([sys.executable, "-m", "venv", str(venv_dir)])

def _pip_install(py: Path, args: list[str]) -> None:
    subprocess.check_call([str(py), "-m", "pip"] + args)

def _hash_files(paths: list[Path]) -> str:
    h = hashlib.sha256()
    for p in paths:
        if p.exists():
            h.update(p.name.encode())
            h.update(b"\0")
            h.update(p.read_bytes())
            h.update(b"\0")
    return h.hexdigest()

def _ensure_dependencies(py: Path, repo_root: Path) -> None:
    req = repo_root / "requirements.txt"
    req_lock = repo_root / "requirements-lock.txt"
    sentinel = repo_root / ".venv" / ".deps_hash.json"

    # ensure pip exists inside venv
    try:
        subprocess.check_call([str(py), "-m", "ensurepip", "--upgrade"])
    except Exception:
        pass

    os.environ.setdefault("PIP_DISABLE_PIP_VERSION_CHECK", "1")

    files = [p for p in [req_lock, req] if p.exists()]
    want = _hash_files(files)
    if sentinel.exists():
        try:
            have = json.loads(sentinel.read_text()).get("hash")
            if have == want:
                return
        except Exception:
            pass

    _pip_install(py, ["install", "--upgrade", "pip", "setuptools", "wheel"])
    if req_lock.exists():
        _pip_install(py, ["install", "-r", str(req_lock), "--upgrade-strategy", "only-if-needed"])
    elif req.exists():
        _pip_install(py, ["install", "-r", str(req), "--upgrade-strategy", "only-if-needed"])

    try:
        sentinel.write_text(json.dumps({"hash": want, "ts": int(time.time())}))
    except Exception:
        pass

def _bootstrap_venv_and_deps() -> None:
    repo_root = Path(__file__).resolve().parent
    venv_dir = Path(os.environ.get("PEDA_VENV_DIR", repo_root / ".venv")).resolve()
    vpy = _venv_python(venv_dir)

    if not _is_in_this_venv(venv_dir):
        if not vpy.exists():
            print(f"[bootstrap] Creating venv at {venv_dir} ...")
            _create_venv(venv_dir)
        print(f"[bootstrap] Re-launching inside venv: {vpy}")
        os.execv(str(vpy), [str(vpy)] + sys.argv)

    try:
        if os.environ.get("PEDA_NO_PIP", "0") != "1":
            _ensure_dependencies(Path(sys.executable), repo_root)
    except subprocess.CalledProcessError as e:
        sys.stderr.write(f"\n[bootstrap] Dependency setup failed (pip exit {e.returncode}). Continuing.\n")
        sys.stderr.write("Set PEDA_NO_PIP=1 to suppress future attempts.\n\n")

_bootstrap_venv_and_deps()


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
    cands = [
        p for p in search_dir.iterdir()
        if p.is_dir() and p.name.startswith("PEDAv") and not p.name.lower().endswith("-video")
    ]
    return max(cands, key=lambda p: p.stat().st_mtime) if cands else None


def _archive_pedav_dir(
    pedav_dir: Path, out_case_dir: Path, case_id: str, label_name: str | None,
    logger: logging.Logger, clean: bool = False
) -> Path:
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

    logs_arg = logs_root.parent if logs_root.name.lower() == "logs" else logs_root
    args = ["--input", str(mri_input), "--birthdate", bd, "--out-root", str(out_root), "--logs-root", str(logs_arg)]
    if apply_flag:
        args.append("--apply")
    return args, "", meta


# =============================
# TDC helpers
# =============================

def _autodetect_tdc_input(raw_case: Path, case_dir: Path, norm_id: str) -> Path | None:
    candidates: list[Path] = []
    if raw_case.is_file() and raw_case.suffix.lower() == ".zip" and "tdc" in raw_case.name.lower():
        candidates.append(raw_case)
    candidates += [Path(p) for p in iglob(str(raw_case.parent / "*_TDC.zip"))]
    candidates += [Path(p) for p in iglob(str(case_dir / "**" / "*_TDC.zip"), recursive=True)]
    if not candidates:
        return None
    exact = [c for c in candidates if extract_norm_id(c.name) == norm_id]
    pool = exact if exact else candidates
    pool = list({p.resolve(): p for p in pool}.values())  # unique
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


# =============================
# Find local.db
# =============================

def find_local_db(case_dir: Path, norm_id: str) -> Path | None:
    tdc_root = case_dir / f"{norm_id} TDC Sessions"
    if not tdc_root.exists():
        return None
    cands = [p for p in tdc_root.rglob("local.db") if p.is_file()]
    if not cands:
        return None
    cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0]


# =============================
# PDF handling (with parent search)
# =============================

_PDF_KEYWORDS = ["treatment", "report", "treatmentreport", "summary"]
_PDF_EXT_RE = re.compile(r"(?i)\.pdf(?:\.pdf)+$")

def _normalize_pdf_suffix(name: str) -> str:
    """Collapse trailing .pdf.pdf… → .pdf and normalize case to .pdf."""
    if _PDF_EXT_RE.search(name):
        return _PDF_EXT_RE.sub(".pdf", name)
    if name.lower().endswith(".pdf") and not name.endswith(".pdf"):
        return name[:-4] + ".pdf"
    return name

def _score_pdf_candidate(p: Path, case_id: str) -> int:
    """Heuristic: case_id match, keywords, shallow path; tiny penalty for long names."""
    n = p.name.lower()
    score = 0
    if case_id.lower() in n:
        score += 3
    if any(k in n for k in _PDF_KEYWORDS):
        score += 2
    # Prefer shallow paths relative to drive root; bounded
    score += max(0, 4 - len(p.parts))
    score -= int(len(p.name) / 50)
    return score

def _find_all_pdfs_multi(case_dir: Path, case_id: str) -> List[Path]:
    """
    Search order:
      1) Inside the case directory (recursive)
      2) One level up (case_dir.parent), shallow, only files that contain the case id
         or its swapped variant (e.g., 017_01-479 or 017-01_479)
    """
    results: List[Path] = []
    # 1) Search inside the case dir
    if case_dir.exists():
        results += [p for p in case_dir.rglob("*") if p.is_file() and p.suffix.lower() == ".pdf"]

    # 2) Shallow parent search
    parent = case_dir.parent
    if parent.exists():
        swapped = make_swapped_variant(case_id) or ""
        for p in parent.glob("*.pdf"):
            n = p.name.lower()
            if case_id.lower() in n or swapped.lower() in n:
                results.append(p)
    return results

def _best_pdf(pdfs: List[Path], case_id: str) -> Optional[Path]:
    if not pdfs:
        return None
    return sorted(
        pdfs,
        key=lambda p: (_score_pdf_candidate(p, case_id), p.stat().st_mtime),
        reverse=True,
    )[0]

def handle_case_pdf(
    case_dir: Path,
    norm_id: str,
    logger: logging.Logger,
    dry_run: bool,
    dest_basename: str = "TreatmentReport",
    explicit_pdf: Optional[str] = None,
) -> None:
    """
    Finds or uses an explicit case PDF, normalizes extension, and moves it to:
        <CASEID> Misc/<CASEID>_<dest_basename>.pdf
    """
    # --- Explicit PDF path provided ---
    if explicit_pdf:
        cand = Path(explicit_pdf).resolve()
        if not cand.exists():
            logger.warning(f"PDF: Explicit path not found: {cand}")
            return
        logger.info(f"PDF: Using explicit file '{cand}'")
        pdfs = [cand]
    else:
        logger.info("PDF: Scanning case folder and parent for PDFs")
        pdfs = _find_all_pdfs_multi(case_dir, norm_id)
        if not pdfs:
            logger.info("PDF: None found; skipping")
            return
        cand = _best_pdf(pdfs, norm_id)
        if not cand:
            logger.info("PDF: No suitable candidate; skipping")
            return
        logger.info(f"PDF: Selected '{cand}'")

    # --- Normalize extension (.pdf.pdf -> .pdf, .PDF -> .pdf) ---
    fixed_name = _normalize_pdf_suffix(cand.name)
    if fixed_name != cand.name:
        logger.info(f"PDF: Normalizing extension '{cand.name}' → '{fixed_name}'")
        if not dry_run:
            try:
                new_path = cand.with_name(fixed_name)
                cand.rename(new_path)
                cand = new_path
            except Exception as e:
                logger.warning(f"PDF: Rename failed ({e}); continuing with original")

    # --- Destination ---
    misc_dir = case_dir / f"{norm_id} Misc"
    misc_dir.mkdir(parents=True, exist_ok=True)
    dest = misc_dir / f"{norm_id}_{dest_basename}.pdf"
    if dest.exists():
        i = 2
        while True:
            alt = misc_dir / f"{norm_id}_{dest_basename}_{i}.pdf"
            if not alt.exists():
                dest = alt
                break
            i += 1
    logger.info(f"PDF: → Target '{dest.name}'")

    if dry_run:
        logger.info("PDF: [DRY-RUN] Would move file")
        return

    try:
        shutil.move(str(cand), str(dest))
        logger.info(f"PDF: Moved to '{dest}'")
    except Exception as e:
        logger.error(f"PDF: Move failed ({e})")



# =============================
# Main
# =============================

def main():
    ap = argparse.ArgumentParser(description="TDC → MRI → PDF normalize → local.db anonymize → PEDA → archive orchestrator.")

    # Core
    ap.add_argument("case_dir", help="Case directory OR *_TDC.zip")
    ap.add_argument("--out-root", default=None)
    ap.add_argument("--allow-id-mismatch", action="store_true")

    ap.add_argument("--pdf-input", default=None, help="Explicit path to case PDF (overrides search)")

    # Archive-only
    ap.add_argument("--archive-only", action="store_true")
    ap.add_argument("--peda-path", default=None)
    ap.add_argument("--peda-name", default=None)

    # TDC
    ap.add_argument("--tdc-input", default=None)

    # MRI
    ap.add_argument("--mri-input", default=None)
    ap.add_argument("--mri-dir", default=None)
    ap.add_argument("--patient-birthdate", default=None)
    ap.add_argument("--mri-apply", action="store_true")
    ap.add_argument("--simulate-mri", action="store_true")

    # PEDA
    ap.add_argument("--peda-home", default=DEFAULT_PEDA_HOME)
    ap.add_argument("--matlab-exe", default=None)
    ap.add_argument("--simulate-peda", action="store_true")
    ap.add_argument("--force-matlab", action="store_true")

    # Toggles
    ap.add_argument("--skip-tdc", action="store_true")
    ap.add_argument("--simulate-tdc", action="store_true")
    ap.add_argument("--skip-mri", action="store_true")
    ap.add_argument("--skip-peda", action="store_true")
    ap.add_argument("--skip-pdf", action="store_true", help="Skip case PDF detection/normalization")
    ap.add_argument("--pdf-dest-name", default="TreatmentReport", help="Basename for normalized PDF (default: TreatmentReport)")

    # Logging
    ap.add_argument("--log-root", default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-archive", action="store_true")
    ap.add_argument("--clean-peda", action="store_true")

    # local.db anonymization
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--anonymize-localdb", dest="anon_db", action="store_true")
    grp.add_argument("--skip-anonymize-localdb", dest="anon_db", action="store_false")
    ap.set_defaults(anon_db=True)
    ap.add_argument("--db-date-shift-days", type=int, default=137)

    args = ap.parse_args()
    raw_case = Path(args.case_dir).resolve()

    try:
        norm_id = resolve_norm_id(raw_case, args.mri_input, args.mri_dir)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)

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
    if warn: logger.warning(warn)

    # ---------------- ARCHIVE-ONLY ----------------
    if args.archive_only:
        pedav = Path(args.peda_path).resolve() if args.peda_path else _find_latest_pedav_dir(case_dir)
        if pedav and pedav.name.lower().endswith("-video"):
            logger.error("Refusing to archive '*-Video'")
            sys.exit(2)
        if not pedav:
            logger.warning("ARCHIVE: No PEDAv* folder found")
            sys.exit(0)
        label = args.peda_name or pedav.name
        if args.dry_run:
            logger.info(f"[DRY-RUN] Would archive {pedav}")
            sys.exit(0)
        try:
            _archive_pedav_dir(pedav, case_dir, norm_id, label, logger, clean=args.clean_peda)
        except Exception as e:
            logger.error(f"ARCHIVE: Failed {e}")
            sys.exit(1)
        sys.exit(0)

    # ---------------- TDC ----------------
    if not args.skip_tdc:
        tdc = Path(__file__).parent / "clean_tdc_data.py"
        if tdc.exists():
            tdc_input = Path(args.tdc_input).resolve() if args.tdc_input else _autodetect_tdc_input(raw_case, case_dir, norm_id)
            tdc_args = [str(case_dir), "--norm-id", norm_id, "--log-root", str(log_root)]
            if args.simulate_tdc:
                tdc_args.append("--simulate")
            if tdc_input:
                tdc_args += ["--input", str(tdc_input)]
            if args.allow_id_mismatch:
                tdc_args.append("--allow-id-mismatch")
            rc = run_py(logger, tdc, tdc_args, args.dry_run)
            if rc != 0: sys.exit(rc)
        else:
            logger.warning("clean_tdc_data.py not found")

    # ---------------- MRI ----------------
    if not args.skip_mri:
        mri = Path(__file__).parent / "process_mri_package.py"
        if mri.exists():
            mri_args, reason, _ = build_mri_args(case_dir, log_root, norm_id,
                                                 args.mri_input, args.mri_dir,
                                                 args.patient_birthdate,
                                                 args.mri_apply,
                                                 not args.allow_id_mismatch,
                                                 logger)
            if mri_args is None:
                logger.warning(f"MRI skipped: {reason}")
            else:
                if args.simulate_mri:
                    mri_args.append("--simulate")
                rc = run_py(logger, mri, mri_args, args.dry_run)
                if rc != 0: sys.exit(rc)
        else:
            logger.warning("process_mri_package.py not found")

    # ---------------- PDF handling ----------------
    if args.skip_pdf:
        logger.info("PDF: Skipped by flag")
    else:
        try:
            handle_case_pdf(case_dir=case_dir, norm_id=norm_id, logger=logger, dry_run=args.dry_run, dest_basename=args.pdf_dest_name, explicit_pdf=args.pdf_input)
        except Exception as e:
            logger.warning(f"PDF: Failed ({e}); continuing")

    # ---------------- local.db anonymization ----------------
    if args.anon_db:
        db_path = find_local_db(case_dir, norm_id)
        if not db_path:
            logger.warning("ANON: local.db not found; skipping")
        else:
            try:
                try:
                    from localdb_anon import anonymize_in_place
                    logger.info(f"ANON: Found local.db {db_path}")
                    anonymize_in_place(db_path, args.db_date_shift_days, make_temp_proof=True)
                except Exception:
                    cmd = [sys.executable, str(Path(__file__).parent / "localdb_anon.py"),
                           "--db", str(db_path),
                           "--date-shift-days", str(args.db_date_shift_days)]
                    if args.dry_run:
                        logger.info(f"[DRY-RUN] Would run {cmd}")
                    else:
                        rc = subprocess.run(cmd).returncode
                        if rc != 0:
                            raise RuntimeError(f"localdb_anon.py exited {rc}")
            except Exception as e:
                logger.error(f"ANON: Failed {e}")
                sys.exit(1)
    else:
        logger.info("ANON: Skipped by flag")

    # ---------------- PEDA ----------------
    if not args.skip_peda:
        if args.dry_run:
            logger.info("[DRY-RUN] Would run PEDA")
        else:
            if run_peda is None:
                logger.warning("run_peda unavailable")
            else:
                rc, plog = run_peda(
                    case_dir=case_dir,
                    peda_home=Path(args.peda_home),
                    matlab_exe=args.matlab_exe,
                    log_root=log_root,
                    simulate=bool(args.simulate_peda),               # <- always a real bool
                    force_matlab=args.force_matlab
                )
                logger.info(f"PEDA log {plog}")
                if rc != 0: sys.exit(rc)

    # ---------------- Archive PEDAv ----------------
    if not args.no_archive:
        pedav = Path(args.peda_path).resolve() if args.peda_path else _find_latest_pedav_dir(case_dir)
        if args.simulate_peda and not pedav:
            logger.info("ARCHIVE: Skipping (simulated run produced no PEDAv folder)")
        else:
            if pedav and pedav.name.lower().endswith("-video"):
                pedav = None
            if pedav:
                try:
                    _archive_pedav_dir(pedav, case_dir, norm_id, (args.peda_name or pedav.name), logger, clean=args.clean_peda)
                except Exception as e:
                    logger.error(f"ARCHIVE failed {e}")


    logger.info("==== MASTER COMPLETE ====")
    sys.exit(0)


if __name__ == "__main__":
    main()
