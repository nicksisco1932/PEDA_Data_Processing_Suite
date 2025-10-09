#!/usr/bin/env python3
r"""
process_mri_package.py  (v1.4.0)

Purpose (canonical result):
  <CASE>/<ID> MR DICOM/<ID>_MRI.zip

Back-compat & auto-detect:
- If 'case_dir' and/or '--norm-id' are omitted, they are auto-derived from --input and --out-root.
- If both are provided, they are honored.

Other behavior:
- If --input is a .zip, it is moved/copied into MR DICOM with canonical name.
- If --input is a directory (DICOM tree), it is zipped to canonical name (requires --apply to actually write).
- Removes empty "<ID> MR DICOM/DICOM" directories after packaging.
- Logs go to <CASE>/applog/Logs/process_mri_package.log (arguments are canonicalized accordingly).
"""

from __future__ import annotations
import argparse, logging, os, re, shutil, sys, zipfile
from pathlib import Path
from typing import Optional

_CASE_PAT = re.compile(r"(?P<a>\d{3})[-_](?P<b>\d{2})[-_](?P<c>\d{3})")

def _extract_norm_id(text: str) -> Optional[str]:
    if not text:
        return None
    m = _CASE_PAT.search(str(text))
    if not m:
        return None
    return f"{m.group('a')}_{m.group('b')}-{m.group('c')}"

def _canonical_logs_root(case_dir: Path, logs_root: Optional[Path]) -> Path:
    # always end up at <case_dir>/applog/Logs
    dest = case_dir / "applog" / "Logs"
    dest.mkdir(parents=True, exist_ok=True)
    return dest

def _setup_logger(case_root: Path, logs_root: Optional[Path]) -> logging.Logger:
    logs_root = _canonical_logs_root(case_root, logs_root)
    log_file = logs_root / "process_mri_package.log"
    logger = logging.getLogger("mri")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    sh = logging.StreamHandler(sys.stdout)
    fh.setFormatter(fmt); sh.setFormatter(fmt)
    logger.handlers.clear(); logger.addHandler(fh); logger.addHandler(sh)
    return logger

def _zip_dir(src_dir: Path, dst_zip: Path) -> None:
    dst_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dst_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for p in src_dir.rglob("*"):
            if p.is_file():
                zf.write(p, arcname=p.relative_to(src_dir))

def _remove_empty_dicom(mr_dir: Path) -> None:
    dcm = mr_dir / "DICOM"
    if dcm.exists():
        try:
            next(dcm.iterdir())
        except StopIteration:
            try:
                dcm.rmdir()
            except Exception:
                pass

def _auto_resolve(case_dir: Optional[Path], norm_id: Optional[str], input_path: Optional[Path], out_root: Optional[Path]) -> tuple[Path, str]:
    """
    Fill in missing case_dir and/or norm_id using input file/dir and out_root.
    Priority for norm_id detection:
      1) from input path name
      2) from existing case_dir path
      3) from out_root children (not used unless both above missing)
    """
    # Resolve norm_id if missing
    nid = norm_id or _extract_norm_id(str(input_path)) or ( _extract_norm_id(str(case_dir)) if case_dir else None )
    if not nid:
        raise SystemExit("ERROR: Unable to derive case ID (NNN_NN-NNN). Provide --norm-id or an --input name containing it.")

    # Resolve case_dir if missing
    if case_dir is None:
        base = out_root if out_root else (input_path.parent if input_path else Path.cwd())
        case_dir = Path(base) / nid
    return case_dir, nid

def main():
    ap = argparse.ArgumentParser(description="Normalize MRI package into canonical <ID>_MRI.zip")
    # Make positional case_dir optional for back-compat
    ap.add_argument("case_dir", type=Path, nargs="?", default=None, help="Canonical case directory (optional; auto-inferred)")
    ap.add_argument("--norm-id", required=False, help="Case ID like 017_01-479 (optional; auto-inferred)")
    ap.add_argument("--input", type=Path, required=False, help="Path to MRI zip or DICOM directory")
    ap.add_argument("--out-root", type=Path, default=None, help="Parent folder that will contain the case dir (used when inferring case_dir)")
    ap.add_argument("--logs-root", type=Path, default=None)
    ap.add_argument("--birthdate", type=str, default="19000101")  # placeholder passthrough
    ap.add_argument("--apply", action="store_true", help="If input is a directory, write the zip when set")
    ap.add_argument("--simulate", action="store_true")
    args = ap.parse_args()

    # Auto-resolve case_dir + norm_id if needed
    case_dir, nid = _auto_resolve(args.case_dir, args.norm_id, args.input, args.out_root)
    mr_dir = case_dir / f"{nid} MR DICOM"
    mr_dir.mkdir(parents=True, exist_ok=True)

    logger = _setup_logger(case_dir, args.logs_root)
    dst_zip = mr_dir / f"{nid}_MRI.zip"

    logger.info(f"Case dir : {case_dir}")
    logger.info(f"Norm ID  : {nid}")
    logger.info(f"Target   : {dst_zip}")

    if args.input is None:
        logger.info("No MRI input provided; nothing to do.")
        _remove_empty_dicom(mr_dir)
        sys.exit(0)

    src = args.input
    if not src.exists():
        logger.error(f"Input not found: {src}")
        sys.exit(2)

    if src.is_file() and src.suffix.lower() == ".zip":
        if args.simulate:
            logger.info("[SIM] Would move/copy MRI zip to canonical target.")
        else:
            try:
                # Try move; fall back to copy (handles different volumes/locks)
                try:
                    shutil.move(str(src), str(dst_zip))
                except Exception:
                    shutil.copy2(src, dst_zip)
                logger.info(f"Placed MRI zip → {dst_zip}")
            except Exception as e:
                logger.error(f"Failed to place MRI zip: {e}")
                sys.exit(1)
    elif src.is_dir():
        if not args.apply:
            logger.info("[DRY] Input is a directory; add --apply to create the zip.")
            _remove_empty_dicom(mr_dir)
            sys.exit(0)
        try:
            _zip_dir(src, dst_zip)
            logger.info(f"Zipped directory → {dst_zip}")
        except Exception as e:
            logger.error(f"Failed to zip MRI directory: {e}")
            sys.exit(1)
    else:
        logger.error(f"Unsupported input type: {src}")
        sys.exit(2)

    # Remove empty DICOM dir to match canonical layout
    _remove_empty_dicom(mr_dir)
    logger.info("MRI packaging complete.")
    sys.exit(0)

if __name__ == "__main__":
    main()
