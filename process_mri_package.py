#!/usr/bin/env python3
r"""
process_mri_package.py  (v1.3.0)

Guarantees this layout:
<CASE> MR DICOM\
  ├─ DICOM\
  └─ <CASE>_MRI.zip
"""

from __future__ import annotations
import argparse, logging, shutil, sys, os, zipfile, re
from pathlib import Path

_CASE_PAT = re.compile(r"(?P<a>\d{3})[-_](?P<b>\d{2})[-_](?P<c>\d{3})")

def extract_norm_id(text: str) -> str | None:
    m = _CASE_PAT.search(text or "")
    if not m: return None
    return f"{m.group('a')}_{m.group('b')}-{m.group('c')}"

def setup_logger(logs_root: Path) -> logging.Logger:
    logs_root.mkdir(parents=True, exist_ok=True)
    log_file = logs_root / "process_mri_package.log"
    logger = logging.getLogger("mri")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    sh = logging.StreamHandler(sys.stdout)
    fh.setFormatter(fmt); sh.setFormatter(fmt)
    logger.handlers.clear(); logger.addHandler(fh); logger.addHandler(sh)
    return logger

def ensure_layout(case_dir: Path, norm_id: str, logger: logging.Logger) -> tuple[Path, Path]:
    out_dir = case_dir / f"{norm_id} MR DICOM"
    dicom_dir = out_dir / "DICOM"
    dicom_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Ensured MR DICOM structure at {out_dir}")
    return out_dir, dicom_dir

def copy_or_zip_input(mri_input: Path, out_dir: Path, norm_id: str, logger: logging.Logger, simulate: bool) -> Path:
    mri_zip = out_dir / f"{norm_id}_MRI.zip"
    if simulate:
        logger.info("SIMULATION: not modifying input; creating marker only")
        return mri_zip

    if mri_input.is_file() and mri_input.suffix.lower() == ".zip":
        logger.info(f"Copying MRI zip to {mri_zip}")
        shutil.copy2(mri_input, mri_zip)
    elif mri_input.is_dir():
        logger.info(f"Zipping directory {mri_input} -> {mri_zip}")
        with zipfile.ZipFile(mri_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
            for p in mri_input.rglob("*"):
                if p.is_file():
                    zf.write(p, arcname=p.relative_to(mri_input))
    else:
        logger.warning("MRI input not found or unsupported; skipping zip creation")
    return mri_zip

def main():
    ap = argparse.ArgumentParser(description="Prepare MRI output structure.")
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--birthdate", type=str, required=True)
    ap.add_argument("--out-root", type=Path, required=True)
    ap.add_argument("--logs-root", type=Path, required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--simulate", action="store_true", help="Create only structure and write _sim_mri.txt")
    args = ap.parse_args()

    # Derive case_dir and norm_id
    # out-root = <Data_Clean>; case_dir = out-root/<CASE>
    norm_id = extract_norm_id(args.input.name) or extract_norm_id(str(args.input))
    if not norm_id:
        print("Cannot derive case id from input; name should contain NNN- NN -NNN", file=sys.stderr)
        sys.exit(2)
    case_dir = args.out_root / norm_id

    logger = setup_logger(args.logs_root)
    out_dir, dicom_dir = ensure_layout(case_dir, norm_id, logger)

    # Honor simulate: only place marker
    mri_zip = copy_or_zip_input(args.input, out_dir, norm_id, logger, simulate=args.simulate)

    if args.simulate:
        try:
            (out_dir / "_sim_mri.txt").write_text("Simulated MRI processing.\n", encoding="utf-8")
            logger.info("SIM: wrote _sim_mri.txt")
        except Exception as e:
            logger.warning(f"SIM: could not write marker: {e}")

    logger.info(f"Final MRI zip: {mri_zip}")
    logger.info("MRI step complete.")
    sys.exit(0)

if __name__ == "__main__":
    main()
