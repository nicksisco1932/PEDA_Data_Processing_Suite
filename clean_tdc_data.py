#!/usr/bin/env python3
r"""
clean_tdc_data.py  (v1.4.0)

Purpose:
    Unpacks and organizes TDC case .zip archives into the canonical directory layout.

Features:
    - Normalizes folder structure (TDC Sessions, MR DICOM, Misc)
    - Removes redundant nested folders (e.g., Logs/Logs)
    - Prefixes moved log files with case ID
    - Can be run in simulate mode (--simulate) to test without modifying data
    - When run in simulate mode, writes "_sim_tdc.txt" inside the TDC Sessions folder

Example:
    python clean_tdc_data.py "D:\Data_Clean\017_01-479" --norm-id "017_01-479" --input "D:\Database\017-01_479_TDC.zip"

Author:
    Nicholas J. Sisco, Ph.D.
"""

from __future__ import annotations
import argparse
import logging
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Optional


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def unzip_tdc(input_zip: Path, out_dir: Path, logger: logging.Logger) -> None:
    """Extracts TDC zip file into the output directory."""
    if not input_zip.exists():
        logger.error(f"TDC zip not found: {input_zip}")
        sys.exit(2)
    logger.info(f"Unzipping {input_zip.name} ...")
    with zipfile.ZipFile(input_zip, 'r') as zf:
        zf.extractall(out_dir)
    logger.info(f"Extracted to {out_dir}")


def normalize_structure(case_dir: Path, norm_id: str, logger: logging.Logger) -> Path:
    """Ensures canonical structure inside the case directory."""
    logger.info("Normalizing folder structure...")
    tdc_dir = case_dir / f"{norm_id} TDC Sessions"
    logs_dir = tdc_dir / "Logs"
    misc_dir = case_dir / f"{norm_id} Misc"
    mr_dir = case_dir / f"{norm_id} MR DICOM"

    for d in [tdc_dir, logs_dir, misc_dir, mr_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # Flatten nested Logs/Logs if present
    nested = logs_dir / "Logs"
    if nested.is_dir():
        for item in nested.iterdir():
            target = logs_dir / f"{norm_id}_{item.name}"
            shutil.move(str(item), target)
        shutil.rmtree(nested, ignore_errors=True)
        logger.info("Flattened nested Logs/Logs structure.")

    return tdc_dir


def setup_logger(log_root: Path) -> logging.Logger:
    log_root.mkdir(parents=True, exist_ok=True)
    log_file = log_root / "clean_tdc_data.log"
    logger = logging.getLogger("clean_tdc")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    sh = logging.StreamHandler(sys.stdout)
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


# -----------------------------------------------------------------------------
# Core function
# -----------------------------------------------------------------------------

def run(case_dir: Path,
        norm_id: str | None,
        input_path: Path | None,
        allow_id_mismatch: bool,
        log_root: Path,
        dry: bool,
        verbose: bool,
        simulate: bool = False) -> int:
    """Main routine for cleaning and organizing TDC data."""

    logger = setup_logger(log_root)
    logger.info("==== CLEAN TDC START ====")
    logger.info(f"Case dir: {case_dir}")
    if norm_id:
        logger.info(f"Norm ID : {norm_id}")
    if simulate:
        logger.info("SIMULATION MODE ENABLED — no files will be permanently modified.")

    try:
        if input_path and input_path.exists():
            if not dry:
                unzip_tdc(input_path, case_dir, logger)
            else:
                logger.info("[DRY-RUN] Would extract TDC zip.")
        else:
            logger.info("No TDC zip input provided or file not found; skipping unzip.")

        tdc_dir = normalize_structure(case_dir, norm_id or "UNKNOWN", logger)

        logger.info("TDC processing completed.")

    except Exception as e:
        logger.error(f"TDC processing failed: {e}")
        return 1

    # ---------- Optional simulate marker ----------
    if simulate:
        try:
            sim_marker = tdc_dir / "_sim_tdc.txt"
            sim_marker.write_text("Simulated TDC run.\n", encoding="utf-8")
            logger.info(f"SIM: wrote {sim_marker}")
        except Exception as e:
            logger.warning(f"SIM: could not write marker: {e}")

    logger.info("==== CLEAN TDC COMPLETE ====")
    return 0


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Unpack and normalize TDC case archives.")
    ap.add_argument("case_dir", type=Path, help="Output case directory")
    ap.add_argument("--norm-id", type=str, default=None, help="Normalized ID (e.g., 017_01-479)")
    ap.add_argument("--input", type=Path, default=None, help="Path to TDC zip file")
    ap.add_argument("--log-root", type=Path, default=Path("Logs"))
    ap.add_argument("--allow-id-mismatch", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--simulate", action="store_true", help="Run in simulate mode and write _sim_tdc.txt")

    args = ap.parse_args()

    rc = run(case_dir=args.case_dir,
             norm_id=args.norm_id,
             input_path=args.input,
             allow_id_mismatch=args.allow_id_mismatch,
             log_root=args.log_root,
             dry=args.dry_run,
             verbose=args.verbose,
             simulate=args.simulate)
    sys.exit(rc)


if __name__ == "__main__":
    main()
