#!/usr/bin/env python3
r"""
clean_tdc_data.py  (v1.4.1)
"""

from __future__ import annotations
import argparse
import logging
import shutil
import sys
import zipfile
from pathlib import Path

def setup_logger(log_root: Path) -> logging.Logger:
    log_root.mkdir(parents=True, exist_ok=True)
    log_file = log_root / "clean_tdc_data.log"
    logger = logging.getLogger("clean_tdc")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    sh = logging.StreamHandler(sys.stdout)
    fh.setFormatter(fmt); sh.setFormatter(fmt)
    logger.handlers.clear(); logger.addHandler(fh); logger.addHandler(sh)
    return logger

def unzip_tdc(input_zip: Path, out_dir: Path, logger: logging.Logger) -> None:
    if not input_zip.exists():
        logger.error(f"TDC zip not found: {input_zip}"); sys.exit(2)
    logger.info(f"Unzipping {input_zip.name} ...")
    with zipfile.ZipFile(input_zip, 'r') as zf:
        zf.extractall(out_dir)
    logger.info(f"Extracted to {out_dir}")

def normalize_structure(case_dir: Path, norm_id: str, logger: logging.Logger) -> Path:
    legacy_dirs = []
    if norm_id:
        legacy_dirs = [
            case_dir / f"{norm_id} TDC Sessions",
            case_dir / f"{norm_id} Misc",
            case_dir / f"{norm_id} MR DICOM",
        ]
    if any(p.exists() for p in legacy_dirs):
        logger.warning(
            "Legacy case-prefixed folders exist; using new unprefixed schema under %s",
            case_dir,
        )
    tdc_dir  = case_dir / "TDC Sessions"
    misc_dir = case_dir / "Misc"
    mr_dir   = case_dir / "MR DICOM"
    for d in (tdc_dir, misc_dir, mr_dir):
        d.mkdir(parents=True, exist_ok=True)

    (tdc_dir / "applog" / "Logs").mkdir(parents=True, exist_ok=True)

    nested = None  # no tdc-level logs
    # (deprecated: nested logs)

    if nested and getattr(nested, "is_dir", lambda: False)():
        for item in nested.iterdir():
            target = tdc_logs / item.name
            if target.exists(): target.unlink()
            item.rename(target)
        shutil.rmtree(nested, ignore_errors=True)

    return tdc_dir


def _merge_tdc_applog_to_tdc(case_dir: Path, norm_id: str, logger):
    tdc_dir  = case_dir / "TDC Sessions"
    tdc_logs = tdc_dir / "applog" / "Logs"
    tdc_logs.mkdir(parents=True, exist_ok=True)

    root_logs = case_dir / "Logs"
    if root_logs.exists():
        for p in root_logs.rglob("*"):
            if p.is_file():
                rel = p.relative_to(root_logs)
                dest = tdc_logs / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                try:
                    shutil.move(str(p), str(dest))
                except Exception:
                    try:
                        shutil.copy2(p, dest)
                    except Exception:
                        logger.warning(f"Could not relocate {p} -> {dest}")
        shutil.rmtree(root_logs, ignore_errors=True)
        logger.info("Merged root Logs -> TDC Sessions/applog/Logs")

    root_applog = case_dir / "applog" / "Logs"
    if root_applog.exists():
        for p in root_applog.rglob("*"):
            if p.is_file():
                rel = p.relative_to(root_applog)
                dest = tdc_logs / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                try:
                    shutil.move(str(p), str(dest))
                except Exception:
                    try:
                        shutil.copy2(p, dest)
                    except Exception:
                        logger.warning(f"Could not relocate {p} -> {dest}")
        shutil.rmtree(case_dir / "applog", ignore_errors=True)
        logger.info("Merged root applog/Logs -> TDC Sessions/applog/Logs")


def run(case_dir: Path, norm_id: str | None, input_path: Path | None,
        allow_id_mismatch: bool, log_root: Path, dry: bool, verbose: bool,
        simulate: bool = False) -> int:
    logger = setup_logger(log_root)
    nid = norm_id or "UNKNOWN"
    logger.info("==== CLEAN TDC START ====")
    logger.info(f"Case dir: {case_dir}")
    logger.info(f"Norm ID : {nid}")
    if simulate: logger.info("SIMULATION: enabled")

    try:
        if input_path and input_path.exists():
            if dry: logger.info("[DRY-RUN] Would extract TDC zip.")
            else:   unzip_tdc(input_path, case_dir, logger)
        else:
            logger.info("No TDC zip provided; continuing")

        tdc_dir = normalize_structure(case_dir, nid, logger)
        _merge_tdc_applog_to_tdc(case_dir, nid, logger)
        logger.info("TDC processing completed.")
    except Exception as e:
        logger.error(f"TDC processing failed: {e}")
        return 1

    if simulate:
        try:
            (tdc_dir / "_sim_tdc.txt").write_text("Simulated TDC run.\n", encoding="utf-8")
            logger.info("SIM: wrote _sim_tdc.txt")
        except Exception as e:
            logger.warning(f"SIM: could not write marker: {e}")

    logger.info("==== CLEAN TDC COMPLETE ====")
    return 0

def main():
    ap = argparse.ArgumentParser(description="Unpack and normalize TDC case archives.")
    ap.add_argument("case_dir", type=Path)
    ap.add_argument("--norm-id", type=str, default=None)
    ap.add_argument("--input", type=Path, default=None)
    ap.add_argument("--log-root", type=Path, default=Path("Logs"))
    ap.add_argument("--allow-id-mismatch", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--simulate", action="store_true", help="Run in simulate mode and write _sim_tdc.txt")
    args = ap.parse_args()
    rc = run(args.case_dir, args.norm_id, args.input, args.allow_id_mismatch, args.log_root, args.dry_run, args.verbose, args.simulate)
    sys.exit(rc)

if __name__ == "__main__":
    main()
