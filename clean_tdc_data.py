#!/usr/bin/env python3
r"""
clean_tdc_data.py  (v1.5.1)

Guarantees (idempotent):
  <CASE>/<ID> TDC Sessions/
  <CASE>/<ID> Misc/
  <CASE>/<ID> MR DICOM/
  <CASE>/applog/Logs/ (all logs here; merges any root 'Logs')
  Moves stray root session folders into "<ID> TDC Sessions/".
"""

from __future__ import annotations
import argparse, logging, shutil, sys, zipfile, re
from pathlib import Path

SESSION_DIR_RE = re.compile(r"^_\d{4}-\d{2}-\d{2}--\d{2}-\d{2}-\d{2}\s+\d+$")

def _canon_logs(log_root: Path) -> Path:
    # Force applog/Logs
    if log_root.name.lower() == "logs" and log_root.parent.name.lower() != "applog":
        log_root = log_root.parent / "applog" / "Logs"
    log_root.mkdir(parents=True, exist_ok=True)
    return log_root

def setup_logger(log_root: Path) -> logging.Logger:
    log_root = _canon_logs(log_root)
    log_file = log_root / "clean_tdc_data.log"
    logger = logging.getLogger("clean_tdc"); logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(log_file, encoding="utf-8"); sh = logging.StreamHandler(sys.stdout)
    fh.setFormatter(fmt); sh.setFormatter(fmt)
    logger.handlers.clear(); logger.addHandler(fh); logger.addHandler(sh)
    return logger

def unzip_tdc(input_zip: Path, out_dir: Path, logger: logging.Logger) -> None:
    logger.info(f"Unzipping {input_zip.name} ...")
    with zipfile.ZipFile(input_zip, 'r') as zf:
        zf.extractall(out_dir)
    logger.info(f"Extracted to {out_dir}")

def _merge_root_logs_to_applog(case_dir: Path, logger: logging.Logger) -> None:
    root_logs = case_dir / "Logs"
    if not root_logs.exists(): return
    dest = case_dir / "applog" / "Logs"
    dest.mkdir(parents=True, exist_ok=True)
    for p in root_logs.rglob("*"):
        if p.is_file():
            rel = p.relative_to(root_logs)
            d = dest / rel
            d.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(p), str(d))
            except Exception:
                shutil.copy2(p, d)
    try:
        shutil.rmtree(root_logs)
    except Exception:
        pass
    logger.info("Merged root 'Logs' into applog/Logs")

def _move_stray_sessions(case_dir: Path, norm_id: str, logger: logging.Logger) -> None:
    tdc_dir = case_dir / f"{norm_id} TDC Sessions"
    tdc_dir.mkdir(parents=True, exist_ok=True)
    for p in case_dir.iterdir():
        if p.is_dir() and SESSION_DIR_RE.match(p.name):
            dest = tdc_dir / p.name
            if dest.resolve() != p.resolve():
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(p), str(dest))
                logger.info(f"Moved session dir → {dest}")

def normalize_structure(case_dir: Path, norm_id: str, logger: logging.Logger) -> None:
    (case_dir / f"{norm_id} TDC Sessions").mkdir(parents=True, exist_ok=True)
    (case_dir / f"{norm_id} Misc").mkdir(parents=True, exist_ok=True)
    (case_dir / f"{norm_id} MR DICOM").mkdir(parents=True, exist_ok=True)
    _merge_root_logs_to_applog(case_dir, logger)
    _move_stray_sessions(case_dir, norm_id, logger)

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
            else: unzip_tdc(input_path, case_dir, logger)
        else:
            logger.info("No TDC zip provided; continuing")
        normalize_structure(case_dir, nid, logger)
        logger.info("TDC processing completed.")
    except Exception as e:
        logger.error(f"TDC processing failed: {e}")
        return 1
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
    ap.add_argument("--simulate", action="store_true", help="Run in simulate mode")
    args = ap.parse_args()
    rc = run(args.case_dir, args.norm_id, args.input, args.allow_id_mismatch, args.log_root, args.dry_run, args.verbose, args.simulate)
    sys.exit(rc)

if __name__ == "__main__":
    main()
