#!/usr/bin/env python3
r"""
clean_tdc_data.py  (v1.1 • ZIP-aware, logging-focused)

- Accepts optional --input pointing to TDC ZIP (or directory).
- If ZIP, extracts to <CASE_DIR>/<norm_id> TDC Sessions/.
- Strict ID by default; --allow-id-mismatch to override.
- Rich logging + summary footer.
"""

from __future__ import annotations
import argparse, logging, sys, re, traceback, shutil, zipfile
from pathlib import Path
from datetime import datetime

# ---------- ID normalization ----------

_CASE_PAT = re.compile(r'(?P<a>\d{3})[-_](?P<b>\d{2})[-_](?P<c>\d{3})')

def extract_norm_id(text: str) -> str | None:
    m = _CASE_PAT.search(text)
    if not m:
        return None
    return f"{m.group('a')}_{m.group('b')}-{m.group('c')}"

# ---------- Logging ----------

def setup_logger(log_root: Path, verbose: bool) -> tuple[logging.Logger, Path]:
    log_root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_root / f"TDC_{ts}.log"
    logger = logging.getLogger("tdc")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8"); fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt)
    sh.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.addHandler(fh); logger.addHandler(sh)
    return logger, log_path

# ---------- Counters ----------

class RunStats:
    def __init__(self) -> None:
        self.dirs_created = 0
        self.files_extracted = 0
        self.files_moved = 0
        self.files_renamed = 0
        self.warnings = 0
        self.errors = 0
        self.alerts: list[str] = []
    def warn(self, _msg: str): self.warnings += 1
    def err(self, _msg: str): self.errors += 1

# ---------- Helpers ----------

def ensure_dir(p: Path, logger: logging.Logger, stats: RunStats, dry: bool=False):
    if p.exists():
        if not p.is_dir():
            logger.warning(f"Exists but not a directory: {p}"); stats.warn("non_dir")
    else:
        logger.info(f"Create dir: {p}")
        if not dry: p.mkdir(parents=True, exist_ok=True)
        stats.dirs_created += 1

def unzip_to(zip_path: Path, dest: Path, logger: logging.Logger, stats: RunStats, dry: bool=False):
    logger.info(f"Extract ZIP: {zip_path} → {dest}")
    ensure_dir(dest, logger, stats, dry)
    if dry:
        return
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(dest)
        stats.files_extracted += len(z.namelist())

# ---------- Core ----------

def run(case_dir: Path, norm_id: str|None, input_path: Path|None, allow_id_mismatch: bool, log_root: Path, dry: bool, verbose: bool) -> int:
    logger, log_path = setup_logger(log_root, verbose)
    stats = RunStats()

    # Resolve norm_id strictly by default
    derived = extract_norm_id(case_dir.name) or extract_norm_id(str(case_dir))
    resolved_id = norm_id or derived
    if not resolved_id:
        msg = "Unable to derive normalized Case ID (NNN_NN-NNN)."
        logger.error(msg); stats.err(msg); return 2

    # Enforce case_dir name (strict unless overridden)
    name_id = extract_norm_id(case_dir.name)
    if name_id and name_id != resolved_id:
        m = f"Case directory ID mismatch: expected {resolved_id}, got {case_dir.name}"
        if allow_id_mismatch:
            logger.warning(m + " (continuing)"); stats.warn(m); stats.alerts.append(m)
        else:
            logger.error(m); stats.err(m); return 2

    tdc_dir = case_dir / f"{resolved_id} TDC Sessions"
    applog = case_dir / "applog"; logs_dir = applog / "Logs"

    logger.info("==== TDC CLEAN START ====")
    logger.info(f"NORM ID     : {resolved_id}")
    logger.info(f"CASE DIR    : {case_dir}")
    logger.info(f"LOG FILE    : {log_path}")
    logger.info(f"TDC SESSIONS: {tdc_dir}")
    logger.info(f"INPUT       : {input_path if input_path else 'N/A'}")
    logger.info(f"DRY RUN     : {dry}")

    # Ensure dirs
    ensure_dir(case_dir, logger, stats, dry)
    ensure_dir(applog, logger, stats, dry)
    ensure_dir(logs_dir, logger, stats, dry)
    ensure_dir(tdc_dir, logger, stats, dry)

    try:
        # ---------- ZIP/FOLDER handling ----------
        if input_path and input_path.exists():
            found = extract_norm_id(input_path.name)
            if found and found != resolved_id:
                m = f"TDC input naming mismatch. Expected {resolved_id}, got {input_path.name}"
                if allow_id_mismatch:
                    logger.warning(m + " (continuing)"); stats.warn(m); stats.alerts.append(m)
                else:
                    logger.error(m); stats.err(m); return 2

            if input_path.is_file() and input_path.suffix.lower() == ".zip":
                unzip_to(input_path, tdc_dir, logger, stats, dry)
            elif input_path.is_dir():
                logger.info(f"Input directory provided; leaving in place: {input_path}")
            else:
                logger.warning(f"Unsupported input type: {input_path}")
                stats.warn("unsupported_input")

        # ---------- Optional hygiene: flatten nested Logs/Logs ----------
        nested_logs_root = logs_dir / "Logs"
        if nested_logs_root.exists():
            logger.warning(f"Found nested Logs/Logs; flattening into {logs_dir}")
            stats.warn("nested_logs")
            if not dry:
                for p in nested_logs_root.glob("*"):
                    try:
                        shutil.move(str(p), str(logs_dir / p.name))
                        stats.files_moved += 1
                    except Exception as e:
                        logger.warning(f"Flatten move failed: {p} → {logs_dir}: {e}")
                        stats.warn("flatten_fail")
                shutil.rmtree(nested_logs_root, ignore_errors=True)

        logger.info("TDC processing completed.")
    except Exception as e:
        logger.error(f"UNEXPECTED ERROR: {e}\n{traceback.format_exc()}")
        stats.err("unexpected_exception")

    # ---------- Summary ----------
    logger.info("==== TDC CLEAN COMPLETE ====")
    sep = "=" * 36
    logger.info(sep)
    logger.info("TDC Summary:")
    logger.info(f"- Case Dir        : {case_dir}")
    logger.info(f"- Norm ID         : {resolved_id}")
    logger.info(f"- TDC Sessions    : {tdc_dir}")
    logger.info(f"- Log Root        : {logs_dir}")
    logger.info(f"- Files extracted : {stats.files_extracted}")
    logger.info(f"- Files moved     : {stats.files_moved}")
    logger.info(f"- Files renamed   : {stats.files_renamed}")
    logger.info(f"- Dirs created    : {stats.dirs_created}")
    logger.info(f"- Warnings        : {stats.warnings}")
    logger.info(f"- Errors          : {stats.errors}")
    if stats.alerts:
        logger.info("")
        logger.info("ALERTS:")
        for a in stats.alerts:
            logger.info(f"  - {a}")
    logger.info(sep)
    logger.info("End of TDC phase. See above for details.")

    return 0 if stats.errors == 0 else 1

# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="Clean/normalize TDC data (ZIP-aware) with rich logging.")
    ap.add_argument("case_dir", help="Canonical case directory (final destination).")
    ap.add_argument("--input", default=None, help="TDC input: ZIP or directory.")
    ap.add_argument("--norm-id", default=None, help="Expected normalized ID (NNN_NN-NNN). If omitted, derive from path.")
    ap.add_argument("--allow-id-mismatch", action="store_true",
                    help="Proceed even if names do not match the derived/expected norm_id. Default is strict.")
    ap.add_argument("--log-root", default=None, help="Where to write logs (default: <case_dir>/applog/Logs).")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    case_dir = Path(args.case_dir).resolve()
    input_path = Path(args.input).resolve() if args.input else None
    logs = Path(args.log_root).resolve() if args.log_root else (case_dir / "applog" / "Logs")

    sys.exit(run(
        case_dir=case_dir,
        norm_id=args.norm_id,
        input_path=input_path,
        allow_id_mismatch=args.allow_id_mismatch,
        log_root=logs,
        dry=args.dry_run,
        verbose=args.verbose,
    ))

if __name__ == "__main__":
    main()
