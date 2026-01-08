#!/usr/bin/env python3
from __future__ import annotations
import argparse, logging, sys, re, shutil
from pathlib import Path

CASE_RE = re.compile(r"[0-9]{3}[-_][0-9]{2}[-_][0-9]{3,}")

def setup_logger(root: Path|None, name="master"):
    logger = logging.getLogger(name); logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout); h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.handlers.clear(); logger.addHandler(h); return logger

def infer_case_dir(args):
    for attr in ("mri_input","mri_dir","pdf_input","tdc_input"):
        val = getattr(args, attr, None)
        if not val: continue
        p = Path(val).parent
        for part in reversed(p.parts):
            if CASE_RE.fullmatch(part): return str(Path(*p.parts[:p.parts.index(part)+1]))
    return None

def case_id_from_path(case_dir: Path) -> str:
    name = case_dir.name
    m = CASE_RE.search(name)
    return m.group(0) if m else name

def normalize_case_id(text: str) -> str | None:
    m = CASE_RE.search(text)
    if not m:
        return None
    parts = re.split(r"[-_]", m.group(0))
    if len(parts) < 3:
        return m.group(0)
    return f"{parts[0]}_{parts[1]}-{parts[2]}"

def run_tdc_clean(case_dir: Path, log_root: Path, allow_id_mismatch: bool, dry: bool, verbose: bool, simulate: bool, logger: logging.Logger, args_ref=None):
    import clean_tdc_data as tdc
    return tdc.run(case_dir,
        norm_id=case_id_from_path(case_dir),
        input_path=(Path(args_ref.tdc_input) if (args_ref and getattr(args_ref, 'tdc_input', None)) else None),
        allow_id_mismatch=allow_id_mismatch,
        log_root=log_root,
        dry=dry,
        verbose=verbose,
        simulate=simulate)

def run_mri_process(mri_input, birthdate, out_root, logs_root, apply, simulate, logger):
    if not mri_input: return 0
    import process_mri_package as mri
    if hasattr(mri, "run"):
        return mri.run(input=mri_input, birthdate=birthdate, out_root=out_root, logs_root=logs_root, apply=apply, simulate=simulate)
    return 0

def build_parser():
    ap = argparse.ArgumentParser()
    ap.add_argument("case_dir", nargs="?", default=None)
    ap.add_argument("--out-root", default=None)
    ap.add_argument("--tdc-input", default=None)
    ap.add_argument("--mri-input", default=None)
    ap.add_argument("--mri-dir", default=None)
    ap.add_argument("--pdf-input", default=None)
    ap.add_argument("--patient-birthdate", default=None)
    ap.add_argument("--mri-apply", dest="mri_apply", action="store_true")
    ap.add_argument("--simulate-mri", action="store_true")
    ap.add_argument("--simulate-peda", action="store_true")
    ap.add_argument("--skip-tdc", action="store_true")
    ap.add_argument("--simulate-tdc", action="store_true")
    ap.add_argument("--skip-mri", action="store_true")
    ap.add_argument("--skip-anonymize-localdb", action="store_true")
    ap.add_argument("--log-root", default=None)
    ap.add_argument("--dry-run", dest="dry", action="store_true")
    return ap

def main():
    ap = build_parser(); args = ap.parse_args()
    if args.case_dir and Path(args.case_dir).is_file():
        if not args.tdc_input:
            args.tdc_input = args.case_dir
        inferred_id = normalize_case_id(Path(args.case_dir).name)
        if not inferred_id:
            ap.error("case_dir required or inferable from inputs.")
        if args.out_root:
            args.case_dir = str(Path(args.out_root) / inferred_id)
        else:
            args.case_dir = str(Path(args.case_dir).parent / inferred_id)

    if args.case_dir is None:
        args.case_dir = infer_case_dir(args)
        if not args.case_dir:
            ap.error("case_dir required or inferable from inputs.")
    case_dir = Path(args.case_dir)
    out_root = Path(args.out_root) if args.out_root else case_dir.parent
    case_id = normalize_case_id(case_dir.name) or case_id_from_path(case_dir)
    default_log_root = out_root / case_id / f"{case_id} TDC Sessions" / "applog" / "Logs"
    log_root = Path(args.log_root) if args.log_root else default_log_root
    logger = setup_logger(log_root)
    logger.info(f"Case dir: {case_dir}"); logger.info(f"Out root: {out_root}"); logger.info(f"Log root: {log_root}")
    if not args.skip_tdc:
        rc = run_tdc_clean(case_dir, log_root, False, args.dry, False, args.simulate_tdc, logger, args_ref=args)
        if rc != 0: sys.exit(rc)
    if not args.skip_mri:
        rc = run_mri_process(Path(args.mri_input) if args.mri_input else None, args.patient_birthdate, out_root, log_root, bool(args.mri_apply), bool(args.simulate_mri), logger)
        if rc != 0: sys.exit(rc)

    if args.pdf_input:
        misc_dir = case_dir / f"{case_id} Misc"
        misc_dir.mkdir(parents=True, exist_ok=True)
        src = Path(args.pdf_input)
        dest = misc_dir / f"{case_id}_TreatmentReport.pdf"
        try:
            shutil.copy2(src, dest)
            logger.info(f"PDF normalized: {dest}")
        except Exception as e:
            logger.warning(f"Could not normalize PDF: {e}")
    logger.info("All steps complete."); return 0

if __name__ == "__main__":
    sys.exit(main())
