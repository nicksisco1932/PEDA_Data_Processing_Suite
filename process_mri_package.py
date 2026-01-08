#!/usr/bin/env python3
from __future__ import annotations
import argparse, logging, sys, shutil, re
from pathlib import Path

CASE_RE = re.compile(r"(\d{3})[-_](\d{2})[-_](\d{3,})")

def setup_logger(root: Path):
    logger = logging.getLogger("process_mri_package"); logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout); h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.handlers.clear(); logger.addHandler(h); return logger

def _extract_norm_id(name: str) -> str | None:
    m = CASE_RE.search(Path(name).name)
    if not m:
        return None
    g1, g2, g3 = m.group(1), m.group(2), m.group(3)
    return f"{g1}_{g2}-{g3}"

def extract_norm_id(name: str) -> str | None:
    return _extract_norm_id(name)

def run(*, input: Path, birthdate: str, out_root: Path, logs_root: Path, apply: bool, simulate: bool) -> int:
    logger = setup_logger(logs_root)
    if simulate:
        (out_root / "SIM_MARKER.txt").write_text("sim\n", encoding="utf-8")
        logger.info("SIM: wrote SIM_MARKER.txt")
    case_id = _extract_norm_id(input.name)
    if not case_id:
        logger.error("Could not infer case ID from input zip name.")
        return 2

    case_dir = out_root / case_id
    mr_dir = case_dir / f"{case_id} MR DICOM"
    mr_dir.mkdir(parents=True, exist_ok=True)

    if apply:
        dest_zip = mr_dir / f"{case_id}_MRI.zip"
        shutil.copy2(input, dest_zip)
        logger.info(f"Wrote MRI zip: {dest_zip}")
    else:
        logger.info("Dry/preview mode: not copying MRI zip.")

    logger.info("MRI step complete.")
    return 0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--birthdate", type=str, required=True)
    ap.add_argument("--out-root", type=Path, required=True)
    ap.add_argument("--logs-root", type=Path, required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--simulate", action="store_true")
    args = ap.parse_args()
    return run(input=args.input, birthdate=args.birthdate, out_root=args.out_root, logs_root=args.logs_root, apply=bool(args.apply), simulate=bool(args.simulate))

if __name__ == "__main__":
    sys.exit(main())
