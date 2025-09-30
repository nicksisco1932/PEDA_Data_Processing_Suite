#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
process_mri_package.py  (v2.1, 2025-09-29)

End-to-end pipeline:
1) Accepts a ZIP (usual) or directory containing MRI DICOMs.
2) Stages to a temp folder.
3) Calls anonymizer (module call) on the staged files.
4) Normalizes a single top folder: "<norm_id> MR DICOM".
5) Packages to: <out_root>\<norm_id>\<norm_id> MR DICOM\<norm_id>_MRI.zip
   where norm_id swaps the first '-' and '_' of the canonical site_id
   e.g., "017-01_474" -> "017_01-474"
6) Writes orchestration + anonymizer logs under <logs_root>\Logs
   (logs_root defaults to out_root)

Usage:
  python process_mri_package.py --input "D:\\...\\mri-017-01_474.zip" --birthdate 19600101 \
    --out-root "D:\\Data_Clean" --apply --backup --csv-audit
"""

from __future__ import annotations
import argparse
import datetime as dt
import logging
import os
from pathlib import Path
import shutil
import sys
import tempfile
import zipfile
from typing import Tuple, Optional

import anonymize_dicom as ad


def setup_logger(logs_root: Path) -> Tuple[logging.Logger, Path]:
    logs_dir = logs_root / "Logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = logs_dir / f"process_mri_package_{ts}.log"
    logger = logging.getLogger("process_mri")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    sh = logging.StreamHandler(sys.stdout)
    fh.setFormatter(fmt); sh.setFormatter(fmt)
    logger.addHandler(fh); logger.addHandler(sh)
    logger.info(f"Log file: {log_path}")
    return logger, log_path

def close_all_handlers(logger: logging.Logger) -> None:
    for h in list(logger.handlers):
        try:
            h.flush(); h.close()
        finally:
            logger.removeHandler(h)

def parse_site_id_from_any(p: Path) -> str:
    stem = p.stem if p.is_file() else p.name
    for pref in ("mri-", "MRI-", "mri_", "MRI_"):
        if stem.startswith(pref):
            stem = stem[len(pref):]
            break
    for suf in ("_MRI", "-MRI", "_mri", "-mri"):
        if stem.endswith(suf):
            stem = stem[: -len(suf)]
            break
    return stem

def normalize_id(site_id: str) -> str:
    # swap first '-' with '_' and first '_' with '-'
    return site_id.replace("-", "TMP", 1).replace("_", "-", 1).replace("TMP", "_", 1)

def ensure_top_folder(staging_dir: Path, desired_top: str) -> Path:
    items = [p for p in staging_dir.iterdir()]
    target = staging_dir / desired_top
    if len(items) == 1 and items[0].is_dir() and items[0].name == desired_top:
        return items[0]
    target.mkdir(exist_ok=True)
    for p in items:
        if p == target: continue
        shutil.move(str(p), str(target / p.name))
    return target

def zip_directory(src_top_dir: Path, zip_path: Path) -> None:
    if zip_path.exists():
        zip_path.unlink()
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    base = src_top_dir.parent
    with zipfile.ZipFile(str(zip_path), "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(src_top_dir):
            root_path = Path(root)
            for fn in files:
                fp = root_path / fn
                arc = fp.relative_to(base)
                zf.write(str(fp), arcname=str(arc))

def process_package(
    input_path: Path,
    birthdate: str,
    out_root: Path,
    logs_root: Path,
    apply: bool,
    backup: bool,
    write_extras: bool,
    csv_audit: bool,
    site_id_override: Optional[str],
    logger: logging.Logger,
) -> Path:
    if not input_path.exists():
        raise FileNotFoundError(f"Input not found: {input_path}")

    site_id = site_id_override or parse_site_id_from_any(input_path)  # canonical (for DICOM tags)
    norm_id = normalize_id(site_id)                                   # for output paths
    top_folder_name = f"{norm_id} MR DICOM"
    out_dir = (out_root / norm_id / top_folder_name)
    out_zip = out_dir / f"{norm_id}_MRI.zip"

    logger.info(f"Canonical site_id: {site_id} | Output norm_id: {norm_id}")
    logger.info(f"Output ZIP: {out_zip}")
    out_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix=f"mri_stage_{norm_id}_") as tmp:
        stage = Path(tmp)
        stage_site = stage / site_id  # keep canonical for anonymizer
        stage_site.mkdir(parents=True, exist_ok=True)

        if input_path.is_file() and input_path.suffix.lower() == ".zip":
            logger.info("Unzipping input...")
            with zipfile.ZipFile(str(input_path), "r") as zf:
                zf.extractall(str(stage_site))
        elif input_path.is_dir():
            logger.info("Copying directory input to staging...")
            for item in input_path.iterdir():
                dest = stage_site / item.name
                if item.is_dir(): shutil.copytree(str(item), str(dest))
                else: shutil.copy2(str(item), str(dest))
        else:
            raise ValueError("Input must be a .zip file or a directory.")

        logger.info("Running metadata anonymization...")
        code, audits = ad.run_anonymize(
            site_id=site_id,
            birthdate=birthdate,
            site_dir=stage_site,
            apply=apply,
            backup=backup,
            write_extras=write_extras,
            plan_json=None,
            skip_suffixes=None,
            csv_audit=csv_audit,
            logs_root=logs_root,
        )
        if code == 2:
            raise RuntimeError("Anonymizer failed due to bad inputs.")
        if code == 1:
            logger.warning("Anonymizer reported partial errors; see audit logs.")
        logger.info(f"Anonymizer audit: {audits}")

        logger.info("Normalizing folder structure...")
        top_dir = ensure_top_folder(stage_site, top_folder_name)

        logger.info("Creating final ZIP...")
        zip_directory(src_top_dir=top_dir, zip_path=out_zip)

        logger.info("Packaging complete.")
        return out_zip

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Unzip/copy MRI package, anonymize metadata, and rezip to standardized name and folder layout."
    )
    ap.add_argument("--input", required=True, help="Path to ZIP or directory (MRI package)")
    ap.add_argument("--birthdate", required=True, help="YYYYMMDD, YYYY-MM-DD, or MM/DD/YYYY")
    ap.add_argument("--out-root", required=True, help="Output root where the site folder will be created")
    ap.add_argument("--logs-root", default=None, help="Root for logs/audits (default: same as --out-root)")
    ap.add_argument("--apply", action="store_true", help="Apply metadata changes (otherwise dry-run)")
    ap.add_argument("--backup", action="store_true", help="Create .bak backups during anonymization")
    ap.add_argument("--write-extras", action="store_true", help="Apply extended fields (invalid VRs are skipped)")
    ap.add_argument("--csv-audit", action="store_true", help="Also emit a CSV audit from the anonymizer")
    ap.add_argument("--site-id", default=None, help="Override site id parsed from input")
    return ap

def main() -> None:
    args = build_parser().parse_args()
    out_root = Path(args.out_root).resolve()
    logs_root = Path(args.logs_root).resolve() if args.logs_root else out_root
    out_root.mkdir(parents=True, exist_ok=True)
    logs_root.mkdir(parents=True, exist_ok=True)

    logger, _ = setup_logger(logs_root)
    try:
        out_zip = process_package(
            input_path=Path(args.input),
            birthdate=args.birthdate,
            out_root=out_root,
            logs_root=logs_root,
            apply=args.apply,
            backup=args.backup,
            write_extras=args.write_extras,
            csv_audit=args.csv_audit,
            site_id_override=args.site_id,
            logger=logger,
        )
        logger.info(f"Output ZIP: {out_zip}")
        print(str(out_zip))
    except Exception as e:
        logger.error(f"FAILED: {e}")
        sys.exit(1)
    finally:
        close_all_handlers(logger)

if __name__ == "__main__":
    main()
