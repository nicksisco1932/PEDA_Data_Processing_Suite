#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
anonymize_dicom.py  (v2.0.1, 2025-09-30)

Robust, modular DICOM metadata updater using pydicom.

Key points:
- No hard-coded roots. Caller must provide paths (via module call or CLI).
- Two modes:
    A) Module API: run_anonymize(...), preferred for programmatic use.
    B) CLI: can target either <root>/<site_id> or a direct --site-dir.
- Dry-run by default; --apply to write. Optional .bak backup.
- Atomic writes; JSONL + optional CSV audit; logs under <logs_root>\Logs.
- Extra fields are optional; invalid VRs are skipped (no crash).

Usage (CLI examples):

  # Direct folder mode (recommended):
  python anonymize_dicom.py --site-dir "D:\\staging\\017-01_474" --birthdate 19600101 --apply \
    --logs-root "D:\\Data_Clean" --csv-audit

  # Root/site-id mode:
  python anonymize_dicom.py --root "D:\\Data_Clean_Stage" --site-id 017-01_474 --birthdate 19600101 --apply
"""

from __future__ import annotations
import argparse
import datetime as dt
import json
import logging
import os
from pathlib import Path
import shutil
import sys
from typing import Dict, Iterable, List, Tuple, Optional

# -------- Third-party --------
try:
    import pydicom
    from pydicom.errors import InvalidDicomError
except Exception as e:
    print("ERROR: pydicom is required. Install with: pip install pydicom", file=sys.stderr)
    raise

# ========= Defaults / Config =========
DEFAULT_SKIP_SUFFIXES = [
    "Raw.dat",
    "Anatomy.dat",
    "CurrentTemperature.dat",
    "MaximumTemperature.dat",
]

# ======= Tag plans =======

def default_minimal_plan(site_id: str, birthdate_ymd: str) -> Dict[str, str]:
    return {
        "PatientName": site_id,              # (0010,0010)
        "PatientBirthDate": birthdate_ymd,   # (0010,0030) YYYYMMDD
        "StudyID": "1",                      # (0020,0010)
    }

KAL_EXTRAS = {
    "AccessionNumber": "Accession",                 # (0008,0050)
    "InstitutionName": "Institution",               # (0008,0080)
    "InstitutionAddress": "Address",                # (0008,0081)
    "ReferringPhysicianName": "ProfoundMedical",    # (0008,0090)
    "OperatorsName": "PMI",                         # (0008,1070)
    "PatientSex": "M",                              # (0010,0040)
    "PatientAge": "65Y",                            # (0010,1010)
    "PatientSize": "1.8",                           # (0010,1020)
    "PatientWeight": "80",                          # (0010,1030)
    "CountryOfResidence": "Country",                # (0010,2150)
    "EthnicGroup": "Group",                         # (0010,2160)
    "Occupation": "Occupation",                     # (0010,2180)
    "SmokingStatus": " ",                           # (0010,21A0)
    # "PregnancyStatus": "Status",                  # (0010,21C0) → US VR; intentionally omitted
    "PatientReligiousPreference": "ReligiousPreference",  # (0010,21F0)
}

# ========= Utilities =========

def _effective_logs_dir(log_root: Path | None, case_dir: Path) -> Path:
    """
    If log_root is None → <case_dir>/applog/Logs
    If log_root provided:
        - If it already endswith 'Logs', use as-is.
        - Else append 'Logs'.
    """
    if log_root is None:
        return case_dir / "applog" / "Logs"
    return log_root if log_root.name.lower() == "logs" else (log_root / "Logs")

def parse_birthdate_to_ymd(s: str) -> str:
    s = s.strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime(s, fmt).strftime("%Y%m%d")
        except ValueError:
            pass
    raise ValueError(f"Birthdate '{s}' not in YYYYMMDD, YYYY-MM-DD, or MM/DD/YYYY format")

def looks_like_dicom(path: Path) -> bool:
    try:
        with path.open("rb") as f:
            f.seek(128)
            return f.read(4) == b"DICM"
    except Exception:
        return False

def discover_files(root: Path, skip_suffixes: Iterable[str]) -> Iterable[Path]:
    for p in root.rglob("*"):
        if p.is_file() and not any(str(p).endswith(s) for s in skip_suffixes):
            yield p

def setup_loggers(site_id: str, logs_root: Path) -> tuple[logging.Logger, Path, Path, Path]:
    # Normalize logs root: if caller passed ...\Logs use as-is; otherwise append \Logs
    logs_dir = logs_root if logs_root.name.lower() == "logs" else (logs_root / "Logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = logs_dir / f"{site_id}__anonymize_dicom_{ts}.log"
    audit_jsonl = logs_dir / f"{site_id}__anonymize_dicom_{ts}__audit.jsonl"
    audit_csv = logs_dir / f"{site_id}__anonymize_dicom_{ts}__audit.csv"

    logger = logging.getLogger(f"anonymize_dicom[{site_id}]")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    sh = logging.StreamHandler(sys.stdout)
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger, log_path, audit_jsonl, audit_csv

def close_all_handlers(logger: logging.Logger) -> None:
    for h in list(logger.handlers):
        try:
            h.flush()
            h.close()
        finally:
            logger.removeHandler(h)

def atomic_write(ds: pydicom.dataset.FileDataset, target: Path, make_backup: bool) -> None:
    tmp = target.with_suffix(target.suffix + ".tmp")
    ds.save_as(str(tmp))
    if make_backup and target.exists():
        bak = target.with_suffix(target.suffix + ".bak")
        if not bak.exists():
            shutil.copy2(str(target), str(bak))
    os.replace(str(tmp), str(target))

def read_dicom_meta(path: Path) -> Optional[pydicom.dataset.FileDataset]:
    try:
        return pydicom.dcmread(str(path), stop_before_pixels=True, force=True)
    except InvalidDicomError:
        return None
    except Exception:
        return None

def capture_values(ds: pydicom.dataset.FileDataset, keys: Iterable[str]) -> Dict[str, str]:
    return {k: (str(getattr(ds, k, "")) if ds is not None else "") for k in keys}

def apply_tag_plan(ds: pydicom.dataset.FileDataset, plan: Dict[str, str]) -> tuple[bool, Dict[str, tuple[str, str]]]:
    """
    Returns (changed, diff_map) where diff_map[k] = (before, after).
    Skips invalid VR assignments gracefully (logs at INFO).
    """
    changed = False
    diffs: Dict[str, tuple[str, str]] = {}
    for k, v in plan.items():
        before = getattr(ds, k, None)
        before_s = "" if before is None else str(before)
        after_s = "" if v is None else str(v)
        if before_s == after_s:
            continue
        try:
            setattr(ds, k, v)
            diffs[k] = (before_s, after_s)
            changed = True
        except Exception as e:
            logging.getLogger(f"anonymize_dicom[{getattr(ds, 'PatientName', 'unknown')}]").info(f"Skip tag {k}: {e}")
    return changed, diffs

def load_plan_from_json(path: Path) -> Dict[str, str]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Plan JSON must be an object mapping DICOM keywords to values.")
    return {str(k): ("" if v is None else str(v)) for k, v in data.items()}

# ========= Core (module API) =========

def run_anonymize(
    site_id: str,
    birthdate: str,
    site_dir: Path,
    apply: bool,
    backup: bool,
    write_extras: bool,
    plan_json: Optional[Path],
    skip_suffixes: Optional[List[str]],
    csv_audit: bool,
    logs_root: Path,
) -> tuple[int, Dict[str, Path]]:
    """
    Preferred programmatic API. Returns (exit_code, audit_paths_dict).
    exit_code: 0 ok, 1 partial errors, 2 bad inputs
    """
    logger, log_path, audit_jsonl_path, audit_csv_path = setup_loggers(site_id, logs_root)
    try:
        code = _process(
            site_id=site_id,
            birthdate=birthdate,
            site_root=site_dir,
            apply=apply,
            backup=backup,
            write_extras=write_extras,
            plan_json=plan_json,
            skip_suffixes=skip_suffixes or DEFAULT_SKIP_SUFFIXES,
            csv_audit=csv_audit,
            logger=logger,
            audit_jsonl_path=audit_jsonl_path,
            audit_csv_path=audit_csv_path,
        )
    finally:
        close_all_handlers(logger)

    audits = {"log": log_path, "jsonl": audit_jsonl_path}
    if csv_audit:
        audits["csv"] = audit_csv_path
    return code, audits


def _process(
    site_id: str,
    birthdate: str,
    site_root: Path,
    apply: bool,
    backup: bool,
    write_extras: bool,
    plan_json: Optional[Path],
    skip_suffixes: List[str],
    csv_audit: bool,
    logger: logging.Logger,
    audit_jsonl_path: Path,
    audit_csv_path: Path,
) -> int:
    if not site_root.exists():
        logger.error(f"Site folder not found: {site_root}")
        return 2

    # Build TagPlan
    try:
        bd = parse_birthdate_to_ymd(birthdate)
    except ValueError as e:
        logger.error(str(e))
        return 2

    plan: Dict[str, str] = default_minimal_plan(site_id, bd)
    if write_extras:
        plan = {**plan, **KAL_EXTRAS}
    if plan_json:
        try:
            user_plan = load_plan_from_json(plan_json)
            plan.update(user_plan)
            logger.info(f"Loaded plan overrides from JSON: {plan_json}")
        except Exception as e:
            logger.error(f"Failed to load plan JSON: {plan_json} | {e}")
            return 2

    # CSV header
    if csv_audit:
        with audit_csv_path.open("w", encoding="utf-8", newline="") as f:
            cols = ["path", "status", "reason"] + [f"{k}__before" for k in plan] + [f"{k}__after" for k in plan]
            f.write(",".join(cols) + "\n")

    total = scanned = updated = failed = non_dicm = 0
    logger.info(f"Scanning: {site_root}")
    for path in discover_files(site_root, skip_suffixes):
        scanned += 1
        if not looks_like_dicom(path):
            non_dicm += 1
            continue

        ds = read_dicom_meta(path)
        if ds is None:
            _emit_jsonl(audit_jsonl_path, {"path": str(path), "status": "skip", "reason": "Invalid/Unreadable DICOM"})
            continue

        total += 1
        pre = capture_values(ds, plan.keys())
        changed, diffs = apply_tag_plan(ds, plan)
        if not changed:
            _emit_jsonl(audit_jsonl_path, {"path": str(path), "status": "unchanged", "pre": pre})
            if csv_audit:
                _emit_csv(audit_csv_path, path, "unchanged", "", pre, pre)
            continue

        if apply:
            try:
                if not path.exists():
                    raise IOError("Target file missing at write time")
                atomic_write(ds, path, make_backup=backup)
                updated += 1
                _emit_jsonl(audit_jsonl_path, {"path": str(path), "status": "updated", "diffs": diffs})
                if csv_audit:
                    post = capture_values(ds, plan.keys())
                    _emit_csv(audit_csv_path, path, "updated", "", pre, post)
                logger.info(f"Updated: {path}")
            except Exception as e:
                failed += 1
                _emit_jsonl(audit_jsonl_path, {"path": str(path), "status": "error", "error": str(e), "pre": pre})
                if csv_audit:
                    _emit_csv(audit_csv_path, path, "error", str(e), pre, pre)
                logger.error(f"Write error: {path} | {e}")
        else:
            _emit_jsonl(audit_jsonl_path, {"path": str(path), "status": "dry-run-change", "diffs": diffs})
            if csv_audit:
                post = pre.copy()
                for k, (_, after) in diffs.items():
                    post[k] = after
                _emit_csv(audit_csv_path, path, "dry-run-change", "", pre, post)
            logger.info(f"DRY-RUN would update: {path}")

    logger.info(
        f"Done. scanned={scanned} dicom={total} non_dicm={non_dicm} "
        f"updated={updated} failed={failed} apply={apply} extras={write_extras}"
    )
    return 0 if failed == 0 else 1

def _emit_jsonl(path: Path, obj: dict) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def _emit_csv(path: Path, fpath: Path, status: str, reason: str, pre: Dict[str, str], post: Dict[str, str]) -> None:
    line = [str(fpath).replace(",", " "), status, reason.replace(",", " ")]
    for k in pre:
        line.append(pre.get(k, ""))
    for k in post:
        line.append(post.get(k, ""))
    with path.open("a", encoding="utf-8") as f:
        f.write(",".join(line) + "\n")

# ========= CLI =========

def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Update/anonymize DICOM metadata using pydicom.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--site-id", help="Uses <root>/<site_id> as the target directory.")
    g.add_argument("--site-dir", help="Direct path to a site folder containing DICOMs (preferred).")
    ap.add_argument("--birthdate", required=True, help="Birthdate (YYYYMMDD, YYYY-MM-DD, or MM/DD/YYYY)")
    ap.add_argument("--root", default=None, help="Root folder if using --site-id (ignored with --site-dir)")
    ap.add_argument("--logs-root", required=True, help="Root for logs/audits (e.g., D:\\Data_Clean)")
    ap.add_argument("--apply", action="store_true", help="Apply changes (otherwise dry-run)")
    ap.add_argument("--backup", action="store_true", help="Create .bak backups before replacing")
    ap.add_argument("--write-extras", action="store_true", help="Also apply extended fields (invalid VRs are skipped)")
    ap.add_argument("--plan-json", type=str, help="Path to JSON mapping DICOM keywords → values")
    ap.add_argument("--skip-suffix", action="append", default=[], help="Additional filename suffix to skip (repeatable)")
    ap.add_argument("--csv-audit", action="store_true", help="Also write a CSV audit file alongside JSONL")
    ap.add_argument("--simulate", action="store_true", help="Do not write changes; emit _sim_anondicom.txt in the site folder")
    return ap

def main() -> None:
    args = build_arg_parser().parse_args()

    # Resolve target site directory
    if args.site_dir:
        site_dir = Path(args.site_dir).resolve()
        site_id = site_dir.name
    else:
        if not args.root:
            print("ERROR: --root is required when using --site-id", file=sys.stderr)
            sys.exit(2)
        site_dir = (Path(args.root) / args.site_id).resolve()
        site_id = args.site_id

    logs_root = Path(args.logs_root).resolve()
    logs_root.mkdir(parents=True, exist_ok=True)

    plan_json = Path(args.plan_json).resolve() if args.plan_json else None
    skip_suffixes = DEFAULT_SKIP_SUFFIXES + list(args.skip_suffix or [])

    # Simulation flag forces apply=False and writes a site-level marker
    if args.simulate:
        try:
            (site_dir / "_sim_anondicom.txt").write_text("Simulated anonymize_dicom run.\n", encoding="utf-8")
        except Exception:
            pass
    code, audits = run_anonymize(
        site_id=site_id,
        birthdate=args.birthdate,
        site_dir=site_dir,
        apply=(False if args.simulate else args.apply),
        backup=args.backup,
        write_extras=args.write_extras,
        plan_json=plan_json,
        skip_suffixes=skip_suffixes,
        csv_audit=args.csv_audit,
        logs_root=logs_root,
    )
    print("\n".join(f"{k}: {v}" for k, v in audits.items()))
    sys.exit(code)

if __name__ == "__main__":
    main()
