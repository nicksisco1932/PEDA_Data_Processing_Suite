#!/usr/bin/env python3
# structure_guard.py (v0.2a ASCII)
from __future__ import annotations
import argparse, re, shutil, sys
from pathlib import Path

SESSION_DIR_RE = re.compile(r"^_\d{4}-\d{2}-\d{2}--\d{2}-\d{2}-\d{2}\s+\d+$")

def _ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def verify(case_root: Path, case_id: str) -> list[str]:
    errs: list[str] = []
    misc = case_root / f"{case_id} Misc"
    mr   = case_root / f"{case_id} MR DICOM"
    tdc  = case_root / f"{case_id} TDC Sessions"
    applog = case_root / "applog"
    logs = applog / "Logs"
    for d in [misc, mr, tdc, applog, logs]:
        if not d.exists():
            errs.append(f"MISSING: {d}")
    pdf = misc / f"{case_id}_TreatmentReport.pdf"
    if not pdf.exists():
        found = list(misc.glob(f"{case_id}_TreatmentReport.pdf*"))
        if not found:
            errs.append(f"PDF not normalized: expected {pdf.name} in {misc}")
    if not (mr / f"{case_id}_MRI.zip").exists():
        errs.append(f"MRI zip missing: {mr / (case_id + '_MRI.zip')}")
    stray_sessions = [p for p in case_root.iterdir() if p.is_dir() and SESSION_DIR_RE.match(p.name)]
    if stray_sessions:
        errs.append("Stray session dirs at root: " + ", ".join(p.name for p in stray_sessions))
    root_logs = case_root / "Logs"
    if root_logs.exists():
        errs.append("Root 'Logs' should be under applog/Logs")
    return errs

def fix(case_root: Path, case_id: str) -> list[str]:
    changes: list[str] = []
    misc = _ensure_dir(case_root / f"{case_id} Misc")
    mr   = _ensure_dir(case_root / f"{case_id} MR DICOM")
    tdc  = _ensure_dir(case_root / f"{case_id} TDC Sessions")
    applog = _ensure_dir(case_root / "applog")
    logs = _ensure_dir(applog / "Logs")
    for p in case_root.iterdir():
        if p.is_dir() and SESSION_DIR_RE.match(p.name):
            dest = tdc / p.name
            if dest.resolve() != p.resolve():
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(p), str(dest))
                changes.append(f"MOVED session dir -> {dest}")
    root_logs = case_root / "Logs"
    if root_logs.exists():
        for item in root_logs.rglob("*"):
            if item.is_file():
                rel = item.relative_to(root_logs)
                dst = logs / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                try:
                    shutil.move(str(item), str(dst))
                except Exception:
                    shutil.copy2(item, dst)
        try:
            shutil.rmtree(root_logs)
            changes.append("MERGED root Logs -> applog/Logs")
        except Exception:
            pass
    cand = None
    for p in case_root.rglob("*.pdf"):
        n = p.name.lower()
        if "treatment" in n and "report" in n:
            cand = p; break
    if cand:
        target = misc / f"{case_id}_TreatmentReport.pdf"
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            try:
                shutil.move(str(cand), str(target))
                changes.append(f"MOVED PDF -> {target}")
            except Exception:
                pass
    for p in case_root.rglob(f"{case_id}_MRI.zip"):
        if p.parent.resolve() != mr.resolve():
            dst = mr / p.name
            dst.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(p), str(dst))
                changes.append(f"MOVED MRI zip -> {dst}")
            except Exception:
                pass
    dcm = mr / "DICOM"
    if dcm.exists():
        try:
            next(dcm.iterdir())
        except StopIteration:
            try:
                dcm.rmdir()
                changes.append("REMOVED empty MR DICOM\\DICOM")
            except Exception:
                pass
    return changes

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("case_root", type=Path)
    ap.add_argument("--id", required=True)
    ap.add_argument("--fix", action="store_true")
    args = ap.parse_args()
    errs = verify(args.case_root, args.id)
    if errs:
        print("FAIL: Layout issues detected:")
        for e in errs:
            print(" -", e)
        if args.fix:
            print("Attempting to fix...")
            changes = fix(args.case_root, args.id)
            if changes:
                print("Applied changes:")
                for c in changes:
                    print(" -", c)
            else:
                print("No changes applied.")
            errs2 = verify(args.case_root, args.id)
            if errs2:
                print("Remaining issues:")
                for e in errs2:
                    print(" -", e)
                sys.exit(1)
            else:
                print("OK: Layout is now canonical.")
                sys.exit(0)
        else:
            sys.exit(1)
    else:
        print("OK: Layout is canonical.")
        sys.exit(0)

if __name__ == "__main__":
    main()
