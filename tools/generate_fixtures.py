#!/usr/bin/env python3
from __future__ import annotations

import shutil
import sqlite3
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = ROOT / "tests" / "fixtures"
MRI_ZIP = FIXTURES_DIR / "mri_dummy.zip"
TDC_ZIP = FIXTURES_DIR / "tdc_dummy.zip"


def _zip_dir(src_dir: Path, dest_zip: Path) -> None:
    if dest_zip.exists():
        dest_zip.unlink()
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dest_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in src_dir.rglob("*"):
            if path.is_file():
                zf.write(path, arcname=str(path.relative_to(src_dir)))


def _write_mri_fixture() -> None:
    staging = FIXTURES_DIR / "_mri_tmp"
    if staging.exists():
        for p in staging.rglob("*"):
            if p.is_file():
                p.unlink()
        for p in sorted(staging.rglob("*"), reverse=True):
            if p.is_dir():
                p.rmdir()
    (staging / "DICOM").mkdir(parents=True, exist_ok=True)
    (staging / "DICOM" / "dummy.dcm").write_text("DUMMY_DICOM", encoding="utf-8")
    _zip_dir(staging, MRI_ZIP)
    shutil.rmtree(staging, ignore_errors=True)


def _write_min_db(path: Path) -> None:
    if path.exists():
        path.unlink()
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS patients (id INTEGER, name TEXT)")
    cur.execute("INSERT INTO patients (id, name) VALUES (?, ?)", (1, "TEST"))
    con.commit()
    con.close()


def _write_tdc_fixture() -> None:
    staging = FIXTURES_DIR / "_tdc_tmp"
    if staging.exists():
        for p in staging.rglob("*"):
            if p.is_file():
                p.unlink()
        for p in sorted(staging.rglob("*"), reverse=True):
            if p.is_dir():
                p.rmdir()
    session_root = staging / "TDC Sessions" / "_TEST_SESSION"
    (session_root / "Raw").mkdir(parents=True, exist_ok=True)
    (session_root / "Raw" / "dummy.txt").write_text("raw", encoding="utf-8")
    (session_root / "2025-01-01--00-00-00").mkdir(parents=True, exist_ok=True)
    (session_root / "2025-01-01--00-00-00" / "dummy.txt").write_text(
        "timestamp", encoding="utf-8"
    )
    _write_min_db(session_root / "local.db")
    _zip_dir(staging, TDC_ZIP)
    shutil.rmtree(staging, ignore_errors=True)


def main() -> int:
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    _write_mri_fixture()
    _write_tdc_fixture()
    print(f"Wrote MRI fixture: {MRI_ZIP}")
    print(f"Wrote TDC fixture: {TDC_ZIP}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
