#!/usr/bin/env python3
# PURPOSE: Generate tiny MRI/TDC fixture zips for fast test runs.
# INPUTS: None (writes under tests/fixtures).
# OUTPUTS: tests/fixtures/mri_dummy.zip and tests/fixtures/tdc_dummy.zip.
# NOTES: Idempotent; uses stdlib sqlite3/zipfile.
from __future__ import annotations

import shutil
import sqlite3
import tempfile
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
    if TDC_ZIP.exists():
        TDC_ZIP.unlink()
    TDC_ZIP.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as td:
        tmp_root = Path(td)
        db_path = tmp_root / "local.db"
        _write_min_db(db_path)

        with zipfile.ZipFile(TDC_ZIP, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(
                db_path,
                arcname="TDC Sessions/_TEST_SESSION/local.db",
            )
            zf.writestr(
                "TDC Sessions/_TEST_SESSION/Raw/dummy.txt",
                "raw",
            )
            zf.writestr(
                "TDC Sessions/_TEST_SESSION/2025-01-01--00-00-00/dummy.txt",
                "timestamp",
            )


def main() -> int:
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    _write_mri_fixture()
    _write_tdc_fixture()
    print(f"Wrote MRI fixture: {MRI_ZIP}")
    print(f"Wrote TDC fixture: {TDC_ZIP}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
