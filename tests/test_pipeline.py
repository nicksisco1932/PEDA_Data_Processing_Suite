import os
import sys
import zipfile
import subprocess
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]

def run_py(script, *args):
    cmd = [sys.executable, str(SCRIPTS_DIR / script), *map(str, args)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr

def make_zip(path: Path, entries: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in entries.items():
            zf.writestr(name, content)

def test_extract_norm_id_from_input_zip():
    sys.path.insert(0, str(SCRIPTS_DIR))
    import process_mri_package as mri  # type: ignore
    assert mri._extract_norm_id("MRI-017-01_479.zip") == "017_01-479"
    assert mri._extract_norm_id("abc 123") is None

def test_structure_guard_fixes_layout(tmp_path: Path):
    case_id = "017_01-479"
    case_dir = tmp_path / case_id
    case_dir.mkdir(parents=True)

    (case_dir / "_2025-08-12--12-02-57 1255595998").mkdir()
    (case_dir / "Logs").mkdir()
    (case_dir / "Logs" / "orphan.log").write_text("hi")
    (case_dir / f"{case_id}_MRI.zip").write_text("zipbytes")
    (case_dir / "weird.PDF.PDF").write_text("pdfbytes")

    rc, out, err = run_py("structure_guard.py", str(case_dir), "--id", case_id, "--fix")
    assert rc == 0, f"guard failed: {out}\n{err}"

    assert (case_dir / f"{case_id} TDC Sessions").exists()
    assert (case_dir / "applog" / "Logs" / "orphan.log").exists()
    assert (case_dir / f"{case_id} MR DICOM" / f"{case_id}_MRI.zip").exists()
    assert (case_dir / f"{case_id} Misc" / f"{case_id}_TreatmentReport.pdf").exists()

def test_process_mri_autodetects_case_dir_and_id(tmp_path: Path):
    out_root = tmp_path
    case_id = "017_01-478"
    src_zip = tmp_path / f"MRI-{case_id.replace('_','-')}.zip"
    make_zip(src_zip, {"DICOM/IMG1": "x"})

    rc, out, err = run_py("process_mri_package.py",
                          "--input", src_zip,
                          "--out-root", out_root)
    assert rc == 0, f"process_mri failed: {out}\n{err}"

    case_dir = out_root / case_id
    dst_zip = case_dir / f"{case_id} MR DICOM" / f"{case_id}_MRI.zip"
    assert dst_zip.exists(), "MRI zip not placed in canonical location"

def test_master_run_end_to_end_smoke(tmp_path: Path):
    case_id = "017_01-477"
    out_root = tmp_path / "Data_Clean"
    out_root.mkdir()

    tdc_zip = tmp_path / f"{case_id.replace('_','-')}_TDC.zip"
    with zipfile.ZipFile(tdc_zip, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"_2025-08-12--12-02-57 999999999/local.db", "sqlite-bytes")
        z.writestr(f"Logs/tdc.log", "hello")

    mri_zip = tmp_path / f"MRI-{case_id.replace('_','-')}.zip"
    with zipfile.ZipFile(mri_zip, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("DICOM/IMG1", "x")

    bad_pdf = tmp_path / f"{case_id}_TreatmentReport.pdf.pdf"
    bad_pdf.write_text("pdf")

    rc, out, err = run_py("master_run.py",
                          tdc_zip,
                          "--mri-input", mri_zip,
                          "--pdf-input", bad_pdf,
                          "--patient-birthdate", "19000101",
                          "--mri-apply",
                          "--out-root", out_root,
                          "--simulate-peda",
                          "--skip-anonymize-localdb")
    assert rc == 0, f"master_run failed: {out}\n{err}"

    case_dir = out_root / case_id
    assert (case_dir / f"{case_id} TDC Sessions").exists()
    assert (case_dir / f"{case_id} MR DICOM" / f"{case_id}_MRI.zip").exists()
    assert (case_dir / f"{case_id} Misc" / f"{case_id}_TreatmentReport.pdf").exists()
    assert (case_dir / "applog" / "Logs").exists()
