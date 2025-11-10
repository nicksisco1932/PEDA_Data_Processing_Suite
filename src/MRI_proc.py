# MRI_proc.py
from pathlib import Path
import sys, shutil, zipfile, tempfile, os

def fail(msg, code=1):
    print(f"[ERROR] {msg}", file=sys.stderr); sys.exit(code)

def _zip_dir(src_dir: Path, dest_zip: Path):
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dest_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(src_dir):
            rp = Path(root)
            for name in files:
                p = rp / name
                zf.write(p, arcname=str(p.relative_to(src_dir)))

def run(*, root: Path, case: str, input_zip: Path, scratch: Path):
    case_dir = root / case
    mr_dir = case_dir / f"{case} MR DICOM"

    if not input_zip.exists() or not input_zip.is_file() or input_zip.suffix.lower() != ".zip":
        fail(f"MRI input not found or not .zip: {input_zip}", 2)

    print(f"📦 MRI input: {input_zip}")

    # 1) backup into scratch
    bak = scratch / (input_zip.name + ".bak")
    shutil.copy2(input_zip, bak)
    print(f"🗄️  MRI backup: {bak}")

    # 2) unzip -> (future anonymize) -> rezip into scratch/MRI_anonymized.zip
    with tempfile.TemporaryDirectory(dir=scratch, prefix="mri_unzipped_") as tmpdir:
        tmp = Path(tmpdir)
        with zipfile.ZipFile(bak, "r") as zf:
            zf.extractall(tmp)
        print(f"📥 MRI extracted → {tmp}")

        # (anonymization would happen here later, operating on files under `tmp`)

        out_zip = scratch / "MRI_anonymized.zip"
        if out_zip.exists(): out_zip.unlink()
        _zip_dir(tmp, out_zip)
        print(f"📤 MRI repacked → {out_zip}")

    # 3) copy final to case MR folder as <case>_MRI.zip
    final_zip = mr_dir / f"{case}_MRI.zip"
    final_zip.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(out_zip, final_zip)
    print(f"✅ MRI final → {final_zip}")

    return {"backup": bak, "scratch_zip": out_zip, "final_zip": final_zip}
