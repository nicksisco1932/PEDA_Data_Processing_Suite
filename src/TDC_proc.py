# TDC_proc.py
from pathlib import Path
import sys, shutil, zipfile, tempfile
from datetime import datetime
from localdb_anon import anonymize_in_place  # your utility

def fail(msg, code=1):
    print(f"[ERROR] {msg}", file=sys.stderr); sys.exit(code)

def _is_local_db(p: Path) -> bool:
    return p.name.lower() == "local.db" and p.is_file()

def _zip_dir(src_dir: Path, dest_zip: Path):
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dest_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(src_dir):
            rp = Path(root)
            for name in files:
                p = rp / name
                zf.write(p, arcname=str(p.relative_to(src_dir)))

def _first_session_dir(root: Path) -> Path | None:
    # Prefer a dir starting with '_' and containing a local.db
    candidates = [d for d in root.iterdir() if d.is_dir()]
    # 1) with local.db
    for d in candidates:
        if d.name.startswith("_") and any(_is_local_db(p) for p in d.iterdir()):
            return d
    # 2) any starting with '_'
    for d in candidates:
        if d.name.startswith("_"):
            return d
    # 3) fallback: any dir
    return candidates[0] if candidates else None

def run(*, root: Path, case: str, input_zip: Path, scratch: Path, date_shift_days: int = 137):
    case_dir = root / case
    tdc_dir  = case_dir / f"{case} TDC Sessions"
    misc_dir = case_dir / f"{case} Misc"

    if not input_zip.exists() or not input_zip.is_file() or input_zip.suffix.lower() != ".zip":
        fail(f"TDC input not found or not .zip: {input_zip}", 2)

    print(f"📦 TDC input: {input_zip}")

    # 1) backup in scratch
    bak = scratch / (input_zip.name + ".bak")
    shutil.copy2(input_zip, bak)
    print(f"🗄️  TDC backup: {bak}")

    # 2) unzip -> temp
    with tempfile.TemporaryDirectory(dir=scratch, prefix="tdc_unzipped_") as tmpdir:
        tmp = Path(tmpdir)
        with zipfile.ZipFile(bak, "r") as zf:
            zf.extractall(tmp)
        print(f"📥 TDC extracted → {tmp}")

        # 3) copy Logs -> <case> Misc\Logs (if present)
        logs_src = tmp / "Logs"
        if logs_src.exists() and logs_src.is_dir():
            target_logs = misc_dir / "Logs"
            # avoid overwrite on re-runs
            n, final_logs = 1, target_logs
            while final_logs.exists():
                final_logs = misc_dir / (f"Logs__{n}")
                n += 1
            final_logs.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(logs_src, final_logs)
            print(f"📝 Copied Logs → {final_logs}")
        else:
            print("ℹ️  No Logs/ folder in TDC input (skipping).")

        # 4) find the session directory (e.g., _2025-10-02--06-40-14 1742318679)
        session_dir = _first_session_dir(tmp)
        if session_dir is None:
            fail("No session directory found in TDC archive", 2)
        session_name = session_dir.name
        print(f"🧩 Session: {session_name}")

        # 5) stage destination in scratch
        staged_session = scratch / "TDC_staged" / session_name
        if staged_session.exists():
            shutil.rmtree(staged_session, ignore_errors=True)
        staged_session.mkdir(parents=True, exist_ok=True)

        # 6) process contents of the session
        # - anonymize/copy local.db
        # - zip each top-level subdir in session into staged_session/<subdir>.zip
        # - copy any zip files already present
        local_db_src = None
        for child in session_dir.iterdir():
            if child.is_dir():
                # make <child.name>.zip in staged_session
                dest_zip = staged_session / f"{child.name}.zip"
                _zip_dir(child, dest_zip)
                print(f"📦 Packed {child.name} → {dest_zip}")
            elif child.suffix.lower() == ".zip":
                shutil.copy2(child, staged_session / child.name)
                print(f"➡️  Copied zip → {staged_session / child.name}")
            elif _is_local_db(child):
                local_db_src = child
            else:
                # ignore other files
                pass

        if local_db_src is None:
            # try nested search as fallback
            local_db_src = next((p for p in session_dir.rglob("*") if _is_local_db(p)), None)
        if local_db_src is None:
            fail("local.db not found inside session directory", 2)

        staged_db = staged_session / "local.db"
        shutil.copy2(local_db_src, staged_db)
        # 7) anonymize IN PLACE
        summary = anonymize_in_place(staged_db, date_shift_days=date_shift_days, make_temp_proof=False)
        print(f"🔒 Anonymized local.db (tables: {len(summary.get('tables', []))})")

    # 8) copy staged session to final case tree
    target = tdc_dir / session_name
    n = 1
    while target.exists():  # avoid collisions on re-run
        target = tdc_dir / f"{session_name}__{n}"
        n += 1
    shutil.copytree(staged_session, target)
    print(f"✅ TDC final → {target}")

    return {"backup": bak, "staged_session": staged_session, "final_session": target}
