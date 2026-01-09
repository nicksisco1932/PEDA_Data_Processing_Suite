from __future__ import annotations

import os
import shutil
import subprocess
import zipfile
from pathlib import Path
from typing import Optional


def find_7z() -> Optional[Path]:
    for env_key in ("SEVEN_ZIP", "SEVENZIP", "7Z"):
        raw = os.environ.get(env_key)
        if raw:
            cand = Path(raw.strip().strip('"').strip("'"))
            if cand.exists():
                return cand

    candidates = [
        Path(r"C:\Program Files\7-Zip\7z.exe"),
        Path(r"C:\Program Files (x86)\7-Zip\7z.exe"),
    ]
    for cand in candidates:
        if cand.exists():
            return cand

    which = shutil.which("7z")
    return Path(which) if which else None


def extract_archive(archive_path: Path, dest_dir: Path, prefer_7z: bool = True) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    archive_path = Path(archive_path)

    if prefer_7z:
        exe = find_7z()
        if exe:
            cmd = [str(exe), "x", "-y", f"-o{dest_dir}", str(archive_path)]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                raise RuntimeError(
                    f"7z extract failed (code={proc.returncode}): {proc.stdout} {proc.stderr}"
                )
            return

    if zipfile.is_zipfile(archive_path):
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(dest_dir)
        return

    raise RuntimeError(f"Unsupported archive format: {archive_path}")


def create_zip_from_dir(src_dir: Path, dest_zip: Path, prefer_7z: bool = True) -> None:
    src_dir = Path(src_dir)
    dest_zip = Path(dest_zip)
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    if dest_zip.exists():
        dest_zip.unlink()

    if prefer_7z:
        exe = find_7z()
        if exe:
            src_glob = str(src_dir / "*")
            cmd = [str(exe), "a", "-tzip", "-mx=5", str(dest_zip), src_glob]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                raise RuntimeError(
                    f"7z archive failed (code={proc.returncode}): {proc.stdout} {proc.stderr}"
                )
            return

    with zipfile.ZipFile(dest_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(src_dir):
            rp = Path(root)
            for name in files:
                p = rp / name
                zf.write(p, arcname=str(p.relative_to(src_dir)))
