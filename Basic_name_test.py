#!/usr/bin/env python3
r"""
Basic_name_test.py  (v0.48)

Fixes:
- Avoid extra "Logs/Logs" nesting by moving the CONTENTS of source Logs into
  "<canonical> TDC Sessions\applog\Logs" (creating Logs_02, Logs_03... if needed).
- Rename files placed into applog\Logs to prepend the case prefix
  (e.g., "017-01-474_<original>").

Core behavior (unchanged otherwise):
- Unzips to <case>\<zip-stem>\ (staging)
- Moves 'Raw' and date-like dirs -> "<canonical> TDC Sessions\"
- Autoswitch to 7-Zip for huge archives; heartbeat progress line
- Removes staging folder after organizing
"""

from __future__ import annotations
import argparse, logging, re, sys, shutil, os, time, subprocess, shutil as _shutil_mod
from pathlib import Path
from typing import Iterable, List
from zipfile import ZipFile, BadZipFile

CASE_PATTERN = re.compile(r"(?<!\d)(\d{3})[\-_](\d{2})[\-_](\d{3})(?!\d)")
DATE_DIR_PATTERN = re.compile(r"""
    ^_?                # optional leading underscore
    \d{4}-\d{2}-\d{2}  # YYYY-MM-DD
    (--\d{2}-\d{2}-\d{2})?   # optional --HH-MM-SS
    (?:\s+\d+)?        # optional numeric suffix
    $                  # end
""", re.X)

# ---------- CLI ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Touch, parse IDs, create case folder, unzip zips; optional skeleton + PEDA ingest."
    )
    p.add_argument("items", nargs="+", type=Path,
                   help="One or more inputs: .zip files and/or a PowerShell list file")
    p.add_argument("--dest-root", type=Path, default=Path(r"D:\Data_Clean"),
                   help=r"Destination root (default: D:\Data_Clean)")
    p.add_argument("--quiet", action="store_true", help="Suppress INFO logs")
    p.add_argument("--rm-zip-after", action="store_true",
                   help="Delete each source .zip after successful unzip")
    p.add_argument("--simulate", action="store_true",
                   help="Do not unzip or delete; only print sequencing messages")
    p.add_argument("--init-skeleton", action="store_true",
                   help="Create standard subfolders in the case folder")
    p.add_argument("--peda-version", default="PEDAv9.1.3",
                   help="Version tag used in folder/file names (default: PEDAv9.1.3)")
    p.add_argument("--ingest-peda", action="store_true",
                   help="Copy <case>\\{PEDA} -> '<canonical> {PEDA}-Video' and zip to '<canonical> {PEDA}-Data.zip'")
    p.add_argument("--peda-src", type=Path,
                   help="Explicit path to PEDA source directory (default: <case_folder>\\{PEDA})")
    # unzip controls
    p.add_argument("--unzip-backend", choices=["auto","python","7z"], default="auto",
                   help="Extractor backend to use (default: auto)")
    p.add_argument("--heartbeat-seconds", type=int, default=60,
                   help="Progress heartbeat interval in seconds (0 disables; default: 60)")
    p.add_argument("--7z-threshold-files", type=int, default=50000,
                   dest="threshold_files",
                   help="Auto-switch to 7z when ZIP entries >= this value (default: 50000)")
    p.add_argument("--sevenzip-path", type=Path, default=None,
                   help="Explicit path to 7z/7z.exe (overrides PATH lookup)")
    return p.parse_args()

def setup_logging(quiet: bool) -> None:
    logging.basicConfig(
        level=(logging.ERROR if quiet else logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s"
    )

# ---------- Utilities ----------

def touch_file(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.touch(exist_ok=True)

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def unique_child(parent: Path, basename: str) -> Path:
    """
    Return a non-existing path under parent by suffixing _NN if needed.
    Works for both files and directories.
    """
    cand = parent / basename
    if not cand.exists():
        return cand
    ii = 1
    stem = basename
    suffix = ""
    # If basename has an extension, preserve it for files
    if "." in basename and not basename.endswith("."):
        stem = basename[:basename.rfind(".")]
        suffix = basename[basename.rfind("."):]
    while True:
        cc = parent / f"{stem}_{ii:02d}{suffix}"
        if not cc.exists():
            return cc
        ii += 1

def extract_blocks(name: str) -> tuple[str, str, str]:
    m = CASE_PATTERN.search(name)
    if not m:
        raise ValueError(f"No case key found in {name} (expected XXX-XX-XXX).")
    return m.groups()  # SiteID, TULSA, Treatment

def ensure_case_folder(dest_root: Path, site: str, tulsa: str, treatment: str) -> Path:
    case_folder = dest_root / f"{site}_01-{treatment}"
    case_folder.mkdir(parents=True, exist_ok=True)
    return case_folder

def init_case_skeleton(case_folder: Path, canonical: str, peda_version: str) -> None:
    names = [
        f"{canonical} Misc",
        f"{canonical} MR DICOM",
        f"{canonical} {peda_version}-Video",
        f"{canonical} TDC Sessions",
    ]
    for name in names:
        ensure_dir(case_folder / name)
    ensure_dir(case_folder / f"{canonical} TDC Sessions" / "applog")
    logging.info("Skeleton created: %s", case_folder)

def ingest_peda_dir(case_folder: Path, canonical: str, peda_version: str, peda_src: Path | None) -> None:
    src = peda_src if peda_src else (case_folder / peda_version)
    if not src.exists() or not src.is_dir():
        logging.error("PEDA source directory not found: %s", src)
        return
    video_dir = case_folder / f"{canonical} {peda_version}-Video"
    video_dir.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src, video_dir, dirs_exist_ok=True)
    zip_path = case_folder / f"{canonical} {peda_version}-Data.zip"
    with ZipFile(zip_path, "w") as zf:
        for p in src.rglob("*"):
            if p.is_file():
                zf.write(p, p.relative_to(src))
    logging.info("PEDA copied -> %s; zipped -> %s", video_dir, zip_path)

# ---------- Unzip backends ----------

def _count_zip_entries(zip_path: Path) -> int:
    with ZipFile(zip_path) as zf:
        return len(zf.infolist())

def _find_7z_exe(explicit: Path | None) -> Path | None:
    if explicit:
        return explicit if explicit.exists() else None
    for name in ("7z.exe", "7z"):
        p = _shutil_mod.which(name)
        if p:
            return Path(p)
    return None

def _choose_backend(zip_path: Path, preferred: str, threshold: int, sevenz_path: Path | None) -> str:
    if preferred in ("python", "7z"):
        if preferred == "7z" and not _find_7z_exe(sevenz_path):
            logging.warning("7z requested but not found; falling back to python.")
            return "python"
        return preferred
    # auto
    try:
        n = _count_zip_entries(zip_path)
    except BadZipFile:
        logging.warning("Could not count entries; using python backend for %s", zip_path.name)
        return "python"
    if _find_7z_exe(sevenz_path) and n >= threshold:
        logging.info("ZIP has %d entries ≥ %d; auto-switching to 7z for %s", n, threshold, zip_path.name)
        return "7z"
    logging.info("ZIP has %d entries; using python for %s", n, zip_path.name)
    return "python"

def unzip_with_progress_python(zip_path: Path, out_dir: Path, heartbeat: int = 60) -> None:
    with ZipFile(zip_path) as zf:
        infos = zf.infolist()
        out_dir.mkdir(parents=True, exist_ok=True)
        start = time.time()
        last = start
        for info in infos:
            zf.extract(info, out_dir)
            if heartbeat > 0:
                now = time.time()
                if now - last >= heartbeat:
                    print(f">----------- Unzipping ({int(now-start)} s elapsed) -------------------<")
                    last = now
    logging.info("Unzipped %s -> %s", zip_path.name, out_dir)

def unzip_with_progress_7z(zip_path: Path, out_dir: Path, heartbeat: int = 60, sevenz_path: Path | None = None) -> None:
    sevenz = _find_7z_exe(sevenz_path)
    if not sevenz:
        raise FileNotFoundError("7z executable not found. Install 7-Zip or pass --sevenzip-path.")
    ensure_dir(out_dir)
    proc = subprocess.Popen(
        [str(sevenz), "x", "-y", str(zip_path), f"-o{out_dir}"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    )
    start = time.time()
    last = start
    if proc.stdout:
        for _ in proc.stdout:
            if heartbeat > 0:
                now = time.time()
                if now - last >= heartbeat:
                    print(f">----------- Unzipping ({int(now-start)} s elapsed) -------------------<")
                    last = now
    ret = proc.wait()
    if ret != 0:
        raise RuntimeError(f"7z extraction failed (exit {ret}) for {zip_path}")
    logging.info("Unzipped (7z) %s -> %s", zip_path.name, out_dir)

def unzip_dispatch(zip_path: Path, out_dir: Path, preferred_backend: str,
                   heartbeat: int, threshold: int, sevenz_path: Path | None) -> None:
    backend = _choose_backend(zip_path, preferred_backend, threshold, sevenz_path)
    if backend == "7z":
        unzip_with_progress_7z(zip_path, out_dir, heartbeat=heartbeat, sevenz_path=sevenz_path)
    else:
        unzip_with_progress_python(zip_path, out_dir, heartbeat=heartbeat)

def unzip_all_zips(folder: Path, preferred_backend: str, heartbeat: int, threshold: int, sevenz_path: Path | None) -> None:
    for z in folder.glob("*.zip"):
        out_dir = folder / z.stem
        try:
            unzip_dispatch(z, out_dir, preferred_backend, heartbeat, threshold, sevenz_path)
        except BadZipFile:
            logging.error("Bad/corrupt zip: %s", z)

def unzip_from_path(zip_path: Path, out_parent: Path, preferred_backend: str,
                    heartbeat: int, threshold: int, sevenz_path: Path | None) -> Path:
    if zip_path.suffix.lower() != ".zip":
        return out_parent
    out_dir = out_parent / zip_path.stem  # staging root; name can be anything
    unzip_dispatch(zip_path, out_dir, preferred_backend, heartbeat, threshold, sevenz_path)
    return out_dir

# ---------- Post-unzip organization ----------

def _find_first_logs(root: Path, max_depth: int = 6) -> Path | None:
    root = root.resolve()
    base = len(root.parts)
    for dirpath, dirnames, _ in os.walk(root):
        p = Path(dirpath)
        if len(p.parts) - base > max_depth:
            dirnames[:] = []
            continue
        for d in dirnames:
            if d.lower() == "logs":
                return p / d
    return None

def _iter_data_dirs(root: Path) -> List[Path]:
    """Return candidate data dirs under root: 'Raw' + date-like names (one level down)."""
    out: List[Path] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        name = child.name
        if name.lower() == "raw" or DATE_DIR_PATTERN.match(name):
            out.append(child)
    return out

def _move_dir_contents(src: Path, dst: Path) -> None:
    """
    Move all children of src into dst (flattening one level). Preserve subfolders.
    If a name collides, suffix _NN.
    """
    for item in src.iterdir():
        target = dst / item.name
        if target.exists():
            target = unique_child(dst, item.name)
        try:
            shutil.move(str(item), str(target))
        except Exception:
            if item.is_dir():
                shutil.copytree(item, target, dirs_exist_ok=True)
                shutil.rmtree(item, ignore_errors=True)
            else:
                shutil.copy2(item, target)
                try: item.unlink()
                except FileNotFoundError: pass

def _prefix_files_in_dir(root: Path, prefix: str) -> None:
    """
    Prepend `prefix + '_'` to each file directly under `root` (not recursive).
    Skip files already starting with the prefix.
    Use suffixing if the target name exists.
    """
    for f in root.iterdir():
        if not f.is_file():
            continue
        if f.name.startswith(prefix + "_"):
            continue
        target = root / f"{prefix}_{f.name}"
        if target.exists():
            target = unique_child(root, target.name)
        try:
            f.rename(target)
        except Exception:
            # fall back to copy+delete
            shutil.copy2(f, target)
            try: f.unlink()
            except FileNotFoundError: pass

def organize_unzipped_session(staging_root: Path, case_folder: Path, canonical: str, simulate: bool = False) -> None:
    """
    1) Move CONTENTS of Logs -> "<canonical> TDC Sessions\applog\Logs" (with auto-suffix if exists)
    2) Rename files in applog\Logs to prepend "<site>-<tulsa>-<treatment>_"
    3) Move Raw + date-like dirs -> "<canonical> TDC Sessions\"
    """
    tdc_sessions = case_folder / f"{canonical} TDC Sessions"
    applog = tdc_sessions / "applog"
    ensure_dir(applog)

    file_prefix = canonical.replace("_", "-")  # e.g., "017_01-474" -> "017-01-474"

    # 1) Logs (flatten)
    logs_dir = _find_first_logs(staging_root)
    if logs_dir:
        target_logs = applog / "Logs"
        if target_logs.exists():
            target_logs = unique_child(applog, "Logs")
        logging.info("Place Logs CONTENTS: %s -> %s", logs_dir, target_logs)
        if not simulate:
            target_logs.mkdir(parents=True, exist_ok=True)
            _move_dir_contents(logs_dir, target_logs)
            # try to remove the now-empty source folder
            try: logs_dir.rmdir()
            except OSError: pass

            # 2) Rename files inside applog\Logs with prefix
            _prefix_files_in_dir(target_logs, file_prefix)
    else:
        logging.info("No 'Logs' found under staging: %s", staging_root)

    # 3) Data dirs (Raw + date-like)
    for dd in _iter_data_dirs(staging_root):
        dest = tdc_sessions / dd.name
        if dest.exists():
            dest = unique_child(tdc_sessions, dd.name)
        logging.info("Place data dir: %s -> %s", dd, dest)
        if not simulate:
            try:
                shutil.move(str(dd), str(dest))
            except Exception:
                shutil.copytree(dd, dest, dirs_exist_ok=True)
                shutil.rmtree(dd, ignore_errors=True)

def cleanup_staging(staging_root: Path, simulate: bool = False) -> None:
    """Remove the staging folder (best-effort)."""
    if simulate:
        logging.info("SIMULATE cleanup: would remove staging %s", staging_root)
        return
    try:
        shutil.rmtree(staging_root, ignore_errors=True)
        logging.info("Removed staging folder: %s", staging_root)
    except Exception as e:
        logging.warning("Could not fully remove staging %s: %s", staging_root, e)

# ---------- PowerShell list parsing ----------

def _read_text_guess(path: Path) -> str:
    for enc in ("utf-8", "utf-16", "utf-16-le", "utf-16-be"):
        try:
            return path.read_text(encoding=enc)
        except Exception:
            continue
    return path.read_bytes().decode(errors="ignore")

def _parse_powershell_list(text: str) -> List[Path]:
    base: Path | None = None
    out: List[Path] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.lower().startswith("directory:"):
            base_str = line.split(":", 1)[1].strip()
            base = Path(base_str)
            continue
        if ".zip" in line.lower():
            name = line.split()[-1]
            p = Path(name)
            if not p.is_absolute() and base is not None:
                p = base / name
            out.append(p)
    return out

def collect_input_paths(items: Iterable[Path]) -> List[Path]:
    paths: List[Path] = []
    for it in items:
        if it.suffix.lower() == ".zip":
            paths.append(it)
        elif it.is_file():
            text = _read_text_guess(it)
            parsed = _parse_powershell_list(text)
            paths.extend(parsed if parsed else [it])
        else:
            paths.append(it)
    return paths

# ---------- Per-path processing ----------

def process_one_path(path: Path, dest_root: Path, rm_zip: bool,
                     simulate: bool, init_skel: bool, ingest_peda: bool,
                     peda_version: str, peda_src: Path | None, next_name: str | None,
                     unzip_backend: str, heartbeat: int, threshold: int, sevenz_path: Path | None) -> int:
    try:
        touch_file(path)
        logging.info("Touched file: %s", path.resolve())
    except Exception as e:
        logging.error("Failed to touch file: %s", e)
        return 2

    try:
        site, tulsa, treatment = extract_blocks(path.name)
    except ValueError as e:
        logging.error(str(e))
        return 10

    case_folder = ensure_case_folder(dest_root, site, tulsa, treatment)
    canonical = f"{site}_{tulsa}-{treatment}"

    if init_skel:
        init_case_skeleton(case_folder, canonical, peda_version)
    if ingest_peda:
        ingest_peda_dir(case_folder, canonical, peda_version, peda_src)

    if path.suffix.lower() == ".zip":
        print(f"Identified {path.name}, will Unzip now...")
        if not simulate:
            try:
                staging_root = unzip_from_path(path, case_folder,
                                               preferred_backend=unzip_backend,
                                               heartbeat=heartbeat,
                                               threshold=threshold,
                                               sevenz_path=sevenz_path)
            except BadZipFile:
                logging.error("Bad/corrupt zip: %s", path)
                return 20

            deleted = False
            if rm_zip:
                try:
                    path.unlink()
                    deleted = True
                    logging.info("Deleted zip: %s", path)
                except Exception as e:
                    logging.error("Failed to delete %s: %s", path, e)

            msg = f"Unzipped {path.name}"
            if deleted: msg += f", Deleting {path.name}"
            if next_name: msg += f", Proceeding to {next_name}"
            print(msg)

            # Also unzip any nested zips dropped in the case folder
            unzip_all_zips(case_folder,
                           preferred_backend=unzip_backend,
                           heartbeat=heartbeat,
                           threshold=threshold,
                           sevenz_path=sevenz_path)

            # Organize + cleanup
            organize_unzipped_session(staging_root, case_folder, canonical, simulate=False)
            cleanup_staging(staging_root, simulate=False)
        else:
            msg = f"Unzipped {path.name}"
            if rm_zip: msg += f", Deleting {path.name}"
            if next_name: msg += f", Proceeding to {next_name}"
            print(msg)

    # 4-line contract
    print(f"SiteID: {site}")
    print(f"TULSA: {tulsa}")
    print(f"Treatment: {treatment}")
    print(f"{canonical}")
    return 0

# ---------- Main ----------

def main() -> int:
    args = parse_args()
    setup_logging(args.quiet)
    paths = collect_input_paths(args.items)
    for ii, pth in enumerate(paths):
        next_name = paths[ii + 1].name if ii + 1 < len(paths) else None
        rc = process_one_path(
            pth, args.dest_root, args.rm_zip_after, args.simulate,
            args.init_skeleton, args.ingest_peda, args.peda_version, args.peda_src, next_name,
            unzip_backend=args.unzip_backend, heartbeat=args.heartbeat_seconds,
            threshold=args.threshold_files, sevenz_path=args.sevenzip_path
        )
        if rc != 0:
            return rc
    return 0

if __name__ == "__main__":
    sys.exit(main())
