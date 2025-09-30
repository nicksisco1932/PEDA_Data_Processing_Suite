#!/usr/bin/env python3
"""
Basic_name_test.py  (v0.21)

What it does (minimal, robust):
1) Touch the given file path (create if missing).
2) Parse filename blocks -> SiteID (XXX), TULSA (XX), Treatment (XXX).
3) Build a destination folder: <dest_root>\\<SiteID>_01-<Treatment>
4) Ensure the folder exists.
5) Unzip:
   a) If the input path is a .zip (anywhere), extract it into the case folder (no move, no delete).
   b) Also unzip any *.zip found in that case folder to subfolders named after each zip.
6) Print labeled blocks + canonical key (NNN_NN-NNN).

Notes:
- No prompts. No Downloads moves. No Explorer launch.
- Default dest_root is D:\\Data_Clean (override with --dest-root).
"""

from __future__ import annotations
import argparse, logging, re, sys
from pathlib import Path
from zipfile import ZipFile, BadZipFile

CASE_PATTERN = re.compile(r"(?<!\d)(\d{3})[\-_](\d{2})[\-_](\d{3})(?!\d)")

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Touch, parse IDs, create case folder, unzip any zips there.")
    p.add_argument("path", type=Path, help="Input file path (e.g., 017-01_474_TDC.zip)")
    p.add_argument("--dest-root", type=Path, default=Path(r"D:\Data_Clean"),
                   help=r"Destination root (default: D:\Data_Clean)")
    p.add_argument("--quiet", action="store_true", help="Suppress INFO logs")
    return p.parse_args()

def setup_logging(quiet: bool) -> None:
    logging.basicConfig(
        level=(logging.ERROR if quiet else logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s"
    )

def touch_file(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.touch(exist_ok=True)

def extract_blocks(name: str) -> tuple[str, str, str]:
    m = CASE_PATTERN.search(name)
    if not m:
        raise ValueError(f"No case key found in {name} (expected XXX-XX-XXX).")
    return m.groups()  # SiteID, TULSA, Treatment

def ensure_case_folder(dest_root: Path, site: str, tulsa: str, treatment: str) -> Path:
    # Per legacy convention: <SiteID>_01-<Treatment> (TULSA fixed as “01”)
    case_folder = dest_root / f"{site}_01-{treatment}"
    case_folder.mkdir(parents=True, exist_ok=True)
    return case_folder

def unzip_all_zips(folder: Path) -> None:
    """Unzip each *.zip in `folder` into a child folder named after the archive (BaseName)."""
    for z in folder.glob("*.zip"):
        out_dir = folder / z.stem
        try:
            with ZipFile(z) as zf:
                out_dir.mkdir(parents=True, exist_ok=True)
                zf.extractall(out_dir)
            logging.info("Unzipped %s -> %s", z.name, out_dir)
        except BadZipFile:
            logging.error("Bad/corrupt zip: %s", z)

def unzip_from_path(zip_path: Path, out_parent: Path) -> None:
    """Unzip a specific archive into out_parent/<zip_stem>/ without moving or deleting the zip."""
    if zip_path.suffix.lower() != ".zip":
        return
    out_dir = out_parent / zip_path.stem
    try:
        with ZipFile(zip_path) as zf:
            out_dir.mkdir(parents=True, exist_ok=True)
            zf.extractall(out_dir)
        logging.info("Unzipped %s -> %s", zip_path.name, out_dir)
    except BadZipFile:
        logging.error("Bad/corrupt zip: %s", zip_path)

def main() -> int:
    args = parse_args()
    setup_logging(args.quiet)

    try:
        touch_file(args.path)
        logging.info("Touched file: %s", args.path.resolve())
    except Exception as e:
        logging.error("Failed to touch file: %s", e)
        return 2

    try:
        site, tulsa, treatment = extract_blocks(args.path.name)
    except ValueError as e:
        logging.error(str(e))
        return 10

    # Create the case folder under dest_root
    case_folder = ensure_case_folder(args.dest_root, site, tulsa, treatment)
    logging.info("Case folder ready: %s", case_folder)

    # NEW: If the input path is a .zip located anywhere, unzip it into the case folder (keep the zip in place)
    unzip_from_path(args.path, case_folder)

    # Also unzip any zips that are already in the case folder
    unzip_all_zips(case_folder)

    # Print labeled blocks + canonical key (keep this output contract)
    print(f"SiteID: {site}")
    print(f"TULSA: {tulsa}")
    print(f"Treatment: {treatment}")
    print(f"{site}_{tulsa}-{treatment}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
