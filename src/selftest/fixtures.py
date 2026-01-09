from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import List


def run_fixture_script(repo_root: Path) -> subprocess.CompletedProcess[str]:
    fixture_script = repo_root / "tools" / "generate_fixtures.py"
    return subprocess.run(
        [sys.executable, str(fixture_script)],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
    )


def get_fixture_paths(repo_root: Path) -> tuple[Path, Path]:
    mri_fixture = repo_root / "tests" / "fixtures" / "mri_dummy.zip"
    tdc_fixture = repo_root / "tests" / "fixtures" / "tdc_dummy.zip"
    return mri_fixture, tdc_fixture


def apply_path_variant(path: Path, variant: str) -> str:
    raw = str(path)
    if variant == "raw":
        return raw
    if variant == "quoted_padded":
        return f' "{raw}" '
    raise ValueError(f"Unknown path variant: {variant}")


def write_pdf_stub(path: Path) -> None:
    path.write_bytes(b"%PDF-1.4\n%EOF\n")


def build_yaml_config(
    *,
    yaml_path: Path,
    case_num: str,
    out_root: Path,
    scratch_dir: Path,
    raw_mri: str,
    raw_tdc: str,
    raw_pdf: str,
    perm_run_id: str,
    logs_dir: Path,
) -> None:
    lines = [
        "version: 1",
        "case:",
        f'  id: "{case_num}"',
        f"  root: '{out_root}'",
        "inputs:",
        '  mode: "explicit"',
        "  explicit:",
        f"    mri_zip: '{raw_mri}'",
        f"    tdc_zip: '{raw_tdc}'",
        f"    pdf_input: '{raw_pdf}'",
        "run:",
        "  scratch:",
        f"    dir: '{scratch_dir}'",
        "  flags:",
        "    test_mode: true",
        "    allow_workspace_zips: false",
        "    legacy_filename_rules: false",
        "logging:",
        f"  dir: '{logs_dir}'",
        f"  manifest_dir: '{logs_dir}'",
        "metadata:",
        f'  run_id: "{perm_run_id}"',
    ]
    yaml_text = "\n".join(lines) + "\n"
    yaml_path.write_text(yaml_text, encoding="utf-8")


def collect_zip_files(root: Path) -> List[Path]:
    return [p for p in root.rglob("*") if p.is_file() and p.suffix.lower() == ".zip"]
