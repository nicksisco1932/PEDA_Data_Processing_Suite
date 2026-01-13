# PURPOSE: Tests for deterministic unzip of input archives.
# INPUTS: Small temporary zip files with tricky names.
# OUTPUTS: Extracted folders under dest_root.
from __future__ import annotations

import zipfile
from pathlib import Path

from src.pipeline_steps.unzip_inputs import expand_archives


def _make_zip(path: Path, name: str, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, content)


def test_expand_archives_handles_zip_zip(tmp_path: Path) -> None:
    src = tmp_path / "alpha.zip.zip"
    _make_zip(src, "file.txt", "data")
    dest_root = tmp_path / "out"

    summary = expand_archives([src], dest_root)

    dest = dest_root / "alpha"
    assert dest.is_dir()
    assert (dest / "file.txt").is_file()
    assert summary["expanded"] == 1


def test_expand_archives_suffix_on_collision(tmp_path: Path) -> None:
    src = tmp_path / "beta.v1.zip"
    _make_zip(src, "file.txt", "data")
    dest_root = tmp_path / "out"
    (dest_root / "beta.v1").mkdir(parents=True, exist_ok=True)

    summary = expand_archives([src], dest_root)

    dest = dest_root / "beta.v1__1"
    assert dest.is_dir()
    assert (dest / "file.txt").is_file()
    assert summary["expanded"] == 1
