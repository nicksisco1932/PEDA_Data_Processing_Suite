# PURPOSE: Validate PEDA stub artifacts and output placement.
# INPUTS: Temporary working and output directories.
# OUTPUTS: Stub PEDA output folder, video copy, and data zip.
from __future__ import annotations

import zipfile
from pathlib import Path

from src.pipeline_steps.peda_step import run_peda_step


def test_peda_stub_artifacts(tmp_path: Path) -> None:
    case_id = "005_01-082"
    working_case_dir = tmp_path / "case"
    output_root_dir = tmp_path / "out"
    working_case_dir.mkdir(parents=True, exist_ok=True)
    output_root_dir.mkdir(parents=True, exist_ok=True)

    result = run_peda_step(
        case_id=case_id,
        working_case_dir=working_case_dir,
        output_root_dir=output_root_dir,
        peda_version="v9.1.3",
        enabled=True,
        mode="stub",
    )

    peda_out_dir = working_case_dir / "PEDAv9.1.3"
    video_dir = output_root_dir / "005_01-082 PEDAv9.1.3-Video"
    data_zip = output_root_dir / "005_01-082 PEDAv9.1.3-Data.zip"
    stub_log = peda_out_dir / "applog" / "peda_log.txt"

    assert peda_out_dir.is_dir()
    assert video_dir.is_dir()
    assert data_zip.is_file()
    assert stub_log.is_file()

    mat_files = list(video_dir.rglob("*.mat"))
    assert mat_files == []

    with zipfile.ZipFile(data_zip, "r") as zf:
        names = zf.namelist()
    assert any(name.startswith("PEDAv9.1.3/") for name in names)
    assert any(name.endswith("dummy.mat") for name in names)

    stub_text = stub_log.read_text(encoding="utf-8")
    assert "STUB" in stub_text
    assert "MAIN_PEDA" in stub_text

    assert result["peda_out_dir"] == str(peda_out_dir)
