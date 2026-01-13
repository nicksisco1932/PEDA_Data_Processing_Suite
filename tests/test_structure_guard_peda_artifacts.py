# PURPOSE: Ensure structure_guard allows PEDA artifacts only when enabled.
# INPUTS: Temporary case directories with PEDA artifacts.
# OUTPUTS: Assertions on structure_guard pass/fail behavior.
from __future__ import annotations

from pathlib import Path

import src.structure_guard as sg


def _make_case_dirs(case_dir: Path) -> None:
    (case_dir / "Misc").mkdir(parents=True, exist_ok=True)
    (case_dir / "MR DICOM").mkdir(parents=True, exist_ok=True)
    (case_dir / "TDC Sessions").mkdir(parents=True, exist_ok=True)


def test_structure_guard_allows_peda_artifacts_when_enabled(tmp_path: Path) -> None:
    case_id = "093_01-098"
    peda_version = "v9.1.3"
    case_dir = tmp_path / case_id
    _make_case_dirs(case_dir)

    video_dir = case_dir / f"{case_id} PEDA{peda_version}-Video"
    data_zip = case_dir / f"{case_id} PEDA{peda_version}-Data.zip"
    video_dir.mkdir(parents=True, exist_ok=True)
    data_zip.write_bytes(b"zip")

    _, final_errs, _ = sg.enforce(
        case_dir,
        case_id,
        allow_missing_pdf=True,
        allowed_top_dirs={video_dir.name},
        forbidden_top_files=set(),
    )
    assert final_errs == []


def test_structure_guard_rejects_peda_artifacts_when_disabled(tmp_path: Path) -> None:
    case_id = "093_01-098"
    peda_version = "v9.1.3"
    case_dir = tmp_path / case_id
    _make_case_dirs(case_dir)

    video_dir = case_dir / f"{case_id} PEDA{peda_version}-Video"
    data_zip = case_dir / f"{case_id} PEDA{peda_version}-Data.zip"
    video_dir.mkdir(parents=True, exist_ok=True)
    data_zip.write_bytes(b"zip")

    _, final_errs, _ = sg.enforce(
        case_dir,
        case_id,
        allow_missing_pdf=True,
        allowed_top_dirs=set(),
        forbidden_top_files={data_zip.name},
    )
    assert final_errs
    assert any(data_zip.name in err for err in final_errs)
