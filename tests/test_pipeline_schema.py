# PURPOSE: Integration tests for test-mode output schema.
# INPUTS: Dummy fixtures and tmp_path output root.
# OUTPUTS: Assertions on case output directories and workspace contents.
# NOTES: Expects no zips under active workspace by default.
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = ROOT / "tests" / "fixtures"
MRI_ZIP = FIXTURES_DIR / "mri_dummy.zip"
TDC_ZIP = FIXTURES_DIR / "tdc_dummy.zip"


def _ensure_fixtures() -> None:
    if MRI_ZIP.exists() and TDC_ZIP.exists():
        return
    cmd = [sys.executable, str(ROOT / "tools" / "generate_fixtures.py")]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode == 0, f"fixture generation failed:\n{proc.stdout}\n{proc.stderr}"


def _run_controller(*args: str) -> subprocess.CompletedProcess[str]:
    cmd = [sys.executable, str(ROOT / "src" / "controller.py"), *args]
    return subprocess.run(cmd, capture_output=True, text=True)


def test_test_mode_produces_schema(tmp_path: Path) -> None:
    _ensure_fixtures()
    case_id = "TEST_CASE"
    proc = _run_controller(
        "--root",
        str(tmp_path),
        "--case",
        case_id,
        "--mri-input",
        str(MRI_ZIP),
        "--tdc-input",
        str(TDC_ZIP),
        "--test-mode",
        "--run-id",
        "TEST_RUN",
    )
    assert proc.returncode == 0, f"controller failed:\n{proc.stdout}\n{proc.stderr}"

    case_dir = tmp_path / case_id
    misc_dir = case_dir / f"{case_id} Misc"
    mr_dir = case_dir / f"{case_id} MR DICOM"
    tdc_dir = case_dir / f"{case_id} TDC Sessions"
    assert misc_dir.is_dir()
    assert mr_dir.is_dir()
    assert tdc_dir.is_dir()

    workspaces = [p for p in tdc_dir.iterdir() if p.is_dir() and p.name.startswith("_")]
    assert len(workspaces) == 1
    workspace = workspaces[0]

    assert (workspace / "local.db").is_file()
    assert (workspace / "Raw").is_dir()
    assert list(workspace.rglob("*.zip")) == []


def test_no_unprefixed_dirs_created(tmp_path: Path) -> None:
    _ensure_fixtures()
    case_id = "TEST_CASE"
    proc = _run_controller(
        "--root",
        str(tmp_path),
        "--case",
        case_id,
        "--mri-input",
        str(MRI_ZIP),
        "--tdc-input",
        str(TDC_ZIP),
        "--test-mode",
        "--run-id",
        "TEST_RUN",
    )
    assert proc.returncode == 0, f"controller failed:\n{proc.stdout}\n{proc.stderr}"

    case_dir = tmp_path / case_id
    assert not (case_dir / "Misc").exists()
    assert not (case_dir / "MR DICOM").exists()
    assert not (case_dir / "TDC Sessions").exists()
