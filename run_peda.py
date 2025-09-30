#!/usr/bin/env python3
r"""
run_peda.py  (v0.3)

Structure-aware PEDA runner/simulator.

Required input layout:
<OUT_ROOT>/
  └─ <CASE>/                        # must match NNN_NN-NNN (e.g., 017_01-474)
     ├─ <CASE> TDC Sessions/
     └─ <CASE> MR DICOM/

Behaviors:
- Validates the layout before running.
- Real mode: calls MATLAB -batch "cd(PEDA); MAIN_PEDA('<CASE>')"
- Simulation: ensures required subfolders exist, creates small marker files,
  writes a timestamped log, and returns 0.

CLI:
  python run_peda.py <case_dir>
    [--peda-home PATH]
    [--matlab-exe PATH]
    [--log-root PATH]
    [--simulate]           # force sim (no MATLAB)
    [--force-matlab]       # error if MATLAB missing instead of sim fallback
"""

from __future__ import annotations
import argparse, datetime, os, re, sys, subprocess
from pathlib import Path

DEFAULT_PEDA_HOME = os.environ.get("PEDA_HOME", r"C:\PEDA Apps\PEDA_v9.1.3\PEDA")

CASE_NAME_RE = re.compile(r"^\d{3}_\d{2}-\d{3}$")

REQ_TDC_SUFFIX = " TDC Sessions"
REQ_DCM_SUFFIX = " MR DICOM"

def _norm_for_matlab(p: Path) -> str:
    s = str(p.resolve()).replace("\\", "/")
    return s.replace("'", "''")

def _find_matlab_exe(explicit: str|None) -> str|None:
    if explicit:
        p = Path(explicit)
        return str(p) if p.exists() else None
    mr = os.environ.get("MATLAB_ROOT")
    if mr:
        cand = Path(mr) / "bin" / "matlab.exe"
        if cand.exists(): return str(cand)
    common = [
        r"C:\Program Files\MATLAB\R2025a\bin\matlab.exe",
        r"C:\Program Files\MATLAB\R2024b\bin\matlab.exe",
        r"C:\Program Files\MATLAB\R2024a\bin\matlab.exe",
        r"C:\Program Files\MATLAB\R2023b\bin\matlab.exe",
        r"C:\Program Files\MATLAB\R2023a\bin\matlab.exe",
    ]
    for p in common:
        if Path(p).exists(): return p
    return None  # not found

def _timestamp() -> str:
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(text)

def _required_paths(case_dir: Path) -> tuple[Path, Path]:
    case_name = case_dir.name
    return (
        case_dir / f"{case_name}{REQ_TDC_SUFFIX}",
        case_dir / f"{case_name}{REQ_DCM_SUFFIX}",
    )

def _validate_structure(case_dir: Path) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if not case_dir.is_dir():
        errs.append(f"Not a directory: {case_dir}")
        return False, errs
    case_name = case_dir.name
    if not CASE_NAME_RE.match(case_name):
        errs.append(f"Case folder name must match NNN_NN-NNN (e.g., 017_01-474); got: {case_name}")
    tdc_path, dcm_path = _required_paths(case_dir)
    if not tdc_path.exists() or not tdc_path.is_dir():
        errs.append(f"Missing required subfolder: {tdc_path}")
    if not dcm_path.exists() or not dcm_path.is_dir():
        errs.append(f"Missing required subfolder: {dcm_path}")
    return (len(errs) == 0), errs

def _simulate(case_dir: Path, peda_home: Path, log_path: Path) -> tuple[int, Path]:
    ts = _timestamp()
    tdc_path, dcm_path = _required_paths(case_dir)
    # Create required structure if missing
    tdc_path.mkdir(parents=True, exist_ok=True)
    dcm_path.mkdir(parents=True, exist_ok=True)

    # Write tiny placeholders so downstream scripts have something to see
    (case_dir / "PEDA_SIMULATED_OK.txt").write_text(
        f"PEDA simulation success\nCASE   : {case_dir}\nPEDA   : {peda_home}\nTIME   : {ts}\n",
        encoding="utf-8",
    )
    (tdc_path / "_sim_placeholder.txt").write_text("Simulated TDC Sessions present.\n", encoding="utf-8")
    (dcm_path / "_sim_placeholder.txt").write_text("Simulated MR DICOM present.\n", encoding="utf-8")

    _write(
        log_path,
        "-----------------------------------------\n"
        "PEDA SIMULATION (no MATLAB run)\n"
        "-----------------------------------------\n"
        f"INPUT : {case_dir}\n"
        f"PEDA  : {peda_home}\n"
        f"LOG   : {log_path}\n"
        f"TIME  : {ts}\n"
        "Result: SIMULATED OK (required structure ensured)\n"
    )
    return 0, log_path

def run_peda(
    case_dir: Path,
    peda_home: Path|str = DEFAULT_PEDA_HOME,
    matlab_exe: str|None = None,
    log_root: Path|None = None,
    simulate: bool|None = None,
    force_matlab: bool = False,
) -> tuple[int, Path]:
    """
    Returns (exit_code, log_path).

    simulate:
      - True  -> simulate regardless of MATLAB availability
      - False -> try MATLAB; if not found and force_matlab=True, return error; else simulate
      - None  -> try MATLAB; if not found, simulate
    """
    case_dir = Path(case_dir).resolve()
    peda_home = Path(peda_home).resolve()

    ts = _timestamp()
    log_dir = (log_root if log_root else (case_dir / "applog" / "Logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"PEDA_{ts}.log"

    # Decide simulation vs real
    if simulate is True:
        return _simulate(case_dir, peda_home, log_path)

    # If we might run real MATLAB, validate structure FIRST
    ok, errs = _validate_structure(case_dir)
    if not ok:
        # If not sim and structure wrong, either fail (force_matlab) or simulate
        if force_matlab:
            _write(log_path, "ERROR: Required case structure invalid:\n  - " + "\n  - ".join(errs) + "\n")
            return 2, log_path
        # auto-simulate fallback when not forcing
        return _simulate(case_dir, peda_home, log_path)

    # Try to find MATLAB
    m_exe = _find_matlab_exe(matlab_exe)
    if m_exe is None:
        if force_matlab:
            _write(log_path, f"ERROR: matlab.exe not found and --force-matlab set. No simulation fallback.\n")
            return 4, log_path
        # fallback to simulate
        return _simulate(case_dir, peda_home, log_path)

    # Real MATLAB run
    if not peda_home.exists():
        _write(log_path, f"ERROR: PEDA home not found: {peda_home}\n")
        return 3, log_path

    peda_m = _norm_for_matlab(peda_home)
    input_m = _norm_for_matlab(case_dir)
    batch_cmd = f"cd('{peda_m}'); MAIN_PEDA('{input_m}')"

    header = (
        "-----------------------------------------\n"
        "PEDA PROCESSING (run_peda.py)\n"
        "-----------------------------------------\n"
        f"MATLAB: {m_exe}\n"
        f"PEDA  : {peda_home}\n"
        f"INPUT : {case_dir}\n"
        f"LOG   : {log_path}\n"
        f"TIME  : {ts}\n"
        "-----------------------------------------\n"
    )
    _write(log_path, header)

    cmd = [m_exe, "-batch", batch_cmd, "-logfile", str(log_path)]
    try:
        proc = subprocess.run(cmd)
        if proc.returncode != 0:
            _write(log_path, f"ERROR: PEDA failed with code {proc.returncode}\n")
        else:
            _write(log_path, "OK: PEDA completed.\n")
        return proc.returncode, log_path
    except FileNotFoundError as e:
        _write(log_path, f"ERROR: MATLAB launch failed: {e}\n")
        return 4, log_path
    except Exception as e:
        _write(log_path, f"ERROR: Unexpected: {e}\n")
        return 5, log_path

# --------------- CLI ----------------

def _cli():
    ap = argparse.ArgumentParser(description="Run or simulate PEDA with required structure checks.")
    ap.add_argument("case_dir")
    ap.add_argument("--peda-home", default=DEFAULT_PEDA_HOME, help="Folder containing MAIN_PEDA.m")
    ap.add_argument("--matlab-exe", default=None, help="Full path to matlab.exe")
    ap.add_argument("--log-root", default=None, help="Directory for logs; default is <case>\\applog\\Logs")
    ap.add_argument("--simulate", action="store_true", help="Force simulation (no MATLAB)")
    ap.add_argument("--force-matlab", action="store_true", help="Error if MATLAB not found OR structure invalid")
    args = ap.parse_args()

    code, logp = run_peda(
        case_dir=Path(args.case_dir),
        peda_home=Path(args.peda_home),
        matlab_exe=args.matlab_exe,
        log_root=Path(args.log_root) if args.log_root else None,
        simulate=True if args.simulate else None,
        force_matlab=args.force_matlab,
    )
    print(f"PEDA exit code: {code}")
    print(f"Log: {logp}")
    sys.exit(code)

if __name__ == "__main__":
    _cli()
