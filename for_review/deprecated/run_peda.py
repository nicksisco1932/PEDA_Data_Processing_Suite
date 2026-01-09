#!/usr/bin/env python3
r"""
run_peda.py  (v0.4.0)

Structure-aware PEDA runner/simulator.

Accepted input layouts (all are valid):

A) Canonical (preferred):
<ROOT or OUTPUT>\<CASE>\
  ├─ TDC Sessions\
  └─ MR DICOM\

B) Legacy nested case folder:
<ROOT or OUTPUT>\<CASE>\
  ├─ <CASE> TDC Sessions\
  └─ <CASE> MR DICOM\

C) Legacy flat layout:
<ROOT or OUTPUT>\
  ├─ <CASE> TDC Sessions\
  └─ <CASE> MR DICOM\

Behaviors:
- Validates the layout before running (works for A/B/C).
- Real mode: calls MATLAB -batch "cd(PEDA); MAIN_PEDA('<INPUT_DIR>')"
  where <INPUT_DIR> is exactly the directory you pass in (case_dir).
- Simulation: DOES NOT create any directories. Writes a PEDA-only marker
  at the provided input directory ("_sim_peda.txt") and logs the run.

Notes:
- If MATLAB is not found and not forced, we simulate.
- If structure is invalid and --force-matlab is set, we return an error.
"""

from __future__ import annotations
import argparse, datetime, os, re, sys, subprocess
from pathlib import Path

# You can override via environment variable PEDA_HOME
DEFAULT_PEDA_HOME = os.environ.get("PEDA_HOME", r"C:\Users\NicholasSisco\Local_apps\PEDA")

CASE_NAME_RE = re.compile(r"^\d{3}_\d{2}-\d{3}$")
REQ_TDC_NAME = "TDC Sessions"
REQ_DCM_NAME = "MR DICOM"
LEGACY_TDC_SUFFIX = " TDC Sessions"
LEGACY_DCM_SUFFIX = " MR DICOM"

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
        if cand.exists():
            return str(cand)
    common = [
        r"C:\Program Files\MATLAB\R2025a\bin\matlab.exe",
        r"C:\Program Files\MATLAB\R2024b\bin\matlab.exe",
        r"C:\Program Files\MATLAB\R2024a\bin\matlab.exe",
        r"C:\Program Files\MATLAB\R2023b\bin\matlab.exe",
        r"C:\Program Files\MATLAB\R2023a\bin\matlab.exe",
    ]
    for p in common:
        if Path(p).exists():
            return p
    return None  # not found

def _timestamp() -> str:
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(text)

# ---------------- Structure resolution & validation ----------------

def _resolve_case_paths(case_dir: Path) -> tuple[str|None, Path, Path]:
    """
    Returns (case_name, tdc_path, dcm_path) for either layout:
      A) Canonical: case_dir/TDC Sessions and case_dir/MR DICOM
      B) Legacy nested:  case_dir.name == CASE and case_dir/<CASE> TDC Sessions|MR DICOM
      C) Legacy flat:    case_dir contains <CASE> TDC Sessions|MR DICOM directly
    If not resolvable, returns (None, candidate_tdc, candidate_dcm) where candidates
    are what the nested paths *would* be for diagnostics.
    """
    case_dir = case_dir.resolve()
    name = case_dir.name

    # Canonical (unprefixed) first
    tdc_canon = case_dir / REQ_TDC_NAME
    dcm_canon = case_dir / REQ_DCM_NAME
    if tdc_canon.exists() or dcm_canon.exists():
        return name, tdc_canon, dcm_canon

    # Legacy nested
    tdc_nested = case_dir / f"{name}{LEGACY_TDC_SUFFIX}"
    dcm_nested = case_dir / f"{name}{LEGACY_DCM_SUFFIX}"
    if tdc_nested.exists() or dcm_nested.exists():
        # We accept "exists" because some users may only provide one; validation checks both.
        return name, tdc_nested, dcm_nested

    # Legacy flat: scan direct children and infer a consistent CASE ID
    try:
        kids = [p for p in case_dir.iterdir() if p.is_dir()]
    except FileNotFoundError:
        kids = []

    candidates: dict[str, dict[str, Path]] = {}
    for d in kids:
        dn = d.name
        if dn.endswith(LEGACY_TDC_SUFFIX):
            cid = dn[: -len(LEGACY_TDC_SUFFIX)]
            candidates.setdefault(cid, {})["tdc"] = d
        elif dn.endswith(LEGACY_DCM_SUFFIX):
            cid = dn[: -len(LEGACY_DCM_SUFFIX)]
            candidates.setdefault(cid, {})["dcm"] = d

    for cid, have in candidates.items():
        if CASE_NAME_RE.match(cid) and "tdc" in have and "dcm" in have:
            return cid, have["tdc"], have["dcm"]

    # Nothing matched
    return None, tdc_nested, dcm_nested

def _validate_structure(case_dir: Path) -> tuple[bool, list[str], str|None, Path, Path]:
    """
    Validates presence of both required subfolders in either layout.
    Returns: (ok, errs, case_name, tdc_path, dcm_path)
    """
    errs: list[str] = []
    if not case_dir.is_dir():
        errs.append(f"Not a directory: {case_dir}")
        return False, errs, None, case_dir, case_dir

    case_name, tdc_path, dcm_path = _resolve_case_paths(case_dir)
    if case_name is None:
        errs.append(
            "Could not find a valid case layout. Expected either:\n"
            "  A) <...>\\<CASE>\\TDC Sessions and MR DICOM\n"
            "  B) <...>\\<CASE>\\<CASE> TDC Sessions and <CASE> MR DICOM\n"
            "  C) <...>\\<CASE> TDC Sessions and <CASE> MR DICOM"
        )
        return False, errs, None, tdc_path, dcm_path

    if not tdc_path.exists() or not tdc_path.is_dir():
        errs.append(f"Missing required subfolder: {tdc_path}")
    if not dcm_path.exists() or not dcm_path.is_dir():
        errs.append(f"Missing required subfolder: {dcm_path}")

    ok = len(errs) == 0
    return ok, errs, case_name, tdc_path, dcm_path

# ---------------- Simulation & real run ----------------

def _simulate(case_dir: Path, peda_home: Path, log_path: Path) -> tuple[int, Path]:
    """
    Simulation: do not create any directories.
    Only writes a marker and a log entry describing the detected state.
    """
    ts = _timestamp()
    ok, errs, case_name, tdc_path, dcm_path = _validate_structure(case_dir)

    # Marker (at the INPUT directory you passed)
    (case_dir / "_sim_peda.txt").write_text(
        f"PEDA simulated run\nINPUT  : {case_dir}\nPEDA   : {peda_home}\nTIME   : {ts}\n",
        encoding="utf-8",
    )

    if not ok:
        _write(
            log_path,
            "-----------------------------------------\n"
            "PEDA SIMULATION — INVALID STRUCTURE\n"
            "-----------------------------------------\n"
            f"INPUT : {case_dir}\n"
            f"PEDA  : {peda_home}\n"
            f"TIME  : {ts}\n"
            "Errors:\n  - " + "\n  - ".join(errs) + "\n"
        )
        return 2, log_path

    _write(
        log_path,
        "-----------------------------------------\n"
        "PEDA SIMULATION (no MATLAB run)\n"
        "-----------------------------------------\n"
        f"INPUT : {case_dir}\n"
        f"PEDA  : {peda_home}\n"
        f"CASE  : {case_name}\n"
        f"TDC   : {tdc_path}\n"
        f"MR    : {dcm_path}\n"
        f"LOG   : {log_path}\n"
        f"TIME  : {ts}\n"
        "Result: SIMULATED OK (no directories created)\n"
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
    log_dir = (log_root if log_root else (case_dir / "TDC Sessions" / "applog" / "Logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"PEDA_{ts}.log"

    # If simulate is forced, do that immediately (but still log structure)
    if simulate is True:
        return _simulate(case_dir, peda_home, log_path)

    # For real runs or simulate fallback, validate first
    ok, errs, case_name, tdc_path, dcm_path = _validate_structure(case_dir)
    if not ok:
        if force_matlab:
            _write(log_path, "ERROR: Required case structure invalid:\n  - " + "\n  - ".join(errs) + "\n")
            return 2, log_path
        # auto-simulate fallback when not forcing
        return _simulate(case_dir, peda_home, log_path)

    # Try to find MATLAB
    m_exe = _find_matlab_exe(matlab_exe)
    if m_exe is None:
        # in run_peda.py, just before `return _simulate(...)` when m_exe is None
        _write(log_path, "INFO: matlab.exe not found; falling back to SIMULATION (use --force-matlab to error)\n")
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
    input_m = _norm_for_matlab(case_dir)   # pass EXACT directory provided (flat or nested)

    batch_cmd = f"cd('{peda_m}'); MAIN_PEDA('{input_m}')"

    header = (
        "-----------------------------------------\n"
        "PEDA PROCESSING (run_peda.py)\n"
        "-----------------------------------------\n"
        f"MATLAB: {m_exe}\n"
        f"PEDA  : {peda_home}\n"
        f"INPUT : {case_dir}\n"
        f"TDC   : {tdc_path}\n"
        f"MR    : {dcm_path}\n"
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
    ap.add_argument("case_dir", help="Directory that contains the two required folders (flat or nested layout).")
    ap.add_argument("--peda-home", default=DEFAULT_PEDA_HOME, help="Folder containing MAIN_PEDA.m")
    ap.add_argument("--matlab-exe", default=None, help="Full path to matlab.exe")
    ap.add_argument("--log-root", default=None, help="Directory for logs; default is <input>\\applog\\Logs")
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
