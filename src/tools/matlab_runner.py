# PURPOSE: Resolve MATLAB and PEDA paths, build batch commands, and run MATLAB safely.
# INPUTS: MATLAB executable path, PEDA root, working case dir, and logger.
# OUTPUTS: Resolved paths, command args, and subprocess results or errors.
# NOTES: Keeps command strings safe for MATLAB string literals.
from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path
from typing import List

from src.logutil import ProcessingError

DEFAULT_MATLAB_EXE = Path(r"C:\Program Files\MATLAB\R2020b\bin\matlab.exe")
DEFAULT_PEDA_ROOT = Path(r"C:\PEDA Apps\PEDA_v9.1.3")


def resolve_matlab_exe(matlab_exe: Path | str | None) -> Path:
    if matlab_exe:
        candidate = Path(matlab_exe)
        if candidate.exists():
            return candidate
        raise ProcessingError(f"MATLAB executable not found at configured path: {candidate}")

    which = shutil.which("matlab") or shutil.which("matlab.exe")
    if which:
        return Path(which)

    if DEFAULT_MATLAB_EXE.exists():
        return DEFAULT_MATLAB_EXE

    raise ProcessingError(
        "MATLAB executable not found. "
        "Set pipeline.peda.matlab_exe or ensure matlab.exe is on PATH. "
        f"Checked default: {DEFAULT_MATLAB_EXE}"
    )


def resolve_peda_main_dir(peda_root: Path | str | None) -> Path:
    root = Path(peda_root) if peda_root else DEFAULT_PEDA_ROOT
    if not root.exists():
        raise ProcessingError(f"PEDA root not found: {root}")

    matches = sorted(root.rglob("MAIN_PEDA.m"))
    if len(matches) == 1:
        return matches[0].parent
    if not matches:
        raise ProcessingError(f"MAIN_PEDA.m not found under PEDA root: {root}")

    candidates = ", ".join(str(p) for p in matches)
    raise ProcessingError(
        f"Multiple MAIN_PEDA.m found under PEDA root: {root}. Candidates: {candidates}"
    )


def _matlab_escape(value: str) -> str:
    return value.replace("'", "''")


def _matlab_path(path: Path) -> str:
    return _matlab_escape(path.as_posix())


def build_matlab_batch_cmd(peda_main_dir: Path, input_dir: Path) -> str:
    main_dir = _matlab_path(Path(peda_main_dir))
    input_dir = _matlab_path(Path(input_dir))
    return f"cd('{main_dir}');MAIN_PEDA('{input_dir}')"


def build_matlab_args(matlab_exe: Path, log_path: Path, batch_cmd: str) -> List[str]:
    return [str(matlab_exe), "-logfile", str(log_path), "-batch", batch_cmd]


def _log_process_output(logger: logging.Logger, proc: subprocess.CompletedProcess[str]) -> None:
    if proc.stdout:
        for line in proc.stdout.splitlines():
            if line.strip():
                logger.info("MATLAB stdout: %s", line)
    if proc.stderr:
        for line in proc.stderr.splitlines():
            if line.strip():
                logger.error("MATLAB stderr: %s", line)


def _tail_log(path: Path, max_lines: int = 30) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return ""
    lines = text.splitlines()
    if not lines:
        return ""
    return "\n".join(lines[-max_lines:])


def run_matlab_batch(
    *,
    matlab_exe: Path,
    log_path: Path,
    batch_cmd: str,
    logger: logging.Logger,
) -> subprocess.CompletedProcess[str]:
    args = build_matlab_args(matlab_exe, log_path, batch_cmd)
    logger.info("Running MATLAB batch: %s", args)
    proc = subprocess.run(args, capture_output=True, text=True)
    _log_process_output(logger, proc)
    if proc.returncode != 0:
        tail = _tail_log(log_path, max_lines=30)
        message = (
            f"MATLAB failed with exit code {proc.returncode}. "
            f"See log: {log_path}"
        )
        if tail:
            message += "\nLog tail:\n" + tail
        raise ProcessingError(message)
    return proc
