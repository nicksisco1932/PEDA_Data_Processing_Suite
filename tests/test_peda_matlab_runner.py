# PURPOSE: Validate MATLAB command construction and failure handling for PEDA.
# INPUTS: Temporary dirs and monkeypatched subprocess/which.
# OUTPUTS: Assertions on args, resolution, and error messages.
from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path

import pytest

from src.logutil import ProcessingError
from src.tools.matlab_runner import (
    build_matlab_args,
    build_matlab_batch_cmd,
    resolve_matlab_exe,
    resolve_peda_main_dir,
    run_matlab_batch,
)


def test_matlab_command_build_and_resolution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    matlab_fake = tmp_path / "bin" / "matlab.exe"
    matlab_fake.parent.mkdir(parents=True, exist_ok=True)
    matlab_fake.write_text("stub", encoding="utf-8")

    monkeypatch.setattr(shutil, "which", lambda *_args, **_kwargs: str(matlab_fake))

    peda_root = tmp_path / "peda_root"
    main_dir = peda_root / "PEDA"
    main_dir.mkdir(parents=True, exist_ok=True)
    (main_dir / "MAIN_PEDA.m").write_text("% stub\n", encoding="utf-8")

    case_dir = tmp_path / "case with space"
    case_dir.mkdir(parents=True, exist_ok=True)

    matlab_exe = resolve_matlab_exe(None)
    assert matlab_exe == matlab_fake

    peda_main_dir = resolve_peda_main_dir(peda_root)
    assert peda_main_dir == main_dir

    batch_cmd = build_matlab_batch_cmd(peda_main_dir, case_dir)
    log_path = case_dir / "PEDA_run_log.txt"
    args = build_matlab_args(matlab_exe, log_path, batch_cmd)

    assert args[0] == str(matlab_fake)
    assert "-logfile" in args
    log_idx = args.index("-logfile")
    assert args[log_idx + 1] == str(log_path)

    assert "-batch" in args
    batch_idx = args.index("-batch")
    batch_val = args[batch_idx + 1]
    assert "cd('" in batch_val
    assert "MAIN_PEDA('" in batch_val
    assert peda_main_dir.as_posix() in batch_val
    assert case_dir.as_posix() in batch_val


def test_matlab_failure_includes_log_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_path = tmp_path / "PEDA_run_log.txt"
    lines = [f"line {idx}" for idx in range(40)]
    log_path.write_text("\n".join(lines), encoding="utf-8")

    def fake_run(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args[0], 7, stdout="out", stderr="err")

    monkeypatch.setattr(subprocess, "run", fake_run)

    logger = logging.getLogger("test_matlab_runner")
    with pytest.raises(ProcessingError) as excinfo:
        run_matlab_batch(
            matlab_exe=Path("C:/fake/matlab.exe"),
            log_path=log_path,
            batch_cmd="cd('X');MAIN_PEDA('Y')",
            logger=logger,
        )

    message = str(excinfo.value)
    assert str(log_path) in message
    assert "line 39" in message
