# PURPOSE: Centralized handling for anonymization/check log paths and invariants.
# INPUTS: Case root path.
# OUTPUTS: annon_logs dir path and invariant validation.
# NOTES: Enforces that TDC Sessions does not contain applog/Logs.
from __future__ import annotations

from pathlib import Path
from typing import List


def get_annon_logs_dir(case_dir: Path) -> Path:
    path = Path(case_dir) / "annon_logs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def assert_no_tdc_applog_logs(case_dir: Path) -> None:
    tdc_dir = Path(case_dir) / "TDC Sessions"
    if not tdc_dir.exists():
        return
    offenders: List[Path] = []
    for path in tdc_dir.rglob("*"):
        if not path.is_dir():
            continue
        if path.name.lower() != "logs":
            continue
        parent = path.parent
        if parent.name.lower() == "applog":
            offenders.append(path)
    if offenders:
        msg = "Forbidden applog/Logs directories found under TDC Sessions: " + ", ".join(
            str(p) for p in offenders
        )
        raise RuntimeError(msg)
