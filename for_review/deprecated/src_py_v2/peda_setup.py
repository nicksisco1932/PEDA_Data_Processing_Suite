# src_py_v2/peda_setup.py

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any


def peda_setup() -> Dict[str, Any]:
    """
    Minimal Python analogue of the old MATLAB peda_setup.

    Returns:
        dict with at least:
            - APPLOG_DIR: where task_master and other tools should write logs
            - PEDA_ROOT : root directory for PEDA code (used for function lookup)
    """

    # This file lives in src_py_v2, so repo_root is one level up
    here = Path(__file__).resolve().parent
    repo_root = here.parent

    # applog/ lives at the repo root
    applog_dir = repo_root / "applog"
    applog_dir.mkdir(parents=True, exist_ok=True)

    # For now, treat src_py_v2 as the active PEDA root for Python tools
    peda_root = here  # C:\...\PEDA_Data_Processing_Suite\src_py_v2

    return {
        "APPLOG_DIR": str(applog_dir),
        "PEDA_ROOT": str(peda_root),
    }
