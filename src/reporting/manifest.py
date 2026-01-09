from __future__ import annotations

import getpass
import platform
import socket
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from src.manifest import file_metadata as _file_metadata
from src.manifest import write_manifest as _write_manifest


def file_metadata(path: Path, compute_hash: bool = False) -> Dict[str, Any]:
    return _file_metadata(path, compute_hash=compute_hash)


def write_manifest(path: Path, payload: Dict[str, Any]) -> None:
    _write_manifest(path, payload)


def build_manifest_payload(
    *,
    cfg_for_manifest: Dict[str, Any],
    run_id: str,
    case: str,
    status: str,
    test_mode: bool,
    log_file: Path,
    planned_actions: List[str],
    step_results: Dict[str, Any],
    inputs: Dict[str, Path],
    backups: Dict[str, Any],
    outputs_meta: Dict[str, Any],
    hash_outputs: bool,
    dry_run: bool,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "run_id": run_id,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "case": case,
        "status": status,
        "test_mode": test_mode,
        "hostname": socket.gethostname(),
        "user": getpass.getuser(),
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "config": {
            k: str(v) if isinstance(v, Path) else v for k, v in cfg_for_manifest.items()
        },
        "steps": step_results,
        "plan": planned_actions,
        "inputs": {},
        "outputs": {},
        "versions": {"rich": None, "yaml": None},
        "log_file": str(log_file),
    }

    for label, path in inputs.items():
        payload["inputs"][label] = file_metadata(
            Path(path), compute_hash=hash_outputs
        )

    for label, info in backups.items():
        payload["inputs"][label] = info

    for key, meta in outputs_meta.items():
        payload["outputs"][key] = meta

    payload["outputs"]["log_file"] = file_metadata(log_file, compute_hash=False)

    if dry_run:
        payload["outputs"]["note"] = "dry-run: outputs not created"

    return payload
