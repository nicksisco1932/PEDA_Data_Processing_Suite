from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from logutil import sha256_file


def file_metadata(path: Path, *, compute_hash: bool = False) -> Dict[str, Any]:
    data: Dict[str, Any] = {"path": str(path), "exists": path.exists()}
    if not path.exists():
        return data
    st = path.stat()
    data["size_bytes"] = st.st_size
    data["mtime"] = datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds")
    if compute_hash and path.is_file():
        data["sha256"] = sha256_file(path)
    return data


def write_manifest(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
