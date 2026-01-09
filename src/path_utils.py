from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def sanitize_path_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        s = s[1:-1].strip()
    s = os.path.expandvars(s)
    if s == "":
        raise ValueError("Empty path after sanitization")
    return s


def to_path(value: Optional[str]) -> Optional[Path]:
    if value is None:
        return None
    return Path(sanitize_path_str(value))
