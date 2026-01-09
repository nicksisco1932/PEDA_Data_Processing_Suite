# PURPOSE: Ensure repo root is importable during pytest runs.
# INPUTS: None.
# OUTPUTS: sys.path updated for test imports.
# NOTES: Keeps tests runnable without installing the package.
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
