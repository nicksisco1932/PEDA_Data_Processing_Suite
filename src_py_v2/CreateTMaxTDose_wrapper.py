"""
CreateTMaxTDose_wrapper.py

Best-effort Python analogue of CreateTMaxTDose_wrapper.m.
Dispatches to the hardened CreateTMaxTDose implementation, with
re-entrancy protection and graceful fallbacks when supporting pieces
(ParseRawDataFolder/ReadData/CalculateDynamicMasks) are still stubs.
"""

from __future__ import annotations

from typing import Any, Tuple

from CreateTMaxTDose import CreateTMaxTDose


_in_dispatch = False


def CreateTMaxTDose_wrapper(Sx: Any) -> Tuple[Any, Any, Any, Any, Any, Any]:
    global _in_dispatch
    if _in_dispatch:
        raise RuntimeError("CreateTMaxTDose_wrapper re-entry prevented.")
    _in_dispatch = True
    try:
        session_root = Sx.get("sessionRoot") or Sx.get("pathSessionFiles")
        if not session_root:
            raise ValueError("Missing sessionRoot/pathSessionFiles; skipping thermal.")
        return CreateTMaxTDose(Sx)
    finally:
        _in_dispatch = False
