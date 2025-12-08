"""
CreateIsotherms.py

Simple isotherm computation: generates a binary mask where TMax >= 55°C
and saves it as Isotherms.npy (and a 55 overlay mask).
"""

from __future__ import annotations

import numpy as np
from pathlib import Path
from typing import Any


def CreateIsotherms(ctx: Any) -> None:
    path_data = Path(ctx.get("pathData", "."))
    tmax_path = path_data / "TMax.npy"
    if not tmax_path.is_file():
        return
    try:
        tmax = np.load(tmax_path)
    except Exception:
        return
    iso_mask = (tmax >= 55.0).astype(np.uint8)
    np.save(path_data / "Isotherms.npy", iso_mask)
