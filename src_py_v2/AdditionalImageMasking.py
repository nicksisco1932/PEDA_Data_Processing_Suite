# src_py_v2/AdditionalImageMasking.py
"""
AdditionalImageMasking.py

Minimal placeholder that writes HotPixelMask/NoisyPixels as zeros with the
same spatial/slice dimensions as TMap. This unblocks downstream consumers
expecting these artifacts. Replace with real masking when available.
"""

from __future__ import annotations

import numpy as np
from pathlib import Path
from typing import Any


def AdditionalImageMasking(Sx: Any, TMap: Any) -> None:
    if TMap is None:
        return
    arr = np.asarray(TMap)
    if arr.ndim != 4:
        return
    shape = arr.shape[:3]  # rows, cols, slices
    zeros = np.zeros(shape, dtype=np.uint8)
    path_data = Path(Sx.get("pathData", "."))
    masks_dir = path_data / "Masks"
    masks_dir.mkdir(parents=True, exist_ok=True)
    np.save(masks_dir / "HotPixelMask.npy", zeros)
    np.save(masks_dir / "NoisyPixels.npy", zeros)
