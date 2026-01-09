"""
CalculateDynamicMasks.py

Best-effort dynamic mask generation:
- Creates a treatment-region mask (circular) using geometry from TxParameters when available.
- Falls back to unity if inputs are insufficient.

NOTE: Replace with full MATLAB-equivalent logic when available.
"""

from __future__ import annotations

from typing import Any, Tuple

import numpy as np


def _treatment_region_mask(
    shape_rcs: Tuple[int, int, int], tx: Any
) -> np.ndarray:
    rows, cols, slices = shape_rcs
    pixel_size = tx.get("PixelSize")
    max_rad_mm = tx.get("MaximumTreatmentRadiusMM")
    # Fallback radius if metadata is missing
    if pixel_size and max_rad_mm:
        radius_px = float(max_rad_mm) / float(pixel_size)
    else:
        radius_px = min(rows, cols) / 2.0
    yy, xx = np.ogrid[:rows, :cols]
    cy = (rows - 1) / 2.0
    cx = (cols - 1) / 2.0
    dist = np.sqrt((yy - cy) ** 2 + (xx - cx) ** 2)
    base = (dist <= radius_px).astype(float)
    return np.repeat(base[:, :, None], slices, axis=2)


def CalculateDynamicMasks(*args: Any, **kwargs: Any) -> Any:
    """
    Args (best-effort):
        TxParameters: dict-like
        Anatomy: optional np.ndarray
        TMap: np.ndarray (NRows x NCols x NSlices x NDyn)
    """
    if len(args) >= 2:
        tmap = args[-1]
        tx = args[0] if len(args) > 0 else {}
    else:
        raise NotImplementedError("CalculateDynamicMasks needs TMap as last arg.")

    arr = np.asarray(tmap)
    if arr.ndim != 4:
        return np.ones_like(arr, dtype=float)

    rows, cols, slices, dyn = arr.shape
    try:
        region = _treatment_region_mask((rows, cols, slices), tx)
        mask = np.repeat(region[:, :, :, None], dyn, axis=3)
        return mask.astype(float)
    except Exception:
        # Safe fallback if metadata is missing
        return np.ones_like(arr, dtype=float)
