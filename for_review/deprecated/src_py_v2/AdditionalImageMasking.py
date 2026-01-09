# src_py_v2/AdditionalImageMasking.py
"""
AdditionalImageMasking.py

Best-effort masking:
- Detects hot pixels (temporal outliers) and noisy pixels (high temporal std).
- Writes HotPixelMask.npy and NoisyPixels.npy under Masks/.

If inputs are insufficient, falls back to zero masks.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Tuple

import numpy as np


def _detect_hot_pixels(arr: np.ndarray, k_sigma: float = 8.0) -> np.ndarray:
    """
    Mark pixels that ever exceed median + k_sigma * MAD over time.
    Returns a 3D mask (rows, cols, slices).
    """
    # Compute median and MAD per pixel over dynamics
    med = np.median(arr, axis=3)
    mad = np.median(np.abs(arr - med[:, :, :, None]), axis=3)
    threshold = med + k_sigma * (1.4826 * mad + 1e-6)
    hot = (arr > threshold[:, :, :, None]).any(axis=3)
    return hot.astype(np.uint8)


def _detect_noisy_pixels(arr: np.ndarray, k_rel: float = 5.0) -> np.ndarray:
    """
    Mark pixels with temporal std far above the median std.
    Returns a 3D mask (rows, cols, slices).
    """
    std = np.std(arr, axis=3)
    med_std = np.median(std)
    thr = med_std * k_rel
    noisy = std > thr
    return noisy.astype(np.uint8)


def AdditionalImageMasking(Sx: Any, TMap: Any) -> None:
    if TMap is None:
        return
    arr = np.asarray(TMap)
    if arr.ndim != 4:
        return

    path_data = Path(Sx.get("pathData", "."))
    masks_dir = path_data / "Masks"
    masks_dir.mkdir(parents=True, exist_ok=True)

    try:
        hot_mask = _detect_hot_pixels(arr)
    except Exception:
        hot_mask = np.zeros(arr.shape[:3], dtype=np.uint8)  # fallback
    try:
        noisy_mask = _detect_noisy_pixels(arr)
    except Exception:
        noisy_mask = np.zeros(arr.shape[:3], dtype=np.uint8)  # fallback

    np.save(masks_dir / "HotPixelMask.npy", hot_mask)
    np.save(masks_dir / "NoisyPixels.npy", noisy_mask)
