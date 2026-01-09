"""
PlotTmax.py

Saves a simple PNG heatmap of TMax (middle slice) to pathData.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

try:
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover
    plt = None


def orient_for_display(img2d: np.ndarray) -> np.ndarray:
    """
    Map Python-arranged thermal slices into the same orientation as the
    legacy MATLAB PEDA figures (A-P, L-R, I-S conventions).
    This is for DISPLAY ONLY.
    """
    return img2d.T


def PlotTmax(ctx: Any) -> None:
    path_data = Path(ctx.get("pathData", "."))
    tmax_path = path_data / "TMax.npy"
    if not tmax_path.is_file() or plt is None:
        return
    try:
        tmax = np.load(tmax_path)
    except Exception:
        return
    if tmax.ndim != 3:
        return
    mid_slice = min(tmax.shape[2] // 2, tmax.shape[2] - 1)
    plt.figure(figsize=(4, 4))
    slice2d = orient_for_display(tmax[:, :, mid_slice])
    plt.imshow(
        slice2d,
        cmap="hot",
        vmin=20.0,
        vmax=86.0,
        origin="lower",
        aspect="equal",
    )
    plt.title(f"TMax last frame (legacy orientation) – slice {mid_slice}")
    cbar = plt.colorbar()
    cbar.set_label("Temperature (°C)")
    plt.tight_layout()
    path_data.mkdir(parents=True, exist_ok=True)
    plt.savefig(path_data / "TMax_slice.png")
    plt.close()
