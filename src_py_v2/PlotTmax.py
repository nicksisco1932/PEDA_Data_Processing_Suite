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
    plt.imshow(tmax[:, :, mid_slice], cmap="hot")
    plt.title(f"TMax slice {mid_slice}")
    plt.colorbar()
    plt.tight_layout()
    path_data.mkdir(parents=True, exist_ok=True)
    plt.savefig(path_data / "TMax_slice.png")
    plt.close()
