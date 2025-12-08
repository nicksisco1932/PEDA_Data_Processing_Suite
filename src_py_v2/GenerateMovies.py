"""
GenerateMovies.py

Placeholder PNG exporter: saves a simple PNG of the last TMap frame per slice.
This is not a full movie; it provides a quick visual artefact for QA.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

try:
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover
    plt = None


def GenerateMovies(Sx: Any, TMap: Any, Mag: Any, MaxT: Any, TUV: Any, TUVMag: Any) -> None:
    path_data = Path(Sx.get("pathData", "."))
    out_dir = path_data / "Movies"
    out_dir.mkdir(parents=True, exist_ok=True)
    if TMap is None or plt is None:
        return
    arr = np.asarray(TMap)
    if arr.ndim != 4:
        return
    last = arr[:, :, :, -1]
    mid_slice = min(last.shape[2] // 2, last.shape[2] - 1)
    plt.figure(figsize=(4, 4))
    plt.imshow(last[:, :, mid_slice], cmap="hot")
    plt.title(f"TMap last frame slice {mid_slice}")
    plt.colorbar()
    plt.tight_layout()
    plt.savefig(out_dir / "TMap_last.png")
    plt.close()
