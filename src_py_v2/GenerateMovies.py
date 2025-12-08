"""
GenerateMovies.py

Best-effort visual outputs:
 - Saves a PNG of the last TMap frame (middle slice).
 - If imageio is available, writes a short MP4 of the middle slice over dynamics.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

try:
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover
    plt = None

try:
    import imageio.v2 as imageio  # type: ignore
except Exception:  # pragma: no cover
    imageio = None


def GenerateMovies(Sx: Any, TMap: Any, Mag: Any, MaxT: Any, TUV: Any, TUVMag: Any) -> None:
    path_data = Path(Sx.get("pathData", "."))
    out_dir = path_data / "Movies"
    out_dir.mkdir(parents=True, exist_ok=True)
    if TMap is None:
        return
    arr = np.asarray(TMap)
    if arr.ndim != 4:
        return
    last = arr[:, :, :, -1]
    mid_slice = min(last.shape[2] // 2, last.shape[2] - 1)

    if plt is not None:
        plt.figure(figsize=(4, 4))
        plt.imshow(last[:, :, mid_slice], cmap="hot")
        plt.title(f"TMap last frame slice {mid_slice}")
        plt.colorbar()
        plt.tight_layout()
        plt.savefig(out_dir / "TMap_last.png")
        plt.close()

    if imageio is not None:
        frames = []
        vmax = np.nanmax(arr[:, :, mid_slice, :])
        vmin = np.nanmin(arr[:, :, mid_slice, :])
        for idx in range(arr.shape[3]):
            frame = arr[:, :, mid_slice, idx]
            # normalize to 0-255
            if vmax > vmin:
                norm = (frame - vmin) / (vmax - vmin)
            else:
                norm = frame * 0
            frame_uint8 = np.clip(norm * 255, 0, 255).astype(np.uint8)
            frames.append(frame_uint8)
        try:
            imageio.mimsave(out_dir / "TMap_middle_slice.mp4", frames, fps=10)
        except Exception:
            pass
