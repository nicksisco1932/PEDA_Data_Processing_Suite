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


def orient_for_display(img2d: np.ndarray) -> np.ndarray:
    """
    Map Python-arranged thermal slices into the same orientation as the
    legacy MATLAB PEDA figures (A-P, L-R, I-S conventions).
    This is for DISPLAY ONLY.
    """
    return img2d.T


def GenerateMovies(Sx: Any, TMap: Any, Mag: Any, MaxT: Any, TUV: Any, TUVMag: Any) -> None:
    path_data = Path(Sx.get("pathData", "."))
    movies_dir = path_data / "Movies"
    movies_dir.mkdir(parents=True, exist_ok=True)

    if TMap is None:
        print("GenerateMovies: skipped (TMap is None)")
        return
    arr = np.asarray(TMap)
    if arr.ndim != 4:
        print("GenerateMovies: skipped (TMap not 4D)")
        return
    last = arr[:, :, :, -1]
    mid_slice = min(last.shape[2] // 2, last.shape[2] - 1)

    if plt is not None:
        plt.figure(figsize=(4, 4))
        slice2d = orient_for_display(last[:, :, mid_slice])
        plt.imshow(
            slice2d,
            cmap="hot",
            vmin=20.0,
            vmax=86.0,
            origin="lower",
            aspect="equal",
        )
        plt.title(f"TMap last frame slice {mid_slice} (legacy orientation)")
        cbar = plt.colorbar()
        cbar.set_label("Temperature (°C)")
        plt.tight_layout()
        out_png = movies_dir / "TMap_last.png"
        plt.savefig(out_png)
        plt.close()
        print(f"GenerateMovies: wrote {out_png}")
    else:
        print("GenerateMovies: skipped PNG (matplotlib missing)")

    if imageio is not None:
        frames = []
        vmax = np.nanmax(arr[:, :, mid_slice, :])
        vmin = np.nanmin(arr[:, :, mid_slice, :])
        for idx in range(arr.shape[3]):
            frame = orient_for_display(arr[:, :, mid_slice, idx])
            # normalize to 0-255
            if vmax > vmin:
                norm = (frame - vmin) / (vmax - vmin)
            else:
                norm = frame * 0
            frame_uint8 = np.clip(norm * 255, 0, 255).astype(np.uint8)
            frames.append(frame_uint8)
        try:
            out_mp4 = movies_dir / "TMap_middle_slice.mp4"
            imageio.mimsave(out_mp4, frames, fps=10)
            print(f"GenerateMovies: wrote {out_mp4}")
        except Exception as exc:  # pragma: no cover
            print(f"GenerateMovies: failed to write MP4: {exc!r}")
    else:
        print("GenerateMovies: skipped MP4 (imageio missing)")
