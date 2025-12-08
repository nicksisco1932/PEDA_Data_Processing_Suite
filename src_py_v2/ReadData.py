"""
ReadData.py

Python port of ReadData.m (best-effort).

Reads raw Thermometry/TUV acquisitions and returns magnitude/phase arrays.
The MATLAB version assumes 128x128 slices, zero-based filenames like
"i0000-s00-Raw.dat", and chooses data type based on file size.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, List, Optional, Tuple

import numpy as np


def _compare_file_size(file_list: List[Path]) -> bool:
    """
    Determine dtype based on average file size (~64kB => uint16).
    """
    expected = 65536
    allowable = 0.10
    if not file_list:
        return False
    avg = sum(p.stat().st_size for p in file_list) / len(file_list)
    return abs(avg - expected) <= (expected * allowable)


def ReadData(
    mainFolder: str | Path,
    fileFilter: str,
    NumberOfDynsToRead: Optional[int] = None,
    Manufacturer: str = "SP",
) -> Tuple[np.ndarray, Optional[np.ndarray]]:
    main_path = Path(mainFolder)
    files = sorted(main_path.glob(fileFilter))
    if not files:
        raise FileNotFoundError(f"No files matching {fileFilter} in {main_path}")

    # Infer NumberOfDynsToRead from last filename
    if NumberOfDynsToRead is None:
        last = files[-1].stem  # e.g., i0000-s00-Raw
        parts = last.split("-")
        dyn_str = parts[0].lstrip("i")
        NumberOfDynsToRead = int(dyn_str) + 1

    is_raw = "Raw" in fileFilter
    use_uint16 = _compare_file_size(files)
    dt_scalar = None
    if is_raw:
        if Manufacturer.lower().startswith("ge"):
            dt_scalar = np.int16 if use_uint16 else np.float32
        else:
            dt_scalar = np.uint16 if use_uint16 else np.float32
    else:
        dt_scalar = np.float32

    # preallocate arrays (dyn, slice) -> (128,128,slice,dyn)
    max_dyn = NumberOfDynsToRead
    max_slice = 0
    dyn_slice_indices: List[Tuple[int, int, Path]] = []
    for f in files:
        stem = f.stem
        parts = stem.split("-")
        dyn = int(parts[0].lstrip("i")) + 1
        slc = int(parts[1].lstrip("s")) + 1
        max_slice = max(max_slice, slc)
        if dyn > max_dyn:
            continue
        dyn_slice_indices.append((dyn, slc, f))

    output = np.zeros((128, 128, max_slice, max_dyn), dtype=np.float32)
    phase = np.zeros_like(output) if is_raw else None

    for dyn, slc, f in dyn_slice_indices:
        data = np.fromfile(f, dtype=dt_scalar, count=128 * 128)
        if data.size != 128 * 128:
            # fallback: try float32
            data = np.fromfile(f, dtype=np.float32, count=128 * 128)
        data = data.reshape((128, 128)).T
        output[:, :, slc - 1, dyn - 1] = data
        if is_raw and phase is not None:
            # For Raw, read second block as phase if present
            with f.open("rb") as fh:
                fh.seek(128 * 128 * np.dtype(dt_scalar).itemsize)
                ph_data = np.fromfile(fh, dtype=dt_scalar, count=128 * 128)
                if ph_data.size == 128 * 128:
                    phase[:, :, slc - 1, dyn - 1] = ph_data.reshape((128, 128)).T

    return output, phase
