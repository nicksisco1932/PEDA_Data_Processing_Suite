"""
CreateTMaxTDose.py

Python port (best-effort) of CreateTMaxTDose.m.

Notes:
- This relies on precomputed matrices (TMap, Mask, etc.) if present under
  `pathData`. If they are missing and no .mat/.npy equivalents are found,
  the routine will warn and return empty outputs.
- Reading MATLAB .mat files requires scipy; if unavailable, install
  `scipy` in your venv or place .npy/.npz exports in `pathData`.
"""

from __future__ import annotations

import json
import math
import warnings
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

try:  # optional dependency
    from scipy.io import loadmat  # type: ignore
except Exception:  # pragma: no cover
    loadmat = None  # type: ignore

from CalculateDynamicMasks import CalculateDynamicMasks
from ParseRawDataFolder import ParseRawDataFolder
from ReadData import ReadData


def _warn(msg: str) -> None:
    warnings.warn(msg, RuntimeWarning)


def _load_mat_variable(mat_path: Path, var_name: str) -> Optional[np.ndarray]:
    if not mat_path.is_file():
        return None
    if loadmat is None:
        _warn(
            f"scipy not available to read {mat_path}; "
            "install scipy or provide .npy equivalents."
        )
        return None
    try:
        data = loadmat(mat_path)
        return np.asarray(data.get(var_name))
    except Exception as exc:  # pragma: no cover - IO path
        _warn(f"Failed to read {var_name} from {mat_path}: {exc}")
        return None


def _load_cached(path: Path, var_name: str) -> Optional[np.ndarray]:
    """
    Try loading from .npy/.npz or .mat under the path.
    """
    npy = path / f"{var_name}.npy"
    npz = path / f"{var_name}.npz"
    mat = path / f"{var_name}.mat"

    if npy.is_file():
        try:
            return np.load(npy)
        except Exception as exc:  # pragma: no cover
            _warn(f"Failed to load {npy}: {exc}")
    if npz.is_file():
        try:
            with np.load(npz) as f:
                if var_name in f:
                    return f[var_name]
                # fall back to first array
                for key in f.files:
                    return f[key]
        except Exception as exc:  # pragma: no cover
            _warn(f"Failed to load {npz}: {exc}")
    if mat.is_file():
        return _load_mat_variable(mat, var_name)
    return None


def _save_numpy(path: Path, name: str, arr: np.ndarray) -> None:
    try:
        np.save(path / f"{name}.npy", arr)
    except Exception:  # pragma: no cover - IO path
        pass


def _save_table(df: "Any", path: Path, name: str) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True)
        df.to_csv(path / f"{name}.csv", index=False)
        # also save npy for fast reload
        np.save(path / f"{name}.npy", df.to_numpy())
    except Exception:
        pass


def _build_tmap_from_raw(tx: Dict[str, Any]) -> Optional[np.ndarray]:
    """
    Fallback: read Thermometry raw files into a TMap volume.
    """
    session_dir = Path(tx.get("pathSessionFiles", "."))
    therm_dir = session_dir / "Thermometry"
    if not therm_dir.is_dir():
        _warn(f"Thermometry folder not found at {therm_dir}")
        return None
    # Prefer subfolder if only one child, otherwise read from therm_dir
    subdirs = [p for p in therm_dir.iterdir() if p.is_dir()]
    if len(subdirs) == 1:
        therm_dir = subdirs[0]
    files = list(therm_dir.glob("*Current*"))
    if not files:
        _warn(f"No Current* files found under {therm_dir}")
        return None
    dyn_from_files = 0
    for f in files:
        stem = f.stem
        parts = stem.split("-")
        try:
            dyn_val = int(parts[0].lstrip("i")) + 1
            dyn_from_files = max(dyn_from_files, dyn_val)
        except Exception:
            continue
    # Number of dynamics to read matches last ImageNumber but capped to available files
    num_dyn = dyn_from_files
    img_num = tx.get("ImageNumber")
    if img_num is not None:
        arr = np.asarray(img_num).ravel()
        if arr.size:
            num_dyn = min(num_dyn, int(arr[-1]))
    try:
        print(f"[TMAX] Building TMap from raw: dir={therm_dir}, dyn={num_dyn}, files={len(files)}")
        tmap, _ = ReadData(therm_dir, "*Current*", num_dyn, tx.get("Manufacturer", "SP"))
        return tmap
    except Exception as exc:  # pragma: no cover - IO path
        _warn(f"ReadData failed to build TMap from {therm_dir}: {exc}")
        return None


def _ensure_mask(mask: Optional[np.ndarray], shape: Tuple[int, ...]) -> np.ndarray:
    if mask is None:
        return np.ones(shape, dtype=float)
    return mask


def _calc_tdose_and_max(
    TMap: np.ndarray,
    Mask: np.ndarray,
    tx: Dict[str, Any],
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Core TDose and MaxTemperatureTime computation.
    """
    ndyn = TMap.shape[3]
    num_slices = int(tx.get("NumberSlices", TMap.shape[2]))
    image_number = np.asarray(tx.get("ImageNumber"), dtype=int).ravel()
    if image_number.size == 0:
        image_number = np.arange(1, ndyn + 1)
    else:
        # Trim/align to available dynamics
        image_number = image_number[:ndyn] if image_number.size > ndyn else image_number
    dyn_start = max(1, int(image_number[0]))
    dyn_end = min(int(image_number[-1]), ndyn)
    num_ref = int(tx.get("NumRefImages", 5))

    ux = np.asarray(tx.get("ux"))
    uy = np.asarray(tx.get("uy"))
    # Pad ux/uy rows to ndyn if needed
    if ux.shape[0] < ndyn:
        pad_rows = ndyn - ux.shape[0]
        ux = np.vstack([ux, np.repeat(ux[-1:, :], pad_rows, axis=0)])
    if uy.shape[0] < ndyn:
        pad_rows = ndyn - uy.shape[0]
        uy = np.vstack([uy, np.repeat(uy[-1:, :], pad_rows, axis=0)])
    # Pad columns (slices) if fewer than num_slices
    if ux.shape[1] < num_slices:
        pad_cols = num_slices - ux.shape[1]
        ux = np.hstack([ux, np.repeat(ux[:, -1:], pad_cols, axis=1)])
    if uy.shape[1] < num_slices:
        pad_cols = num_slices - uy.shape[1]
        uy = np.hstack([uy, np.repeat(uy[:, -1:], pad_cols, axis=1)])

    shiftUX = np.round(ux - ux[0, :]).astype(int)
    shiftUY = np.round(uy - uy[0, :]).astype(int)
    diffX = np.vstack([shiftUX[0, :], np.diff(shiftUX, axis=0)])
    diffY = np.vstack([shiftUY[0, :], np.diff(shiftUY, axis=0)])

    image_time = np.asarray(tx.get("ImageTime"), dtype=float).ravel()
    if image_time.size < ndyn:
        if image_time.size == 0:
            image_time = np.zeros(ndyn, dtype=float)
        else:
            pad = np.full(ndyn - image_time.size, image_time[-1])
            image_time = np.concatenate([image_time, pad])
    delta_time = np.concatenate([[image_time[0]], np.diff(image_time)]) / 60.0

    thresh = float(tx.get("ThermalDoseThreshold", 43))
    tdose = np.zeros(TMap.shape[:3], dtype=float)
    tdose_masked = np.zeros_like(tdose)

    max_temp_time = TMap[:, :, :, : max(num_ref, 1)].copy()
    if max_temp_time.shape[3] < ndyn:
        pad = np.zeros((*TMap.shape[:3], ndyn - max_temp_time.shape[3]))
        max_temp_time = np.concatenate([max_temp_time, pad], axis=3)

    for slice_idx in range(num_slices):
        if slice_idx == 0:
            print(f"[TMAX] TDose loop: slices={num_slices}, dynEnd={dyn_end}, ndyn={ndyn}")
        arr_tdose = np.zeros(TMap.shape[:2], dtype=float)
        arr_tdose_masked = np.zeros_like(arr_tdose)
        for dyn_idx in range(1, dyn_end + 1):
            offset = dyn_idx - dyn_start + 1
            offset = max(1, min(offset, delta_time.size))
            cur_tmap = TMap[:, :, slice_idx, dyn_idx - 1]
            cur_masked = cur_tmap * Mask[:, :, slice_idx, min(dyn_idx - 1, Mask.shape[3] - 1)]

            if dyn_idx >= num_ref:
                if dyn_idx < dyn_start:
                    max_temp_time[:, :, slice_idx, dyn_idx - 1] = np.maximum(
                        max_temp_time[:, :, slice_idx, dyn_idx - 2], cur_tmap
                    )
                else:
                    if diffX[offset - 1, slice_idx] or diffY[offset - 1, slice_idx]:
                        prev = max_temp_time[:, :, slice_idx, dyn_idx - 2]
                        prev = np.roll(prev, shift=(diffY[offset - 1, slice_idx], diffX[offset - 1, slice_idx]), axis=(0, 1))
                        max_temp_time[:, :, slice_idx, dyn_idx - 1] = np.maximum(prev, cur_tmap)
                    else:
                        max_temp_time[:, :, slice_idx, dyn_idx - 1] = np.maximum(
                            max_temp_time[:, :, slice_idx, dyn_idx - 2], cur_tmap
                        )
            else:
                if dyn_idx > 1:
                    max_temp_time[:, :, slice_idx, dyn_idx - 1] = np.maximum(
                        max_temp_time[:, :, slice_idx, dyn_idx - 2], cur_tmap
                    )
                else:
                    max_temp_time[:, :, slice_idx, dyn_idx - 1] = cur_tmap

            if dyn_idx >= dyn_start:
                dt = delta_time[offset - 1]
                ind43 = cur_tmap >= thresh
                ind_rest = ~ind43
                ind43m = cur_masked >= thresh
                ind_rest_m = ~ind43m
                arr_tdose[ind43] += (0.5 ** (thresh - cur_tmap[ind43])) * dt
                arr_tdose[ind_rest] += (0.25 ** (thresh - cur_tmap[ind_rest])) * dt
                arr_tdose_masked[ind43m] += (0.5 ** (thresh - cur_masked[ind43m])) * dt
                arr_tdose_masked[ind_rest_m] += (0.25 ** (thresh - cur_masked[ind_rest_m])) * dt

        tdose[:, :, slice_idx] = arr_tdose
        tdose_masked[:, :, slice_idx] = arr_tdose_masked

    tmax = max_temp_time[:, :, :, ndyn - 1]
    tmax_masked = tmax * Mask[:, :, :, min(ndyn - 1, Mask.shape[3] - 1)]
    return tdose, tdose_masked, tmax, tmax_masked, max_temp_time


def CreateTMaxTDose(
    TxParameters: Dict[str, Any],
) -> Tuple[Any, Any, Any, Any, Any, Any]:
    """
    Returns (TMap, Anatomy/Magnitude placeholder, MaxTemperatureTime, Mask, TUV, TUVMag)
    """
    # Ensure Manufacturer / ImageNumber for legacy logic
    TxParameters.setdefault("Manufacturer", "Unknown")
    img_num = TxParameters.get("ImageNumber")
    if img_num is None or (isinstance(img_num, (list, tuple)) and len(img_num) == 0):
        TxParameters["ImageNumber"] = list(range(1, 11))
    else:
        arr = np.asarray(img_num)
        if arr.size == 0:
            TxParameters["ImageNumber"] = list(range(1, 11))
        else:
            TxParameters["ImageNumber"] = arr

    session_dir = Path(TxParameters.get("pathSessionFiles", "."))
    case_root = session_dir.parent
    # best-effort Raw parse if available
    try:
        ParseRawDataFolder(str(case_root), FilePattern="*.dat", TUVMin=10, ThermMin=20, Verbose=False)
    except NotImplementedError:
        _warn("ParseRawDataFolder is not yet implemented; skipping raw parsing.")
    except Exception as exc:  # pragma: no cover - IO
        _warn(f"ParseRawDataFolder failed: {exc}")

    path_data = Path(TxParameters.get("pathData", session_dir / "output"))
    path_data.mkdir(parents=True, exist_ok=True)
    # Save controller tables if available
    for filename, name in [
        ("TreatmentControllerData.txt", "TreatmentControllerData"),
        ("HardwareInfo.txt", "HardwareInfo"),
        ("TreatmentControllerInfo.txt", "TreatmentControllerInfo"),
    ]:
        pth = session_dir / filename
        if pth.is_file():
            try:
                df = pd.read_table(pth, sep="\t", engine="python")
                _save_table(df, path_data, name)
            except Exception as exc:
                _warn(f"Failed to save {name}: {exc}")

    # Load matrices
    TMap = _load_cached(path_data, "TMap")
    TUV = _load_cached(path_data, "TUV")
    TUVMag = _load_cached(path_data, "TUVMag")
    Anatomy = _load_cached(path_data, "Anatomy")
    Mask = _load_cached(path_data, "Mask")

    if TMap is None:
        TMap = _build_tmap_from_raw(TxParameters)
    if TMap is None:
        _warn("TMap not found; thermal computation skipped.")
        return None, None, None, None, None, None

    if Mask is None:
        try:
            Mask = CalculateDynamicMasks(TxParameters, Anatomy, TMap)  # type: ignore
        except NotImplementedError:
            Mask = np.ones_like(TMap)
        except Exception as exc:  # pragma: no cover
            _warn(f"CalculateDynamicMasks failed: {exc}")
            Mask = np.ones_like(TMap)

    # Best-effort TUV/TUVMag from raw if missing
    if TUV is None:
        tuv_dir = Path(TxParameters.get("pathSessionFiles", ".")) / "TUV"
        if tuv_dir.is_dir():
            subs = [p for p in tuv_dir.iterdir() if p.is_dir()]
            if len(subs) == 1:
                tuv_dir = subs[0]
            try:
                TUV, TUVMag = ReadData(tuv_dir, "*Uncertainty*", None, TxParameters.get("Manufacturer", "SP"))
            except Exception as exc:  # pragma: no cover
                _warn(f"ReadData failed to load TUV from {tuv_dir}: {exc}")

    # Best-effort Magnitude/Phase
    Magnitude = None
    Phase = None
    therm_dir = Path(TxParameters.get("pathSessionFiles", ".")) / "Thermometry"
    if therm_dir.is_dir():
        subs = [p for p in therm_dir.iterdir() if p.is_dir()]
        therm_leaf = subs[0] if len(subs) == 1 else therm_dir
        try:
            Magnitude, Phase = ReadData(therm_leaf, "*Raw*", None, TxParameters.get("Manufacturer", "SP"))
        except Exception as exc:
            _warn(f"ReadData failed to load Magnitude/Phase from {therm_leaf}: {exc}")
        if Anatomy is None:
            try:
                Anatomy, _ = ReadData(therm_leaf, "*Anatomy*", None, TxParameters.get("Manufacturer", "SP"))
            except Exception as exc:
                _warn(f"ReadData failed to load Anatomy from {therm_leaf}: {exc}")

    tdose, tdose_masked, tmax, tmax_masked, max_temp_time = _calc_tdose_and_max(
        TMap, _ensure_mask(Mask, TMap.shape), TxParameters
    )

    # Persist as .npy for downstream steps
    _save_numpy(path_data, "TMap", TMap)
    if Anatomy is not None:
        _save_numpy(path_data, "Anatomy", Anatomy)
    if Magnitude is not None:
        _save_numpy(path_data, "Magnitude", Magnitude)
    if Phase is not None:
        _save_numpy(path_data, "Phase", Phase)
    if Mask is not None:
        _save_numpy(path_data, "Mask", Mask)
    if TUV is not None:
        _save_numpy(path_data, "TUV", TUV)
    if TUVMag is not None:
        _save_numpy(path_data, "TUVMag", TUVMag)
    _save_numpy(path_data, "TMax", tmax)
    _save_numpy(path_data, "TMaxMasked", tmax_masked)
    _save_numpy(path_data, "TDose", tdose)
    _save_numpy(path_data, "TDoseMasked", tdose_masked)
    _save_numpy(path_data, "MaxTemperatureTime", max_temp_time)

    return TMap, Anatomy, max_temp_time, Mask, TUV, TUVMag
