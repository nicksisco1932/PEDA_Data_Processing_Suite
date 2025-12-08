from __future__ import annotations

import argparse
import json
import math
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

PEDA_VERSION = os.getenv("PEDA_VERSION", "v9.1.3")

# Optional heavy dependencies
try:  # pragma: no cover - optional dependency
    import pandas as _pd
except Exception:  # pragma: no cover
    _pd = None

try:  # pragma: no cover - optional dependency
    import numpy as _np
except Exception:  # pragma: no cover
    _np = None


# -------------------------------------------------------------------------
# Low-level text helpers (ports of MATLAB local functions)
# -------------------------------------------------------------------------
def ReadInitializationData(dir_name: Path) -> List[str]:
    """
    Python analogue of ReadInitializationData.m

    Loads InitializationData.txt from the session folder and returns
    a flat list of strings (similar to MATLAB's cell array of lines).
    """
    dir_name = Path(dir_name)
    path = dir_name / "InitializationData.txt"
    if not path.is_file():
        raise FileNotFoundError(f"InitializationData.txt not found at {path}")

    text = path.read_text(encoding="utf-8", errors="ignore")
    # MATLAB textscan('%s','delimiter','\t') effectively splits on tabs and EOL
    tokens = re.split(r"[\t\r\n]+", text)
    return [t for t in tokens if t]


def ParseInitializationData(lines: List[str], search_string: str) -> str:
    """
    Python analogue of ParseInitializationData.m

    Finds the first line containing `search_string` and returns the substring
    after the first ':' (stripped). Raises if not found.
    """
    for line in lines:
        if search_string in line:
            idx = line.find(":")
            if idx >= 0:
                return line[idx + 1 :].strip()
            return line.strip()
    raise KeyError(f"'{search_string}' not found in InitializationData.txt")


def ReadUAcalibration(dir_name: Path) -> List[str]:
    """
    Python analogue of ReadUAcalibration.m

    Loads HardwareUACalibrationData0.txt and returns a flat list of strings.
    """
    dir_name = Path(dir_name)
    path = dir_name / "HardwareUACalibrationData0.txt"
    if not path.is_file():
        # Not all segments may have this; treat as optional for now.
        raise FileNotFoundError(f"HardwareUACalibrationData0.txt not found at {path}")

    text = path.read_text(encoding="utf-8", errors="ignore")
    tokens = re.split(r"[\t\r\n]+", text)
    return [t for t in tokens if t]


# -------------------------------------------------------------------------
# Pandas helpers
# -------------------------------------------------------------------------
def _require_pandas() -> Any:
    """
    Ensure pandas is importable. Raises a clear error if missing.
    """
    if _pd is None:
        raise ImportError("pandas is required for RetrieveSxParameters log parsing.")
    return _pd


def _require_numpy() -> Any:
    if _np is None:
        raise ImportError("numpy is required for RetrieveSxParameters log parsing.")
    return _np


def _safe_numeric(series: "Any") -> "Any":
    pd = _require_pandas()
    return pd.to_numeric(series, errors="coerce")


def _read_table_tab(path: Path, *, na_values: Optional[List[str]] = None) -> "Any":
    """
    Tab-delimited reader with lenient NA parsing. Returns pandas.DataFrame.
    """
    pd = _require_pandas()
    if not path.is_file():
        raise FileNotFoundError(path)
    return pd.read_table(
        path,
        sep="\t",
        engine="python",
        na_values=na_values or ["Pre-Treatment", "NA", "N/A"],
        keep_default_na=True,
    )


def _unique_last_by_column(df: "Any", col: str) -> "Any":
    """
    Keep the last occurrence of each value in `col`, preserving order of appearance.
    """
    pd = _require_pandas()
    if col not in df.columns:
        return df
    idx = (
        df.reset_index()
        .drop_duplicates(subset=[col], keep="last")
        .sort_values("index")["index"]
    )
    return df.loc[idx]


def _unwrap_angles_deg(angles: "Any") -> "Any":
    """
    Unwrap degrees using numpy.unwrap in radians space, then return degrees.
    """
    np = _require_numpy()
    radians = np.deg2rad(angles)
    unwrapped = np.unwrap(radians)
    return np.rad2deg(unwrapped)


def _safe_to_cartesian(B: "Any") -> Tuple["Any", "Any"]:
    """
    Minimal port of safe_toCartesian.m
    Accepts Nx2 or 2xN, theta in deg or rad; returns column vectors.
    """
    np = _require_numpy()
    arr = np.asarray(B)
    if arr.ndim != 2:
        raise ValueError("Boundary array must be 2-D")
    if arr.shape[0] == 2 and arr.shape[1] != 2:
        r, th = arr[0, :], arr[1, :]
    else:
        r, th = arr[:, 0], arr[:, 1]
    th = np.asarray(th, dtype=float)
    if np.any(np.abs(th) > 2 * math.pi + np.finfo(float).eps):
        th = np.deg2rad(th)
    X = r * np.cos(th)
    Y = r * np.sin(th)
    return X.reshape(-1, 1), Y.reshape(-1, 1)


def _get_treated_sector(unwound_angles: "Any") -> List[int]:
    """
    Python port of GetTreatedSector.m
    """
    np = _require_numpy()
    angles = np.asarray(unwound_angles, dtype=float)
    if angles.size == 0:
        return []
    angular_extent = float(np.nanmax(angles) - np.nanmin(angles))
    if angular_extent >= 355:
        return list(range(360))
    lo = int(round(np.nanmin(angles)))
    hi = int(round(np.nanmax(angles)))
    return [int(x % 360) for x in range(lo, hi + 1)]


def _get_treated_sector_subsegment(
    angles: "Any", subsegment_image_numbers: List[int], image_numbers: "Any"
) -> Tuple[List[List[int]], List[int]]:
    """
    Port of GetTreatedSectorSubsegment.m
    """
    np = _require_numpy()
    angles = np.asarray(angles, dtype=float)
    image_numbers = np.asarray(image_numbers, dtype=float)
    treated_per = []
    # Map subsegments to indices in ThermAngle
    offsets = []
    if image_numbers.size == 0:
        offsets = [0 for _ in subsegment_image_numbers]
        treated_per = [_get_treated_sector([]) for _ in subsegment_image_numbers]
        combined = _get_treated_sector([])
        return treated_per, combined
    for x in subsegment_image_numbers:
        matches = np.where(image_numbers == x)[0]
        if matches.size:
            offsets.append(int(matches[0]))
        else:
            offsets.append(len(image_numbers) - 1)
    for idx, offset in enumerate(offsets):
        start = 0 if idx == 0 else offsets[idx - 1] + 1
        stop = offset + 1
        treated_per.append(_get_treated_sector(angles[start:stop]))
    combined = _get_treated_sector([x for seg in treated_per for x in seg])
    return treated_per, combined


def _parse_ua_calibration(tokens: List[str], sw_nums: List[int]) -> Dict[str, Any]:
    """
    Port of the UA calibration block. Returns a nested UA dict.
    """
    ua: Dict[str, Any] = {}
    try:
        ua["Schema"] = int(ParseInitializationData(tokens, "Schema"))
    except Exception:
        ua["Schema"] = math.nan
    try:
        ua["SerialNumber"] = ParseInitializationData(tokens, "Serial Number")
    except Exception:
        ua["SerialNumber"] = ""
    try:
        ua["ProductionDate"] = ParseInitializationData(tokens, "Production Date")
    except Exception:
        ua["ProductionDate"] = ""
    for key, search in [
        ("LowFrequency", "Common Low Frequency"),
        ("HighFrequency", "Common High Frequency"),
        ("BeamAlign", "Beam Align Degrees"),
    ]:
        try:
            ua[key] = float(ParseInitializationData(tokens, search))
        except Exception:
            ua[key] = math.nan

    ua["LowFrequencyEfficiency"] = []
    ua["HighFrequencyEfficiency"] = []
    if ua["Schema"] == 1:
        try:
            offset_low = next(
                i for i, t in enumerate(tokens) if "Efficiency - Low Frequencies:" in t
            )
            offset_high = next(
                i for i, t in enumerate(tokens) if "Efficiency - High Frequencies:" in t
            )
        except StopIteration:
            return ua

        def _extract_block(offset: int) -> List[float]:
            vals: List[float] = []
            for i in range(1, 11):
                try:
                    vals.append(float(tokens[offset + i]))
                except Exception:
                    break
            return vals

        major = sw_nums[0] if sw_nums else 0
        minor = sw_nums[1] if len(sw_nums) > 1 else 0
        patch = sw_nums[2] if len(sw_nums) > 2 else 0
        build = sw_nums[3] if len(sw_nums) > 3 else 0

        if major == 2 and minor < 9:
            ua["LowFrequencyEfficiency"] = _extract_block(offset_low + 12)
            ua["HighFrequencyEfficiency"] = _extract_block(offset_high + 12)
        elif major == 2 and minor == 9 and patch >= 0 and build >= 4847:
            ua["LowFrequencyEfficiency"] = _extract_block(offset_low)
            ua["HighFrequencyEfficiency"] = _extract_block(offset_high)

    return ua


def _parse_boundaries(
    init_data: List[str],
    sw_nums: List[int],
    image_numbers: List[int],
    pixel_size: float,
) -> Dict[str, Any]:
    """
    Port of the boundary parsing and treated sector logic.
    """
    np = _require_numpy()
    result: Dict[str, Any] = {}
    idx_boundary = [i for i, l in enumerate(init_data) if "Radius_mm" in l]
    idx_boundary_change = [
        i for i, l in enumerate(init_data) if re.search(r"CurrentDynamicNumber:\s*\d+$", l)
    ]
    if not idx_boundary:
        return result

    num_subsegments = len(idx_boundary) // 2
    pairs = list(zip(idx_boundary[0::2], idx_boundary[1::2]))
    if num_subsegments > 1 and len(sw_nums) > 1 and sw_nums[1] > 6:
        idx_boundary_change_pairs = list(zip(idx_boundary_change[0::2], idx_boundary_change[1::2]))
        result["SubSegmentImageNumber"] = [1]
    else:
        idx_boundary_change_pairs = []
        result["SubSegmentImageNumber"] = [1, (image_numbers[-1] if image_numbers else 1)]
        num_subsegments = 1

    prostate_mm = []
    control_mm = []
    prostate_theta = None
    control_theta = None

    for seg_idx, (idx1, idx2) in enumerate(pairs):
        # MATLAB is 1-based; here adjust to 0-based.
        start1 = idx1 + 1
        start2 = idx2 + 1
        span = 10 * 360 * 3
        indices1 = list(range(start1, start1 + span))
        indices2 = list(range(start2, start2 + span))

        def _collect(indices: List[int]) -> List[Tuple[float, float, float]]:
            out: List[Tuple[float, float, float]] = []
            for k in range(0, len(indices), 3):
                try:
                    a = float(init_data[indices[k]])
                    b = float(init_data[indices[k] + 1])
                    c = float(init_data[indices[k] + 2])
                    out.append((a, b, c))
                except Exception:
                    break
            return out

        prostate_temp = _collect(indices1)
        control_temp = _collect(indices2)
        if not prostate_temp or not control_temp:
            continue

        prostate_arr = np.asarray(prostate_temp)
        control_arr = np.asarray(control_temp)
        if prostate_theta is None:
            prostate_theta = prostate_arr[:360, 1]
        if control_theta is None:
            control_theta = control_arr[:360, 1]

        def _reshape(arr: "Any") -> "Any":
            slice_data = arr[:, 2]
            try:
                reshaped = slice_data.reshape((360, 10))
            except Exception:
                reshaped = np.full((360, 10), np.nan)
            return np.concatenate([np.zeros((360, 1)), reshaped, np.zeros((360, 1))], axis=1)

        prostate_mm.append(_reshape(prostate_arr))
        control_mm.append(_reshape(control_arr))

        if idx_boundary_change_pairs:
            try:
                temp_line = init_data[idx_boundary_change_pairs[seg_idx][0]]
                dyn_num = int(re.findall(r"\d+", temp_line)[-1]) + 1
            except Exception:
                dyn_num = math.nan
            result["SubSegmentImageNumber"].append(dyn_num)

    # Align subsegment list to MATLAB expectations (append final image)
    if "SubSegmentImageNumber" in result and image_numbers:
        if result["SubSegmentImageNumber"][-1] != image_numbers[-1]:
            result["SubSegmentImageNumber"].append(int(image_numbers[-1]))

    result["ProstateBoundaryTheta"] = (
        np.asarray(prostate_theta) if prostate_theta is not None else np.array([])
    )
    result["ControlBoundaryTheta"] = (
        np.asarray(control_theta) if control_theta is not None else np.array([])
    )
    if prostate_mm:
        result["ProstateBoundaryMM"] = np.stack(prostate_mm, axis=2)
        result["ProstateBoundary"] = result["ProstateBoundaryMM"] / pixel_size
    if control_mm:
        result["ControlBoundaryMM"] = np.stack(control_mm, axis=2)
        result["ControlBoundary"] = result["ControlBoundaryMM"] / pixel_size

    # Cleanup SubSegmentImageNumber
    if "SubSegmentImageNumber" in result:
        arr = [int(x) if not (isinstance(x, float) and math.isnan(x)) else 1 for x in result["SubSegmentImageNumber"]]
        result["SubSegmentImageNumber"] = arr

    return result


def _image_number_column(df: "Any") -> str:
    """
    Best-effort pick of the image number column.
    Prefers 'ImageNumber', then 'TdcDynamicNumber', else the second column.
    """
    for cand in ("ImageNumber", "TdcDynamicNumber"):
        if cand in df.columns:
            return cand
    return df.columns[1]


# -------------------------------------------------------------------------
# Main port: RetrieveSxParameters (partial, geometry/config only)
# -------------------------------------------------------------------------
def RetrieveSxParameters(Sx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Python analogue of the *upper half* of RetrieveSxParameters.m, structured
    as an enrichment step on an existing Sx dict.

    Expected pre-populated keys in Sx:
      - 'pathSessionFiles' : Path-like; session folder for this segment
      - 'pathPEDA'         : Path-like; PEDA output root for the case
      - 'pathData'         : Path-like; segment-specific PEDA output folder
      - 'segmentIdx'       : int, 1-based segment index (optional)
      - 'PatientID'        : optional; if missing, will be derived from pathData

    This version:
      - Ensures pathPEDA / pathData exist.
      - Fills manufacturer / software version / SWVersion.
      - Populates static imaging geometry and thermal configuration fields.
      - Adds a note flagging that dynamic logs, boundaries, UA calibration,
        and thermal-boost logic are still TODO.
    """
    # Normalize and ensure paths
    path_session_files = Path(Sx["pathSessionFiles"]).resolve()
    path_peda = Path(Sx.get("pathPEDA", path_session_files.parent)).resolve()
    seg_idx = int(Sx.get("segmentIdx", 1))
    path_data = Path(Sx.get("pathData", path_peda / f"Segment {seg_idx}")).resolve()

    path_peda.mkdir(parents=True, exist_ok=True)
    path_data.mkdir(parents=True, exist_ok=True)

    Sx["pathSessionFiles"] = path_session_files
    Sx["pathPEDA"] = path_peda
    Sx["pathData"] = path_data
    Sx["segmentIdx"] = seg_idx

    # PatientID: keep existing if present, otherwise derive from pathData
    if not Sx.get("PatientID"):
        m = re.search(r"\d{3}_\d{2}-\d{3}", str(path_data))
        if m:
            Sx["PatientID"] = m.group(0)

    # ------------------------------------------------------------------
    # First loop: InitializationData and basic metadata
    # ------------------------------------------------------------------
    init_data = ReadInitializationData(path_session_files)

    # Manufacturer
    try:
        Sx["Manufacturer"] = ParseInitializationData(
            init_data, "MRI cartridge information - Name"
        )
    except KeyError:
        Sx["Manufacturer"] = ""

    # Software version
    try:
        Sx["SoftwareVersion"] = ParseInitializationData(
            init_data, "Treatment controller version"
        )
    except KeyError:
        Sx["SoftwareVersion"] = ""

    # Parse numeric SWVersion = [major minor patch build ...]
    sw_nums: List[int] = []
    if Sx["SoftwareVersion"]:
        sw_nums = [int(x) for x in re.split(r"[^\d]+", Sx["SoftwareVersion"]) if x]
    Sx["SWVersion"] = sw_nums

    # ------------------------------------------------------------------
    # Static imaging geometry and thermal config
    # ------------------------------------------------------------------
    # SliceThickness: hard-coded 5 mm (until TDC-4721 is fixed)
    Sx["SliceThickness"] = 5.0

    # ImageResolution
    try:
        Sx["ImageResolution"] = float(
            ParseInitializationData(init_data, "Image Resolution")
        )
    except KeyError:
        Sx["ImageResolution"] = math.nan

    # Frequency Encode FOV
    try:
        Sx["FOV"] = float(
            ParseInitializationData(init_data, "Frequency Encode Field-of-View")
        )
    except KeyError:
        Sx["FOV"] = math.nan

    # Number of rows / columns
    try:
        image_size_str = ParseInitializationData(
            init_data, "Number of Rows and Columns of Data"
        )
        nums = [int(n) for n in re.findall(r"\d+", image_size_str)]
        if len(nums) >= 2:
            Sx["NumberOfRows"] = nums[0]
            Sx["NumberOfCols"] = nums[1]
    except KeyError:
        # Leave absent if not found
        pass

    # Number of slices
    try:
        Sx["NumberSlices"] = float(
            ParseInitializationData(init_data, "Number of Slices")
        )
    except KeyError:
        Sx["NumberSlices"] = math.nan

    # Temperatures
    try:
        Sx["Tc"] = float(
            ParseInitializationData(init_data, "Control temperature")
        )
    except KeyError:
        Sx["Tc"] = math.nan

    try:
        Sx["Tb"] = float(
            ParseInitializationData(init_data, "Patient temperature")
        )
    except KeyError:
        Sx["Tb"] = math.nan

    # Rotation direction + MR center frequency
    try:
        Sx["InitialRotationDirection"] = ParseInitializationData(
            init_data, "Rotary rotation direction"
        )
    except KeyError:
        Sx["InitialRotationDirection"] = ""

    try:
        Sx["MRCenterFrequency"] = ParseInitializationData(
            init_data, "MR Center Frequency"
        )
    except KeyError:
        Sx["MRCenterFrequency"] = ""

    # Pixel size (mm/pixel) if FOV and ImageResolution present
    FOV = Sx.get("FOV")
    img_res = Sx.get("ImageResolution")
    if (
        isinstance(FOV, (int, float))
        and isinstance(img_res, (int, float))
        and img_res not in (0, math.inf)
        and not math.isnan(img_res)
        and not math.isnan(FOV)
    ):
        Sx["PixelSize"] = FOV / img_res
    else:
        Sx["PixelSize"] = math.nan

    # Thermal dose thresholds
    Sx["ThermalDoseThreshold"] = 43
    Sx["ThermalDoseCEM"] = 240

    # Radii etc.
    pix = Sx.get("PixelSize")
    if isinstance(pix, (int, float)) and pix > 0 and math.isfinite(pix):
        pix_valid = True
    else:
        pix_valid = False

    try:
        min_tr_mm = float(
            ParseInitializationData(init_data, "Minimum treatment radius")
        ) - 2.0  # TS-301 fix
    except KeyError:
        min_tr_mm = math.nan

    Sx["MinimumTreatmentRadiusMM"] = min_tr_mm
    Sx["MinimumTreatmentRadius"] = (
        (min_tr_mm / pix) if pix_valid and not math.isnan(min_tr_mm) else math.nan
    )

    Sx["MaximumTreatmentRadiusMM"] = 28.0
    Sx["MaximumTreatmentRadius"] = (
        (28.0 / pix) if pix_valid else math.nan
    )

    Sx["UARadiusMM"] = 4.0
    Sx["UARadius"] = (4.0 / pix) if pix_valid else math.nan

    Sx["StabilityThreshold"] = 10
    Sx["TUVThreshold"] = 2

    # PS position (SWVersion(2) > 5)
    if len(sw_nums) >= 2 and sw_nums[1] > 5:
        try:
            Sx["PSposition"] = ParseInitializationData(init_data, "PS position")
        except KeyError:
            # Not fatal; keep absent
            pass

    # ------------------------------------------------------------------
    # Dynamic logs: TreatmentControllerData, HardwareInfo, ControllerInfo
    # ------------------------------------------------------------------
    pd = _require_pandas()
    np = _require_numpy()
    na_vals = ["Pre-Treatment", "NA", "N/A"]
    notes = Sx.setdefault("_notes", [])

    tcd_path = path_session_files / "TreatmentControllerData.txt"
    hwi_path = path_session_files / "HardwareInfo.txt"
    tci_path = path_session_files / "TreatmentControllerInfo.txt"

    tcd = hwi = tci = None
    try:
        tcd = _read_table_tab(tcd_path, na_values=na_vals)
    except Exception as exc:  # pragma: no cover - IO path
        notes.append(f"TreatmentControllerData load failed: {exc}")
    try:
        hwi = _read_table_tab(hwi_path, na_values=na_vals)
    except Exception as exc:  # pragma: no cover - IO path
        notes.append(f"HardwareInfo load failed: {exc}")
    try:
        tci = _read_table_tab(tci_path, na_values=na_vals)
    except Exception as exc:  # pragma: no cover - IO path
        notes.append(f"TreatmentControllerInfo load failed: {exc}")

    # Normalize ImageNumber column in tcd/hwi
    image_col_tcd = None
    if tcd is not None:
        if "ImageNumber" not in tcd.columns and "TdcDynamicNumber" in tcd.columns:
            tcd = tcd.rename(columns={"TdcDynamicNumber": "ImageNumber"})
        image_col_tcd = _image_number_column(tcd)
        Sx["ImageNumber"] = _safe_numeric(tcd[image_col_tcd]).fillna(0).to_numpy() + 1
        Sx["ImageTime"] = _safe_numeric(tcd.get("ElapsedTime_sec", pd.Series([]))).fillna(0).to_numpy()
        Sx["NumRefImages"] = 5
        if "ControlAngle_deg" in tcd.columns:
            Sx["ThermAngle"] = _safe_numeric(tcd["ControlAngle_deg"]).to_numpy()
        elif "ThermAngle" in tcd.columns:
            Sx["ThermAngle"] = _safe_numeric(tcd["ThermAngle"]).to_numpy()
        else:
            Sx["ThermAngle"] = np.array([])
        if Sx["ThermAngle"].size:
            Sx["UnwoundThermAngle"] = _unwrap_angles_deg(Sx["ThermAngle"])
        else:
            Sx["UnwoundThermAngle"] = np.array([])
        # Approaching boiling threshold
        if "TemperatureApproachingBoilingLevelThreshold" in tcd.columns:
            Sx["ApproachingBoilingThreshold"] = (
                _safe_numeric(tcd["TemperatureApproachingBoilingLevelThreshold"])
                .fillna(86)
                .to_numpy()
            )
        else:
            last_idx = Sx["ImageNumber"][-1] if ("ImageNumber" in Sx and len(Sx["ImageNumber"])) else 1
            Sx["ApproachingBoilingThreshold"] = np.full(int(last_idx), 86)

    # HardwareInfo-based UA element activity and power/frequency
    if hwi is not None:
        if "ImageNumber" not in hwi.columns and "TdcDynamicNumber" in hwi.columns:
            hwi = hwi.rename(columns={"TdcDynamicNumber": "ImageNumber"})
        hwi = hwi.dropna(subset=["ElapsedTime_sec"]) if "ElapsedTime_sec" in hwi.columns else hwi
        hwi = _unique_last_by_column(hwi, "ImageNumber")
        if "ImageNumber" in hwi.columns:
            hwi = hwi[hwi["ImageNumber"] > 23]
        enabled_mat = []
        for idx in range(1, 11):
            col = f"IsActive_E{idx}"
            if col in hwi.columns:
                enabled_mat.append(hwi[col].astype(str).str.lower().eq("true").to_numpy())
        if enabled_mat:
            enabled = np.vstack(enabled_mat).T  # dyn x elements (10)
            # Align to tcd ImageNumber (1-based) so shapes match
            dyn_len = len(Sx.get("ImageNumber", []))
            enabled_full = np.zeros((dyn_len, 12), dtype=bool)  # pad M0/M11
            hwi_img = _safe_numeric(hwi.get("ImageNumber", pd.Series([]))).astype(int).to_numpy()
            for row_idx, img_zero_based in enumerate(hwi_img):
                matches = np.where(Sx.get("ImageNumber", []) == (img_zero_based + 1))[0]
                if matches.size:
                    enabled_full[matches[0], 1:-1] = enabled[row_idx]
            Sx["IsElementEnabled"] = enabled_full
            Sx["isUAactive"] = bool(enabled_full.any())
        if tcd is not None and "TreatmentState" in tcd.columns:
            paused_raw = tcd["TreatmentState"].astype(str).str.lower().eq("paused").to_numpy()
            paused = np.repeat(paused_raw[:, None], 10, axis=1)
            paused = np.pad(paused, ((0, 0), (1, 1)), constant_values=False)
            Sx["IsPaused"] = paused
            if "IsElementEnabled" in Sx:
                min_rows = min(Sx["IsElementEnabled"].shape[0], paused.shape[0])
                with np.errstate(invalid="ignore"):
                    Sx["IsElementEnabled"] = Sx["IsElementEnabled"][:min_rows] & (~paused[:min_rows])

        # Element frequencies/powers
        freq_cols = [f"Frequency_E{i}" for i in range(1, 11)]
        power_cols = [f"PowerNetWa_E{i}" for i in range(1, 11)]
        if all(c in hwi.columns for c in freq_cols):
            freqs = hwi[freq_cols].to_numpy()
            Sx["elementFrequencies"] = np.pad(freqs, ((0, 0), (1, 1)), constant_values=0)
        if all(c in hwi.columns for c in power_cols):
            powers = hwi[power_cols].to_numpy()
            Sx["elementPowers"] = np.pad(powers, ((0, 0), (1, 1)), constant_values=0)

    # UA center coordinates (either dynamic columns or static from InitializationData)
    if tcd is not None:
        ux_cols = [c for c in tcd.columns if c.startswith("UACenterInPixelsX_")]
        uy_cols = [c for c in tcd.columns if c.startswith("UACenterInPixelsY_")]
    else:
        ux_cols = uy_cols = []
    if ux_cols and uy_cols:
        ux = tcd[ux_cols].to_numpy(dtype=float) + 0.5  # type: ignore
        uy = tcd[uy_cols].to_numpy(dtype=float) + 0.5  # type: ignore
        Sx["ux"] = ux
        Sx["uy"] = uy
    else:
        try:
            idx_ua = next(i for i, l in enumerate(init_data) if "Urethra Center" in l)
            temp = init_data[idx_ua]
            coords = re.findall(r"[-+]?\d*\.?\d+", temp)
            ux_val = float(coords[0]) + 0.5
            uy_val = float(coords[1]) + 0.5
            dyn_len = len(Sx.get("ImageNumber", []))
            slices = Sx.get("NumberSlices", 12)
            Sx["ux"] = np.full((dyn_len, slices), ux_val)
            Sx["uy"] = np.full((dyn_len, slices), uy_val)
        except Exception:
            pass

    # Boundaries and treated sectors
    if tcd is not None:
        boundary_info = _parse_boundaries(
            init_data,
            sw_nums,
            list(Sx.get("ImageNumber", [])),
            Sx.get("PixelSize", math.nan),
        )
        Sx.update(boundary_info)
        sub_seg = boundary_info.get("SubSegmentImageNumber")
        if boundary_info and sub_seg and len(sub_seg) > 1:
            treated, combined = _get_treated_sector_subsegment(
                Sx.get("ThermAngle", []), sub_seg, Sx.get("ImageNumber", [])
            )
            Sx["TreatedSector"] = treated
            Sx.setdefault("Combined", {})["TreatedSector"] = combined
        else:
            Sx["TreatedSector"] = _get_treated_sector(Sx.get("ThermAngle", []))
            Sx["Combined"] = {}

    # UA calibration
    try:
        ua_tokens = ReadUAcalibration(path_session_files)
        Sx["UA"] = _parse_ua_calibration(ua_tokens, sw_nums)
    except FileNotFoundError:
        notes.append("UA calibration file missing.")
    except Exception as exc:  # pragma: no cover - IO path
        notes.append(f"UA calibration parse failed: {exc}")

    # Thermal boost logic (best-effort)
    if len(sw_nums) >= 2 and (sw_nums[0] >= 2 and sw_nums[1] >= 10):
        tb_info: Dict[str, Any] = {}
        num_images = len(Sx.get("ImageNumber", []))
        num_elements = Sx.get("NumberSlices", 12) - 2
        controller_state = None
        if tci is not None and tci.shape[1] >= 5:
            # Column 4 (0-based) mirrors MATLAB TreatmentControllerInfo{1,5}
            col = tci.columns[4]
            try:
                controller_state_raw = tci[col].astype(str).to_numpy()
                # reshape NumElements x NumImages, then transpose
                controller_state = controller_state_raw.reshape((num_elements, num_images)).T
            except Exception:
                controller_state = None
        tb_info["TreatmentState"] = np.zeros((360, 12), dtype=int)
        tb_info["ControlBoundaryMM"] = np.ones((360, 12)) * Sx.get(
            "MinimumTreatmentRadiusMM", math.nan
        )
        tb_info["ElapsedTime_sec"] = 0.0
        Sx["ThermalBoostInfo"] = tb_info
        if controller_state is not None and Sx.get("ControlBoundaryMM") is not None:
            control_boundary_mm = Sx["ControlBoundaryMM"]
            angles_treated_per_dyn: List[List[Optional[List[int]]]] = [
                [None for _ in range(12)] for _ in range(num_images)
            ]
            is_power_on = None
            if tcd is not None and "TreatmentState" in tcd.columns and "IsElementEnabled" in Sx:
                is_delivery = (
                    tcd["TreatmentState"].astype(str).str.lower().eq("delivery").to_numpy()
                )
                is_power_on = (Sx["IsElementEnabled"] & is_delivery[:, None])
            for slice_idx in range(12):
                if not Sx.get("isUAactive", True):
                    continue
                for dyn_idx in range(2, num_images):
                    if is_power_on is not None and not is_power_on[dyn_idx, slice_idx]:
                        continue
                    start_end = [
                        round(Sx["ThermAngle"][dyn_idx - 1]),
                        round(Sx["ThermAngle"][dyn_idx]),
                    ]
                    span = list(range(min(start_end), max(start_end) + 1))
                    if len(span) > 180:
                        span = list(range(0, min(start_end) + 1)) + list(
                            range(max(start_end), 360)
                        )
                    angles_treated_per_dyn[dyn_idx - 1][slice_idx] = span

            # Rank per angle
            for slice_idx in range(12):
                for angle_idx in range(360):
                    rows = []
                    for dyn_idx in range(num_images):
                        span = angles_treated_per_dyn[dyn_idx][slice_idx]
                        if span and (angle_idx if angle_idx < 360 else 0) in span:
                            rows.append(dyn_idx)
                    if not rows:
                        continue
                    states = []
                    ctrl_mm = []
                    for dyn_idx in rows:
                        state_val = controller_state[dyn_idx, slice_idx]
                        states.append(0 if pd.isna(state_val) else int(state_val == "Boosted") + int(state_val == "Enabled"))
                        subsegment = 0
                        if "SubSegmentImageNumber" in Sx:
                            for i, val in enumerate(Sx["SubSegmentImageNumber"]):
                                if dyn_idx + 1 <= val:
                                    subsegment = i
                                    break
                        try:
                            ctrl_mm.append(
                                control_boundary_mm[angle_idx, slice_idx, subsegment]
                            )
                        except Exception:
                            ctrl_mm.append(Sx.get("MinimumTreatmentRadiusMM", math.nan))
                    # Remove disabled (state 0)
                    states_arr = np.asarray(states)
                    ctrl_arr = np.asarray(ctrl_mm)
                    keep = states_arr > 0
                    if not keep.any():
                        tb_info["TreatmentState"][angle_idx, slice_idx] = 0
                        tb_info["ControlBoundaryMM"][angle_idx, slice_idx] = Sx.get(
                            "MinimumTreatmentRadiusMM", math.nan
                        )
                    else:
                        ctrl_keep = ctrl_arr[keep]
                        state_keep = states_arr[keep]
                        # rank by control boundary then state
                        rank_idx = np.lexsort((state_keep * -1, ctrl_keep * -1))
                        pick = rank_idx[0]
                        tb_info["TreatmentState"][angle_idx, slice_idx] = state_keep[pick]
                        tb_info["ControlBoundaryMM"][angle_idx, slice_idx] = ctrl_keep[pick]

            dyn_tb = (
                controller_state == "Boosted" if controller_state is not None else np.array([])
            )
            if dyn_tb.size:
                dyn_any = dyn_tb.any(axis=1)
                time_elapsed = np.diff(
                    np.concatenate([[0], Sx.get("ImageTime", np.zeros(num_images))])
                )
                tb_info["ElapsedTime_sec"] = float(time_elapsed[dyn_any].sum())

    notes.append("RetrieveSxParameters: geometry, dynamic logs, boundaries, UA calibration, and thermal-boost (best-effort) populated.")

    return Sx


# -------------------------------------------------------------------------
# CLI for debugging / inspection
# -------------------------------------------------------------------------
def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect SxParameters for a single segment (Python port)."
    )
    parser.add_argument(
        "--session-dir",
        required=True,
        help="Path to the segment session folder (where InitializationData.txt lives).",
    )
    parser.add_argument(
        "--path-peda",
        default=None,
        help="Optional PEDA root for the case; defaults to parent of session dir.",
    )
    parser.add_argument(
        "--segment-idx",
        type=int,
        default=1,
        help="Segment index (1-based). Default: 1",
    )
    parser.add_argument(
        "--patient-id",
        default="",
        help="Optional explicit PatientID; if omitted, derived from pathData when possible.",
    )

    args = parser.parse_args()
    session_dir = Path(args.session_dir).resolve()
    path_peda = (
        Path(args.path_peda).resolve()
        if args.path_peda
        else session_dir.parent
    )

    sx_seed: Dict[str, Any] = {
        "pathSessionFiles": session_dir,
        "pathPEDA": path_peda,
        "pathData": path_peda / f"Segment {args.segment_idx}",
        "segmentIdx": args.segment_idx,
        "PatientID": args.patient_id,
    }

    print(f"[DEBUG] session_dir = {session_dir}")
    print(f"[DEBUG] path_peda   = {path_peda}")

    sx_out = RetrieveSxParameters(sx_seed)

    summary_keys = [
        "PatientID",
        "Manufacturer",
        "SoftwareVersion",
        "SWVersion",
        "SliceThickness",
        "FOV",
        "ImageResolution",
        "NumberOfRows",
        "NumberOfCols",
        "NumberSlices",
        "PixelSize",
        "MinimumTreatmentRadiusMM",
        "MaximumTreatmentRadiusMM",
    ]
    summary = {k: sx_out.get(k) for k in summary_keys}
    print("\nSummary:")
    print(json.dumps(summary, indent=2, default=str))

    if "_notes" in sx_out:
        print("\nNotes:")
        for n in sx_out["_notes"]:
            print("  -", n)


if __name__ == "__main__":
    _cli()
