"""
AnalyzeHardwareLogs.py

Lightweight Python analogue that summarizes HardwareInfo/TreatmentControllerData
into CSV/JSON for QA. This is not a full MATLAB feature parity, but captures
basic per-element enable counts and power/frequency stats.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd


def _load_df(path: Path) -> pd.DataFrame:
    if path.is_file():
        return pd.read_csv(path)
    raise FileNotFoundError(path)


def AnalyzeHardwareLogs(Sx: Any) -> None:
    path_data = Path(Sx.get("pathData", "."))
    hw_csv = path_data / "HardwareInfo.csv"
    tcd_csv = path_data / "TreatmentControllerData.csv"
    out_dir = path_data / "HWAnalysis"
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        hw_df = _load_df(hw_csv)
    except Exception:
        return

    summary: Dict[str, Any] = {}
    # Element enable counts
    enable_cols = [c for c in hw_df.columns if c.startswith("IsActive_E")]
    if enable_cols:
        enabled_counts = {
            col: int(hw_df[col].astype(str).str.lower().eq("true").sum())
            for col in enable_cols
        }
        summary["EnabledCounts"] = enabled_counts

    # Frequency / Power stats
    freq_cols = [c for c in hw_df.columns if c.startswith("Frequency_E")]
    power_cols = [c for c in hw_df.columns if c.startswith("PowerNetWa_E")]
    if freq_cols:
        summary["FrequencyMean"] = {
            col: float(hw_df[col].dropna().mean()) for col in freq_cols
        }
    if power_cols:
        summary["PowerMean"] = {col: float(hw_df[col].dropna().mean()) for col in power_cols}

    # TreatmentControllerData timing
    if tcd_csv.is_file():
        try:
            tcd = _load_df(tcd_csv)
            if "ElapsedTime_sec" in tcd.columns:
                et = pd.to_numeric(tcd["ElapsedTime_sec"], errors="coerce").dropna()
                summary["ElapsedTime_sec"] = {
                    "start": float(et.min()) if not et.empty else 0.0,
                    "end": float(et.max()) if not et.empty else 0.0,
                }
        except Exception:
            pass

    # Persist summary JSON
    (out_dir / "hardware_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    # Also export per-element power/freq means as CSV
    if power_cols or freq_cols:
        rows = []
        elems = set(int(c.split("_E")[-1]) for c in freq_cols + power_cols)
        for e in sorted(elems):
            row = {"Element": e}
            fcol = f"Frequency_E{e}"
            pcol = f"PowerNetWa_E{e}"
            row["FrequencyMean"] = summary.get("FrequencyMean", {}).get(fcol)
            row["PowerMean"] = summary.get("PowerMean", {}).get(pcol)
            rows.append(row)
        pd.DataFrame(rows).to_csv(out_dir / "power_frequency_summary.csv", index=False)
