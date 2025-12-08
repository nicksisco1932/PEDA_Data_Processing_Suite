"""
TreatmentControllerSummary.py

Minimal summary of TreatmentControllerData for a segment.
Writes JSON with counts and basic stats to pathData.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import numpy as np


def TreatmentControllerSummary(ctx: Any) -> None:
    path_data = Path(ctx.get("pathData", "."))
    tcd_path = path_data / "TreatmentControllerData.csv"
    if not tcd_path.is_file():
        return
    try:
        df = pd.read_csv(tcd_path)
    except Exception:
        return

    summary: Dict[str, Any] = {}
    summary["NumDynamics"] = int(len(df))
    if "ElapsedTime_sec" in df.columns:
        et = pd.to_numeric(df["ElapsedTime_sec"], errors="coerce").dropna()
        summary["ElapsedTime_sec"] = {
            "start": float(et.min()) if not et.empty else 0.0,
            "end": float(et.max()) if not et.empty else 0.0,
            "total": float(et.max() - et.min()) if len(et) else 0.0,
        }
    if "ControlAngle_deg" in df.columns:
        angles = pd.to_numeric(df["ControlAngle_deg"], errors="coerce").dropna()
        summary["ThermAngle_deg"] = {
            "min": float(angles.min()) if not angles.empty else None,
            "max": float(angles.max()) if not angles.empty else None,
        }
    if "TreatmentState" in df.columns:
        counts = df["TreatmentState"].value_counts().to_dict()
        summary["TreatmentStateCounts"] = counts

    (path_data / "treatment_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
