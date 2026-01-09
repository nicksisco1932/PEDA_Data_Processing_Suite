"""
OutputStatistics.py

Writes a simple stats JSON with a few key metrics for the segment.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import numpy as np


def OutputStatistics(ctx: Any) -> None:
    path_data = Path(ctx.get("pathData", "."))
    stats_path = path_data / "segment_stats.json"
    stats: Dict[str, Any] = {}

    # Pull basic geometry/config
    for key in [
        "PatientID",
        "SoftwareVersion",
        "Manufacturer",
        "PixelSize",
        "MinimumTreatmentRadiusMM",
        "MaximumTreatmentRadiusMM",
    ]:
        if key in ctx:
            stats[key] = ctx[key]

    # Load TMax/TDose if available
    tmax_file = path_data / "TMax.npy"
    tdose_file = path_data / "TDose.npy"
    if tmax_file.is_file():
        tmax = np.load(tmax_file)
        stats["TMax_max"] = float(np.nanmax(tmax))
        stats["TMax_mean"] = float(np.nanmean(tmax))
    if tdose_file.is_file():
        tdose = np.load(tdose_file)
        stats["TDose_max"] = float(np.nanmax(tdose))
        stats["TDose_mean"] = float(np.nanmean(tdose))

    stats_path.write_text(json.dumps(stats, indent=2), encoding="utf-8")
