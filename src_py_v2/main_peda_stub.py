# src_py_v2/main_peda_stub.py

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict

from RetrieveSxParameters import RetrieveSxParameters

# Reuse the I/O + path scaffolding from task_master
from task_master import (
    find_segments,
    pedapaths,
    ensure_raw_present_temp,
    PedaPaths,
)

# Stubbed functional endpoints (they currently raise NotImplementedError)
from CreateTMaxTDose_wrapper import CreateTMaxTDose_wrapper
from AnalyzeHardwareLogs import AnalyzeHardwareLogs
from GenerateMovies import GenerateMovies
from AdditionalImageMasking import AdditionalImageMasking
from CreateIsotherms import CreateIsotherms
from TreatmentControllerSummary import TreatmentControllerSummary
from OutputStatistics import OutputStatistics
from PlotTmax import PlotTmax


def build_minimal_Sx(
    P: PedaPaths,
    case_dir: Path,
    seg_root: Path,
    seg_idx: int,
    patient_id: str,
    staged_root: Path,
) -> Dict[str, Any]:
    """
    Minimal Python analogue of the Sx struct assembled in MAIN_PEDA.m.

    This sets up the fields MAIN_PEDA layered around RetrieveSxParameters.
    RetrieveSxParameters itself will further enrich this dict.
    """
    case_dir = case_dir.resolve()
    seg_root = seg_root.resolve()

    # Legacy PEDA v9.1.3 hardware log convention
    path_hardware_info = case_dir / "PEDAv9.1.3" / f"Segment {seg_idx}"

    Sx: Dict[str, Any] = {
        "PatientID": patient_id,
        # Original segment root (where the TDC session actually lives)
        "origSessionRoot": seg_root,
        # Staged session root under work\segXX_...\session
        "sessionRoot": staged_root,
        # IMPORTANT: this must be the real segment folder for RetrieveSxParameters
        "pathSessionFiles": seg_root,
        # Where this pipeline expects PEDA outputs to go
        "pathData": P.pathData,
        "pathPEDA": P.output_root,
        "segmentOutputDir": P.seg_out,
        # Legacy hardware-info location
        "pathHardwareInfo": path_hardware_info,
        # Raw root (for thermal code / future steps)
        "rawRoot": seg_root.parent / "Raw",
        # ImageNumber will eventually be filled from controller data
        "ImageNumber": None,
    }

    return Sx


def run_segment_stub(
    case_dir: Path,
    seg_root: Path,
    seg_idx: int,
    patient_id: str | None,
) -> None:
    """
    Emulate the per-segment flow of MAIN_PEDA.m using stubs.

    This does NOT perform real computation; it:
      - builds PedaPaths
      - stages Raw/local.db
      - constructs a minimal Sx dict
      - enriches Sx via RetrieveSxParameters
      - calls each stub function in order, catching NotImplementedError
    """
    print("============================================")
    print(f"ANALYZING SEGMENT {seg_idx} at {seg_root}")

    # Build deterministic paths (output/work) and stage Raw/local.db
    P = pedapaths(case_dir, patient_id, seg_root, seg_idx)
    staged_session_root = ensure_raw_present_temp(P)

    # Derive patientID if pedapaths filled it
    if patient_id is None:
        patient_id = P.patientID

    # Build minimal Sx context
    Sx = build_minimal_Sx(
        P=P,
        case_dir=case_dir,
        seg_root=seg_root,
        seg_idx=seg_idx,
        patient_id=patient_id,
        staged_root=staged_session_root,
    )

    print(f"[SEGMENT] patientID={patient_id}")
    print(f"[SEGMENT] sessionRoot={Sx['sessionRoot']}")
    print(f"[SEGMENT] pathPEDA={Sx['pathPEDA']}")
    print(f"[SEGMENT] segmentOutputDir={Sx['segmentOutputDir']}")

    # Enrich Sx with legacy PEDA parameters from the session logs
    try:
        Sx = RetrieveSxParameters(Sx)
    except Exception as exc:
        print(f"\t[RETRIEVE_SX WARNING] RetrieveSxParameters failed: {exc!r}")
    else:
        # Optional quick sanity summary (keeps console output compact)
        print("  [SX SUMMARY]")
        for key in (
            "Manufacturer",
            "SoftwareVersion",
            "SliceThickness",
            "FOV",
            "ImageResolution",
            "NumberOfRows",
            "NumberOfCols",
            "NumberSlices",
            "PixelSize",
            "MinimumTreatmentRadiusMM",
            "MaximumTreatmentRadiusMM",
        ):
            if key in Sx:
                print(f"    {key:26s} = {Sx[key]}")

    # ---- STEP 2: Thermal (CreateTMaxTDose_wrapper) ----
    print("\n\tSTEP 2: Create TMax and TDose (stub)")
    try:
        # This will currently raise NotImplementedError
        TMap, Mag, MaxT, Mask, TUV, TUVMag = CreateTMaxTDose_wrapper(Sx)
    except NotImplementedError as exc:
        print(f"\t[STUB HIT] CreateTMaxTDose_wrapper -> {exc}")
        TMap = Mag = MaxT = Mask = TUV = TUVMag = None

    # ---- STEP 3: Analyze hardware logs ----
    print("\n\tSTEP 3: Analyze hardware logs (stub)")
    try:
        AnalyzeHardwareLogs(Sx)
    except NotImplementedError as exc:
        print(f"\t[STUB HIT] AnalyzeHardwareLogs -> {exc}")

    # ---- STEP 4: Create movies ----
    print("\n\tSTEP 4: Create movies (stub)")
    try:
        GenerateMovies(Sx, TMap, Mag, MaxT, TUV, TUVMag)
    except NotImplementedError as exc:
        print(f"\t[STUB HIT] GenerateMovies -> {exc}")

    # ---- STEP 5: Additional image masking ----
    print("\n\tSTEP 5: Additional image masking (stub)")
    try:
        AdditionalImageMasking(Sx, TMap)
    except NotImplementedError as exc:
        print(f"\t[STUB HIT] AdditionalImageMasking -> {exc}")

    # ---- STEP 6: Create isotherms ----
    print("\n\tSTEP 6: Create isotherms (stub)")
    try:
        CreateIsotherms(Sx)
    except NotImplementedError as exc:
        print(f"\t[STUB HIT] CreateIsotherms -> {exc}")

    # ---- STEP 7: Treatment controller statistics ----
    print("\n\tSTEP 7: Treatment controller summary (stub)")
    try:
        TreatmentControllerSummary(Sx)
    except NotImplementedError as exc:
        print(f"\t[STUB HIT] TreatmentControllerSummary -> {exc}")

    # ---- STEP 8: Segment statistics ----
    print("\n\tSTEP 8: Segment statistics (stub)")
    try:
        OutputStatistics(Sx)
    except NotImplementedError as exc:
        print(f"\t[STUB HIT] OutputStatistics -> {exc}")

    # ---- STEP 9: Plot data ----
    print("\n\tSTEP 9: Plot Tmax (stub)")
    try:
        PlotTmax(Sx)
    except NotImplementedError as exc:
        print(f"\t[STUB HIT] PlotTmax -> {exc}")

    print("\n[SEGMENT DONE] (all stubs reached)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Stubbed PEDA pipeline driver.\n"
            "Discovers segments under a TDC case directory, stages Raw/local.db, "
            "constructs a minimal Sx context, and invokes stubbed processing steps."
        )
    )
    parser.add_argument(
        "--directory",
        "-d",
        required=True,
        help="TDC case directory (e.g., D:\\093_01-098\\TDC_093-01_098\\_YYYY-MM-DD--HH-MM-SS EPOCH)",
    )
    parser.add_argument(
        "--patient-id",
        "-p",
        default=None,
        help="Optional patientID (e.g., 093_01-098). If omitted, derived from directory.",
    )
    parser.add_argument(
        "--segment",
        "-s",
        type=int,
        default=0,
        help="Specific 1-based segment index to run (0 = run all segments).",
    )

    args = parser.parse_args()

    case_dir = Path(args.directory).resolve()
    if not case_dir.is_dir():
        raise FileNotFoundError(f"Case directory not found: {case_dir}")

    segs = find_segments(case_dir)
    if not segs:
        print(f"[WARN] No segments found under {case_dir}")
        return

    print(f"[INFO] Case dir: {case_dir}")
    print(f"[INFO] Found {len(segs)} segment(s):")
    for idx, s in enumerate(segs, start=1):
        print(f"    {idx}: {s}")

    if args.segment > 0:
        # Run a single requested segment
        if args.segment > len(segs):
            raise IndexError(
                f"Requested segment {args.segment}, but only {len(segs)} segment(s) found."
            )
        run_segment_stub(case_dir, segs[args.segment - 1], args.segment, args.patient_id)
    else:
        # Run all segments
        for idx, seg_root in enumerate(segs, start=1):
            run_segment_stub(case_dir, seg_root, idx, args.patient_id)


if __name__ == "__main__":
    main()
