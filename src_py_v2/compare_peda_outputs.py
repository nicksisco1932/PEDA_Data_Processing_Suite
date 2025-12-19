"""
compare_peda_outputs.py

Diagnostic parity checker between legacy MATLAB PEDA outputs and Python PEDA outputs.

Usage:
    python compare_peda_outputs.py --mat-root "D:\\093_01-098 PEDAv9.1.3-Data\\093_01-098 PEDAv9.1.3-Data" \
                                   --py-root  "D:\\093_01-098\\output\\093_01-098 TDC Sessions\\<session_timestamp>\\PEDA" \
                                   [--tol 1e-2] [--print-tree]

This script is read-only; it does not modify any outputs.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from scipy.io import loadmat


def walk_tree(root: Path) -> List[Tuple[str, str, str]]:
    """
    Return a list of (relpath, name, ext) for all files under root.
    """
    entries = []
    for p in sorted(root.rglob("*")):
        if p.is_file():
            rel = str(p.relative_to(root))
            entries.append((rel, p.name, p.suffix))
    return entries


def print_tree_summary(root: Path) -> None:
    print(f"[TREE] {root}")
    for rel, name, ext in walk_tree(root):
        print(f"  {rel}")


def _load_mat_var(path: Path, varname: str):
    data = loadmat(path)
    if varname not in data:
        return None
    arr = np.array(data[varname])
    return np.squeeze(arr)


def _load_npy(path: Path):
    arr = np.load(path, allow_pickle=False)
    return np.squeeze(arr)


def compare_array(label: str, mat_path: Path, mat_var: str, npy_path: Path, tol: float) -> Dict[str, object]:
    result: Dict[str, object] = {
        "label": label,
        "legacy_path": str(mat_path),
        "python_path": str(npy_path),
        "status": "ok",
    }
    if not mat_path.is_file():
        result["status"] = "missing_legacy"
        return result
    if not npy_path.is_file():
        result["status"] = "missing_python"
        return result
    try:
        legacy = _load_mat_var(mat_path, mat_var)
    except Exception as exc:
        result["status"] = f"legacy_load_error: {exc!r}"
        return result
    try:
        py = _load_npy(npy_path)
    except Exception as exc:
        result["status"] = f"python_load_error: {exc!r}"
        return result

    if legacy is None:
        result["status"] = "legacy_var_missing"
        return result

    result["shape_legacy"] = legacy.shape
    result["shape_python"] = py.shape

    if legacy.shape != py.shape:
        result["status"] = "shape_mismatch"
        # attempt to compare on overlapping size if possible
        min_shape = tuple(min(a, b) for a, b in zip(legacy.shape, py.shape))
        try:
            slices = tuple(slice(0, m) for m in min_shape)
            legacy_cmp = legacy[slices]
            py_cmp = py[slices]
        except Exception:
            return result
    else:
        legacy_cmp = legacy
        py_cmp = py

    try:
        diff = np.abs(np.array(legacy_cmp, dtype=float) - np.array(py_cmp, dtype=float))
        result["max_abs_diff"] = float(np.nanmax(diff))
        result["mean_abs_diff"] = float(np.nanmean(diff))
        within = np.sum(diff <= tol)
        total = diff.size
        frac = within / total if total else 0.0
        result[f"within_tol_{tol}"] = frac * 100.0
        if result["status"] == "ok" and frac < 0.99 * 100:
            result["status"] = "differs"
    except Exception as exc:
        result["status"] = f"diff_error: {exc!r}"

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare legacy MATLAB PEDA outputs to Python PEDA outputs.")
    parser.add_argument("--mat-root", required=True, help="Legacy PEDA root (MATLAB outputs).")
    parser.add_argument("--py-root", required=True, help="Python PEDA root (PEDA folder).")
    parser.add_argument("--tol", type=float, default=1e-2, help="Tolerance for array comparisons.")
    parser.add_argument("--print-tree", action="store_true", help="Print tree summaries for both roots.")
    args = parser.parse_args()

    mat_root = Path(args.mat_root)
    py_root = Path(args.py_root)

    if args.print_tree:
        print_tree_summary(mat_root)
        print_tree_summary(py_root)

    # Curated list of key arrays to compare (adjust var names if legacy differs)
    comparisons = [
        ("TMap", "SEGMENT 1/TMap.mat", "TMap", "TMap.npy"),
        ("TMax", "SEGMENT 1/TMax.mat", "TMax", "TMax.npy"),
        ("TDose", "SEGMENT 1/TDose.mat", "TDose", "TDose.npy"),
        ("TDoseMasked", "SEGMENT 1/TDoseMasked.mat", "TDoseMasked", "TDoseMasked.npy"),
        ("MaxTemperatureTime", "SEGMENT 1/MaxTemperatureTime.mat", "MaxTemperatureTime", "MaxTemperatureTime.npy"),
        ("Mask", "SEGMENT 1/Mask.mat", "Mask", "Mask.npy"),
        ("Isotherms", "SEGMENT 1/Isotherms.mat", "Isotherms", "Isotherms.npy"),
        ("TUV", "SEGMENT 1/TUV.mat", "TUV", "TUV.npy"),
    ]

    results = []
    ok = diff = missing = 0
    for label, legacy_rel, varname, py_rel in comparisons:
        mat_path = mat_root / legacy_rel
        npy_path = py_root / py_rel
        res = compare_array(label, mat_path, varname, npy_path, args.tol)
        results.append(res)
        status = res.get("status")
        if status == "ok":
            ok += 1
        elif status in ("missing_legacy", "missing_python", "legacy_var_missing"):
            missing += 1
        else:
            diff += 1

    # Print structured summary
    for res in results:
        print(f"\n{res['label']}:")
        print(f"  legacy: {res.get('legacy_path')}")
        print(f"  python: {res.get('python_path')}")
        print(f"  status: {res.get('status')}")
        if "shape_legacy" in res:
            print(f"  shape_legacy = {res['shape_legacy']}")
            print(f"  shape_python = {res['shape_python']}")
        if "max_abs_diff" in res:
            print(f"  max_abs_diff = {res['max_abs_diff']}")
        if "mean_abs_diff" in res:
            print(f"  mean_abs_diff = {res['mean_abs_diff']}")
        key_tol = f"within_tol_{args.tol}"
        if key_tol in res:
            print(f"  within_tol({args.tol}) = {res[key_tol]:.2f}%")

    print("\n=== Summary ===")
    print(f"Within tol: {ok}")
    print(f"Different / errors: {diff}")
    print(f"Missing: {missing}")


if __name__ == "__main__":
    sys.exit(main())
