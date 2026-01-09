from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from src.path_utils import sanitize_path_str
from src.pipeline_config import CANONICAL_LAYOUT
from src.reporting.manifest import write_manifest
from src.selftest.fixtures import (
    apply_path_variant,
    build_yaml_config,
    collect_zip_files,
    get_fixture_paths,
    run_fixture_script,
    write_pdf_stub,
)
from src.selftest.permutations import NEG_CASES, PATH_VARIANTS, PERM_CASES


def run_self_test(*, keep_temp: bool = False) -> int:
    repo_root = Path(__file__).resolve().parents[2]
    logs_dir = repo_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    run_id = datetime.now().strftime("SELFTEST_%Y%m%d_%H%M%S")
    case_num = "TEST_CASE"
    tmp_root = repo_root / "tests" / "_tmp" / "selftest_perm" / run_id
    tmp_root.mkdir(parents=True, exist_ok=True)

    proc = run_fixture_script(repo_root)
    mri_fixture, tdc_fixture = get_fixture_paths(repo_root)

    layout_names = {
        "mr": CANONICAL_LAYOUT["mr_dir_name"],
        "tdc": CANONICAL_LAYOUT["tdc_dir_name"],
        "misc": CANONICAL_LAYOUT["misc_dir_name"],
    }
    expected_root_dirs = set(layout_names.values())

    results: List[Dict[str, Any]] = []
    failures: List[str] = []
    pass_count = 0
    fail_count = 0

    try:
        if proc.returncode != 0:
            failures.append(f"fixture generation failed: {proc.stdout}\n{proc.stderr}")
            fail_count += 1
        elif not mri_fixture.exists() or not tdc_fixture.exists():
            failures.append("missing fixture zips")
            fail_count += 1
        else:
            for idx, perm in enumerate(PERM_CASES, start=1):
                for variant in PATH_VARIANTS:
                    perm_label = f"perm_{idx:02d}_{variant}"
                    perm_dir = tmp_root / f"case_{idx:02d}" / variant
                    if perm_dir.exists():
                        shutil.rmtree(perm_dir, ignore_errors=True)
                    perm_dir.mkdir(parents=True, exist_ok=True)

                    inputs_dir = perm_dir / "inputs"
                    inputs_dir.mkdir(parents=True, exist_ok=True)
                    mri_path = inputs_dir / perm["mri"]
                    tdc_path = inputs_dir / perm["tdc"]
                    pdf_path = inputs_dir / perm["pdf"]
                    shutil.copy2(mri_fixture, mri_path)
                    shutil.copy2(tdc_fixture, tdc_path)
                    write_pdf_stub(pdf_path)

                    mri_bak = Path(str(mri_path) + ".bak")
                    tdc_bak = Path(str(tdc_path) + ".bak")
                    pdf_bak = Path(str(pdf_path) + ".bak")
                    shutil.copy2(mri_path, mri_bak)
                    shutil.copy2(tdc_path, tdc_bak)
                    shutil.copy2(pdf_path, pdf_bak)

                    out_root = perm_dir / "out"
                    out_root.mkdir(parents=True, exist_ok=True)
                    scratch_dir = perm_dir / "scratch"
                    perm_run_id = f"{run_id}_{idx:02d}_{variant}"

                    raw_mri = apply_path_variant(mri_path, variant)
                    raw_tdc = apply_path_variant(tdc_path, variant)
                    raw_pdf = apply_path_variant(pdf_path, variant)

                    if perm["method"] == "cli":
                        args = [
                            sys.executable,
                            str(repo_root / "src" / "controller.py"),
                            "--root",
                            str(out_root),
                            "--case",
                            case_num,
                            "--mri-input",
                            raw_mri,
                            "--tdc-input",
                            raw_tdc,
                            "--pdf-input",
                            raw_pdf,
                            "--scratch",
                            str(scratch_dir),
                            "--test-mode",
                            "--no-legacy-filename-rules",
                            "--run-id",
                            perm_run_id,
                            "--log-dir",
                            str(logs_dir),
                        ]
                    else:
                        yaml_path = perm_dir / "config.yaml"
                        build_yaml_config(
                            yaml_path=yaml_path,
                            case_num=case_num,
                            out_root=out_root,
                            scratch_dir=scratch_dir,
                            raw_mri=raw_mri,
                            raw_tdc=raw_tdc,
                            raw_pdf=raw_pdf,
                            perm_run_id=perm_run_id,
                            logs_dir=logs_dir,
                        )
                        args = [
                            sys.executable,
                            str(repo_root / "src" / "controller.py"),
                            "--config",
                            str(yaml_path),
                        ]

                    proc = subprocess.run(
                        args, capture_output=True, text=True, cwd=str(repo_root)
                    )
                    combined = f"{proc.stdout}\n{proc.stderr}"
                    perm_manifest = (
                        logs_dir / f"{case_num}__{perm_run_id}__manifest.json"
                    )
                    perm_result: Dict[str, Any] = {
                        "index": idx,
                        "case_type": "positive",
                        "path_variant": variant,
                        "method": perm["method"],
                        "status": "PASS" if proc.returncode == 0 else "FAIL",
                        "permuted_basenames": {
                            "mri": perm["mri"],
                            "tdc": perm["tdc"],
                            "pdf": perm["pdf"],
                        },
                        "backup_paths": {
                            "mri": str(mri_bak),
                            "tdc": str(tdc_bak),
                            "pdf": str(pdf_bak),
                        },
                        "raw_inputs": {
                            "mri": raw_mri,
                            "tdc": raw_tdc,
                            "pdf": raw_pdf,
                        },
                        "sanitized_inputs": {
                            "mri": sanitize_path_str(raw_mri),
                            "tdc": sanitize_path_str(raw_tdc),
                            "pdf": sanitize_path_str(raw_pdf),
                        },
                        "output_dirs": {
                            "case_dir": str(out_root / case_num),
                            "mr_dir": str(out_root / case_num / layout_names["mr"]),
                            "tdc_dir": str(out_root / case_num / layout_names["tdc"]),
                            "misc_dir": str(out_root / case_num / layout_names["misc"]),
                        },
                        "invocation": {"args": args},
                        "return_code": proc.returncode,
                        "manifest_path": str(perm_manifest),
                    }

                    if proc.returncode != 0:
                        perm_result["error"] = combined.strip()
                        failures.append(f"{perm_label}: nonzero exit {proc.returncode}")
                        results.append(perm_result)
                        fail_count += 1
                        continue

                    if "legacy filename rules: true" in combined.lower():
                        perm_result["status"] = "FAIL"
                        perm_result["error"] = "Legacy filename rules enabled in output"
                        failures.append(f"{perm_label}: legacy filename rules enabled")
                        results.append(perm_result)
                        fail_count += 1
                        continue

                    case_dir = out_root / case_num
                    if not case_dir.exists():
                        perm_result["status"] = "FAIL"
                        perm_result["error"] = f"Missing case_dir {case_dir}"
                    else:
                        children = list(case_dir.iterdir())
                        child_dirs = {p.name for p in children if p.is_dir()}
                        child_files = [p.name for p in children if p.is_file()]
                        if child_files:
                            perm_result["status"] = "FAIL"
                            perm_result["error"] = (
                                f"Unexpected files at case root: {child_files}"
                            )
                        elif child_dirs != expected_root_dirs:
                            perm_result["status"] = "FAIL"
                            perm_result["error"] = (
                                f"Expected dirs {sorted(expected_root_dirs)}, got {sorted(child_dirs)}"
                            )

                    if perm_result.get("status") == "PASS":
                        zip_files = collect_zip_files(case_dir)
                        if zip_files:
                            perm_result["status"] = "FAIL"
                            perm_result["error"] = (
                                f"Zip artifacts found: {[str(p) for p in zip_files]}"
                            )

                    if perm_result.get("status") == "PASS":
                        if not perm_manifest.exists():
                            perm_result["status"] = "FAIL"
                            perm_result["error"] = f"Missing manifest {perm_manifest}"
                        else:
                            try:
                                manifest_data = json.loads(
                                    perm_manifest.read_text(encoding="utf-8")
                                )
                            except Exception as exc:
                                perm_result["status"] = "FAIL"
                                perm_result["error"] = f"Manifest unreadable: {exc}"
                            else:
                                config = manifest_data.get("config", {})
                                expected_inputs = perm_result["sanitized_inputs"]
                                for key, label in (
                                    ("mri_input", "mri"),
                                    ("tdc_input", "tdc"),
                                    ("pdf_input", "pdf"),
                                ):
                                    if config.get(key) != expected_inputs[label]:
                                        perm_result["status"] = "FAIL"
                                        perm_result["error"] = (
                                            f"Manifest {key} mismatch: {config.get(key)}"
                                        )
                                        break

                    if perm_result.get("status") == "PASS":
                        for label, value in perm_result["sanitized_inputs"].items():
                            if not Path(value).is_absolute():
                                perm_result["status"] = "FAIL"
                                perm_result["error"] = (
                                    f"Non-absolute {label} path: {value}"
                                )
                                break

                    if perm_result.get("status") != "PASS":
                        failures.append(f"{perm_label}: {perm_result.get('error')}")
                        fail_count += 1
                    else:
                        pass_count += 1

                    results.append(perm_result)

            for idx, neg in enumerate(NEG_CASES, start=1):
                neg_label = f"neg_{idx:02d}_{neg['name']}"
                neg_dir = tmp_root / "negative" / neg_label
                if neg_dir.exists():
                    shutil.rmtree(neg_dir, ignore_errors=True)
                neg_dir.mkdir(parents=True, exist_ok=True)

                inputs_dir = neg_dir / "inputs"
                inputs_dir.mkdir(parents=True, exist_ok=True)
                out_root = neg_dir / "out"
                out_root.mkdir(parents=True, exist_ok=True)
                scratch_dir = neg_dir / "scratch"

                tdc_path = inputs_dir / "tdc_ok.zip"
                pdf_path = inputs_dir / "doc_ok.pdf"
                shutil.copy2(tdc_fixture, tdc_path)
                write_pdf_stub(pdf_path)

                if neg["path_type"] == "missing":
                    mri_path = inputs_dir / neg["mri_name"]
                elif neg["path_type"] == "dir":
                    mri_path = inputs_dir
                else:
                    mri_path = inputs_dir / neg["mri_name"]
                    shutil.copy2(mri_fixture, mri_path)

                if neg["path_type"] == "empty":
                    raw_mri = ""
                else:
                    raw_mri = str(mri_path)

                perm_run_id = f"{run_id}_neg{idx:02d}"
                args = [
                    sys.executable,
                    str(repo_root / "src" / "controller.py"),
                    "--root",
                    str(out_root),
                    "--case",
                    case_num,
                    "--mri-input",
                    raw_mri,
                    "--tdc-input",
                    str(tdc_path),
                    "--pdf-input",
                    str(pdf_path),
                    "--scratch",
                    str(scratch_dir),
                    "--test-mode",
                    "--no-legacy-filename-rules",
                    "--run-id",
                    perm_run_id,
                    "--log-dir",
                    str(logs_dir),
                ]
                proc = subprocess.run(
                    args, capture_output=True, text=True, cwd=str(repo_root)
                )
                combined = f"{proc.stdout}\n{proc.stderr}".lower()
                perm_result = {
                    "index": idx,
                    "case_type": "negative",
                    "name": neg["name"],
                    "status": "PASS" if proc.returncode != 0 else "FAIL",
                    "raw_inputs": {
                        "mri": raw_mri,
                        "tdc": str(tdc_path),
                        "pdf": str(pdf_path),
                    },
                    "invocation": {"args": args},
                    "return_code": proc.returncode,
                }
                if proc.returncode == 0:
                    perm_result["error"] = "Expected failure but got success"
                    failures.append(f"{neg_label}: unexpected success")
                    results.append(perm_result)
                    fail_count += 1
                    continue

                if not any(token in combined for token in neg["expect"]):
                    perm_result["status"] = "FAIL"
                    perm_result["error"] = (
                        f"Expected error containing {neg['expect']}, got output: {combined.strip()}"
                    )
                    failures.append(f"{neg_label}: error message mismatch")
                    fail_count += 1
                else:
                    pass_count += 1
                results.append(perm_result)
    finally:
        if not keep_temp:
            shutil.rmtree(tmp_root, ignore_errors=True)

    run_log = logs_dir / f"RUN_{run_id}.log"
    run_manifest = logs_dir / f"RUN_{run_id}_manifest.json"
    summary = {
        "self_test": True,
        "run_id": run_id,
        "case_num": case_num,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "permutations": results,
        "summary": {"pass": pass_count, "fail": fail_count},
        "artifacts": {
            "run_log": str(run_log),
            "run_manifest": str(run_manifest),
        },
    }
    try:
        write_manifest(run_manifest, summary)
    except Exception as exc:
        failures.append(f"failed to write self-test manifest: {exc}")

    try:
        with run_log.open("a", encoding="utf-8") as fh:
            fh.write("SELF-TEST permutations:\n")
            for perm_result in results:
                backups = perm_result.get("backup_paths") or {}
                fh.write(
                    f" - {perm_result.get('case_type')} "
                    f"{perm_result.get('status')} "
                    f"idx={perm_result.get('index')} "
                    f"variant={perm_result.get('path_variant')} "
                    f"method={perm_result.get('method')} "
                    f"mri={perm_result.get('permuted_basenames', {}).get('mri')} "
                    f"tdc={perm_result.get('permuted_basenames', {}).get('tdc')} "
                    f"pdf={perm_result.get('permuted_basenames', {}).get('pdf')} "
                    f"mri_bak={backups.get('mri')} "
                    f"tdc_bak={backups.get('tdc')} "
                    f"pdf_bak={backups.get('pdf')}\n"
                )
    except Exception as exc:
        failures.append(f"failed to append self-test log: {exc}")

    summary_line = f"[SELF-TEST] PASS: {pass_count} / FAIL: {fail_count}\n"
    if failures:
        sys.stderr.write(summary_line)
        for f in failures:
            sys.stderr.write(f" - {f}\n")
        return 1

    sys.stdout.write(summary_line)
    return 0
