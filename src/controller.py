# PURPOSE: CLI entrypoint that orchestrates pipeline runs and writes logs/manifests.
# INPUTS: CLI args and resolved config paths (MRI/TDC/PDF, root, scratch, flags).
# OUTPUTS: Case output tree, run log, and manifest JSON.
# NOTES: Handles run status and error reporting for downstream steps.
from __future__ import annotations

import argparse
import getpass
import platform
import socket
import subprocess
import sys
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import MRI_proc
import TDC_proc
from src.logutil import (
    init_logger,
    StatusManager,
    StepTimer,
    ValidationError,
    ProcessingError,
    UnexpectedError,
)
from src.manifest import file_metadata, write_manifest
from src.pipeline_config import add_bool_arg, resolve_config
from src.path_utils import sanitize_path_str


def _validate_zip(path: Path, label: str, raw_value: Optional[str] = None) -> None:
    if not path.exists() or not path.is_file():
        extra = f" raw={raw_value!r}" if raw_value else ""
        raise ValidationError(f"{label} not found: {path}.{extra}")
    if path.suffix.lower() != ".zip":
        extra = f" raw={raw_value!r}" if raw_value else ""
        raise ValidationError(f"{label} must be a .zip: {path}.{extra}")


def _assert_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise ProcessingError(f"Missing expected {label}: {path}")


def _build_plan(
    *,
    case_dir: Path,
    misc_dir: Path,
    scratch: Path,
    mr_dir: Path,
    tdc_dir: Path,
    log_dir: Path,
    manifest_path: Path,
    run_id: str,
    case: str,
    mri_input: Optional[Path],
    tdc_input: Optional[Path],
    pdf_input: Optional[Path],
    skip_mri: bool,
    skip_tdc: bool,
    test_mode: bool,
    allow_workspace_zips: bool,
    clean_scratch: bool,
    legacy_names: bool,
) -> List[str]:
    plan: List[str] = []
    plan.append(f"Create case dir: {case_dir}")
    plan.append(f"Create output dir: {misc_dir}")
    plan.append(f"Create output dir: {mr_dir}")
    plan.append(f"Create output dir: {tdc_dir}")
    plan.append(f"Create output dir: {tdc_dir / 'applog' / 'Logs'}")
    plan.append(
        f"TDC Raw output dir created if TDC produces it: {tdc_dir / '<session>' / 'Raw'}"
    )
    plan.append(f"Create scratch dir: {scratch}")
    if not skip_mri and mri_input:
        plan.append(
            f"Copy MRI zip to scratch backup: {mri_input} -> {scratch / (mri_input.name + '.bak')}"
        )
        final_dir = mr_dir / mri_input.stem
        plan.append(f"Extract MRI zip into final dir: {final_dir}")
    if not skip_tdc and tdc_input:
        plan.append(
            f"Copy TDC zip to scratch backup: {tdc_input} -> {scratch / (tdc_input.name + '.bak')}"
        )
        plan.append("Extract TDC zip into temp under scratch")
        plan.append(f"Copy Logs/ to {misc_dir / 'Logs'} if present")
        plan.append(
            "Stage TDC session in scratch/TDC_staged and copy top-level dirs as directories"
        )
        plan.append("Expand Raw.zip or timestamp zips into directories if present")
        if allow_workspace_zips:
            plan.append("Allow zip archives under TDC workspace (override enabled)")
        else:
            plan.append("Disallow zip archives under TDC workspace")
        if test_mode:
            plan.append("Test-mode: keep staging lightweight (no zipping)")
        plan.append(f"Copy staged TDC session to: {tdc_dir / '<session_name>'}")
    if pdf_input:
        pdf_name = f"{case}_TreatmentReport.pdf" if legacy_names else pdf_input.name
        plan.append(f"Copy treatment report to: {misc_dir / pdf_name}")
    plan.append(f"Write log file to: {log_dir / (case + '__' + run_id + '.log')}")
    plan.append(f"Write manifest to: {manifest_path}")
    if clean_scratch:
        plan.append(f"Delete scratch dir: {scratch}")
    return plan


def _run_self_test() -> int:
    repo_root = ROOT
    logs_dir = repo_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    run_id = "TEST_MODE"
    case_num = "TEST_CASE"
    tmp_root = repo_root / "tests" / "_tmp" / run_id
    tmp_root.mkdir(parents=True, exist_ok=True)

    fixture_script = repo_root / "tools" / "generate_fixtures.py"
    proc = subprocess.run(
        [sys.executable, str(fixture_script)],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
    )
    if proc.returncode != 0:
        sys.stderr.write(
            f"[SELF-TEST] Fixture generation failed:\n{proc.stdout}\n{proc.stderr}\n"
        )
        return 1

    mri_fixture = repo_root / "tests" / "fixtures" / "mri_dummy.zip"
    tdc_fixture = repo_root / "tests" / "fixtures" / "tdc_dummy.zip"
    if not mri_fixture.exists() or not tdc_fixture.exists():
        sys.stderr.write("[SELF-TEST] Missing fixture zips.\n")
        return 1

    permutations = [
        {"mri": "alpha.zip", "tdc": "bravo.zip", "pdf": "charlie.pdf", "method": "cli", "quote": False},
        {"mri": "foo bar.zip", "tdc": "echo.zip", "pdf": "delta file.pdf", "method": "cli", "quote": True},
        {"mri": "golf.zip", "tdc": "hotel.zip", "pdf": "india.pdf", "method": "cli", "quote": False},
        {"mri": "juliet.zip", "tdc": "kilo.zip", "pdf": "lima.pdf", "method": "yaml", "quote": False},
        {"mri": "november.zip", "tdc": "oscar.zip", "pdf": "papa file.pdf", "method": "yaml", "quote": True},
        {"mri": "sierra.zip", "tdc": "tango.zip", "pdf": "uniform.pdf", "method": "yaml", "quote": False},
    ]

    def validate_basename(name: str) -> None:
        lowered = name.lower()
        if "mr" in lowered or "tdc" in lowered or case_num.lower() in lowered:
            raise ValueError(f"Invalid basename (contains forbidden tokens): {name}")

    def yaml_path_value(path: Path, wrap_quotes: bool) -> str:
        raw = str(path)
        if wrap_quotes:
            return f'"{raw}"'
        return raw

    results: List[Dict[str, Any]] = []
    failures: List[str] = []

    for idx, perm in enumerate(permutations, start=1):
        try:
            validate_basename(perm["mri"])
            validate_basename(perm["tdc"])
            validate_basename(perm["pdf"])
        except ValueError as exc:
            failures.append(f"perm_{idx:02d}: {exc}")
            results.append(
                {
                    "index": idx,
                    "status": "FAIL",
                    "error": str(exc),
                    "permuted_basenames": perm,
                    "backup_paths": {"mri": None, "tdc": None, "pdf": None},
                }
            )
            continue

        perm_dir = tmp_root / f"perm_{idx:02d}"
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
        pdf_path.write_bytes(b"%PDF-1.4\n%EOF\n")

        mri_bak = Path(str(mri_path) + ".bak")
        tdc_bak = Path(str(tdc_path) + ".bak")
        pdf_bak = Path(str(pdf_path) + ".bak")
        shutil.copy2(mri_path, mri_bak)
        shutil.copy2(tdc_path, tdc_bak)
        shutil.copy2(pdf_path, pdf_bak)

        out_root = perm_dir / "out"
        out_root.mkdir(parents=True, exist_ok=True)

        if perm["method"] == "cli":
            raw_mri = f'"{mri_path}"' if perm["quote"] else str(mri_path)
            raw_tdc = f'"{tdc_path}"' if perm["quote"] else str(tdc_path)
            raw_pdf = f'"{pdf_path}"' if perm["quote"] else str(pdf_path)
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
                "--test-mode",
                "--run-id",
                run_id,
                "--log-dir",
                str(logs_dir),
            ]
        else:
            yaml_path = perm_dir / "config.yaml"
            yaml_mri = yaml_path_value(mri_path, perm["quote"])
            yaml_tdc = yaml_path_value(tdc_path, perm["quote"])
            yaml_pdf = yaml_path_value(pdf_path, perm["quote"])
            yaml_text = (
                "version: 1\n"
                "case:\n"
                f"  id: \"{case_num}\"\n"
                f"  root: '{out_root}'\n"
                "  layout:\n"
                "    mr_dir_name: \"MR DICOM\"\n"
                "    tdc_dir_name: \"TDC Sessions\"\n"
                "    misc_dir_name: \"Misc\"\n"
                "inputs:\n"
                "  mode: \"explicit\"\n"
                "  explicit:\n"
                f"    mri_zip: '{yaml_mri}'\n"
                f"    tdc_zip: '{yaml_tdc}'\n"
                f"    pdf_input: '{yaml_pdf}'\n"
                "run:\n"
                "  flags:\n"
                "    test_mode: true\n"
                "    allow_workspace_zips: false\n"
                "    legacy_filename_rules: false\n"
                "logging:\n"
                f"  dir: '{logs_dir}'\n"
                f"  manifest_dir: '{logs_dir}'\n"
                "metadata:\n"
                f"  run_id: \"{run_id}\"\n"
            )
            yaml_path.write_text(yaml_text, encoding="utf-8")
            raw_mri = yaml_mri
            raw_tdc = yaml_tdc
            raw_pdf = yaml_pdf
            args = [
                sys.executable,
                str(repo_root / "src" / "controller.py"),
                "--config",
                str(yaml_path),
            ]

        proc = subprocess.run(args, capture_output=True, text=True, cwd=str(repo_root))
        perm_result: Dict[str, Any] = {
            "index": idx,
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
            "raw_inputs": {"mri": raw_mri, "tdc": raw_tdc, "pdf": raw_pdf},
            "sanitized_inputs": {
                "mri": sanitize_path_str(raw_mri),
                "tdc": sanitize_path_str(raw_tdc),
                "pdf": sanitize_path_str(raw_pdf),
            },
            "output_dirs": {
                "case_dir": str(out_root / case_num),
                "mr_dir": str(out_root / case_num / "MR DICOM"),
                "tdc_dir": str(out_root / case_num / "TDC Sessions"),
                "misc_dir": str(out_root / case_num / "Misc"),
            },
            "invocation": {"method": perm["method"], "args": args},
            "return_code": proc.returncode,
        }

        if proc.returncode != 0:
            perm_result["error"] = f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
            failures.append(f"perm_{idx:02d}: nonzero exit {proc.returncode}")
            results.append(perm_result)
            continue

        case_dir = out_root / case_num
        expected_dirs = {"MR DICOM", "TDC Sessions", "Misc"}
        if not case_dir.exists():
            failures.append(f"perm_{idx:02d}: missing case_dir {case_dir}")
            perm_result["status"] = "FAIL"
            perm_result["error"] = f"Missing case_dir {case_dir}"
        else:
            children = list(case_dir.iterdir())
            child_dirs = {p.name for p in children if p.is_dir()}
            child_files = [p.name for p in children if p.is_file()]
            if child_files:
                failures.append(f"perm_{idx:02d}: unexpected files {child_files}")
                perm_result["status"] = "FAIL"
                perm_result["error"] = f"Unexpected files at case root: {child_files}"
            elif child_dirs != expected_dirs:
                failures.append(
                    f"perm_{idx:02d}: case root dirs mismatch {child_dirs}"
                )
                perm_result["status"] = "FAIL"
                perm_result["error"] = (
                    f"Expected dirs {sorted(expected_dirs)}, got {sorted(child_dirs)}"
                )

        results.append(perm_result)

    run_log = logs_dir / f"RUN_{run_id}.log"
    run_manifest = logs_dir / f"RUN_{run_id}_manifest.json"
    case_log = logs_dir / f"{case_num}__{run_id}.log"
    summary = {
        "self_test": True,
        "run_id": run_id,
        "case_num": case_num,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "permutations": results,
        "artifacts": {
            "case_log": str(case_log),
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
                    f" - perm_{perm_result.get('index'):02d} {perm_result.get('status')} "
                    f"mri={perm_result.get('permuted_basenames', {}).get('mri')} "
                    f"tdc={perm_result.get('permuted_basenames', {}).get('tdc')} "
                    f"pdf={perm_result.get('permuted_basenames', {}).get('pdf')} "
                    f"mri_bak={backups.get('mri')} "
                    f"tdc_bak={backups.get('tdc')} "
                    f"pdf_bak={backups.get('pdf')}\n"
                )
    except Exception as exc:
        failures.append(f"failed to append self-test log: {exc}")

    if not case_log.exists():
        failures.append(f"missing log: {case_log}")
    if not run_log.exists():
        failures.append(f"missing log: {run_log}")
    if not run_manifest.exists():
        failures.append(f"missing manifest: {run_manifest}")

    if failures:
        sys.stderr.write("[SELF-TEST] FAIL\n")
        for f in failures:
            sys.stderr.write(f" - {f}\n")
        return 1

    sys.stdout.write("[SELF-TEST] PASS\n")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="PEDA mini-pipeline controller")
    parser.add_argument("--config", help="Path to YAML/JSON config file")
    parser.add_argument("--root", help="Root output folder")
    parser.add_argument("--case", help="Case ID, e.g., 101_01-010")
    parser.add_argument(
        "--mri-input", help=r"MRI zip, e.g., E:\101-01-010\MRI-101-01-110.zip"
    )
    parser.add_argument(
        "--tdc-input", help=r"TDC zip, e.g., E:\101-01-010\TDC-101-01-110.zip"
    )
    parser.add_argument("--pdf-input", help="Treatment report PDF path")
    parser.add_argument("--scratch", help="Scratch dir (default <root>/<case>/scratch)")
    parser.add_argument(
        "--scratch-policy",
        choices=["local_temp", "case_root"],
        help="Scratch location policy when --scratch is not set",
    )
    parser.add_argument("--log-dir", help="Directory for log files")
    parser.add_argument("--log-level", help="Log level (INFO, DEBUG, etc.)")
    parser.add_argument("--run-id", help="Optional run identifier")
    parser.add_argument("--date-shift-days", type=int, help="TDC date shift (anonymization)")
    parser.add_argument("--self-test", action="store_true", help="Run built-in self-test and exit")
    add_bool_arg(parser, "clean_scratch", "Delete scratch after success")
    add_bool_arg(parser, "skip_mri", "Skip MRI step")
    add_bool_arg(parser, "skip_tdc", "Skip TDC step")
    add_bool_arg(parser, "dry_run", "Only validate and log planned actions")
    add_bool_arg(parser, "hash_outputs", "Compute SHA-256 hashes for outputs")
    add_bool_arg(parser, "test_mode", "Fast test-mode (skip heavy steps)")
    add_bool_arg(
        parser, "allow_workspace_zips", "Allow zip archives under TDC workspace"
    )
    add_bool_arg(
        parser,
        "legacy_filename_rules",
        "Enable legacy filename-based auto discovery and output naming",
    )
    args = parser.parse_args()

    if args.self_test:
        return _run_self_test()

    cli_overrides = {
        "root": args.root,
        "case": args.case,
        "mri_input": args.mri_input,
        "tdc_input": args.tdc_input,
        "pdf_input": args.pdf_input,
        "scratch": args.scratch,
        "scratch_policy": args.scratch_policy,
        "clean_scratch": args.clean_scratch,
        "date_shift_days": args.date_shift_days,
        "skip_mri": args.skip_mri,
        "skip_tdc": args.skip_tdc,
        "log_dir": args.log_dir,
        "log_level": args.log_level,
        "run_id": args.run_id,
        "dry_run": args.dry_run,
        "hash_outputs": args.hash_outputs,
        "test_mode": args.test_mode,
        "allow_workspace_zips": args.allow_workspace_zips,
        "legacy_filename_rules": args.legacy_filename_rules,
    }

    try:
        cfg, run_id = resolve_config(
            config_path=args.config,
            cli_overrides=cli_overrides,
        )
    except ValidationError as exc:
        sys.stderr.write(f"[ERROR] {exc}\n")
        return exc.code

    root: Path = cfg["root"]
    case: str = cfg["case"]
    case_dir = cfg.get("case_dir") or (root / case)
    mr_dir = cfg.get("mr_dir") or (case_dir / "MR DICOM")
    tdc_dir = cfg.get("tdc_dir") or (case_dir / "TDC Sessions")
    misc_dir = cfg.get("misc_dir") or (case_dir / "Misc")
    scratch: Path = cfg["scratch"] or (case_dir / "scratch")
    if cfg.get("log_dir"):
        log_dir = cfg["log_dir"]
    else:
        log_dir = (misc_dir / "Logs") if case_dir.exists() else (Path.cwd() / "logs")
    log_level: str = cfg["log_level"]
    dry_run: bool = cfg["dry_run"]
    skip_mri: bool = cfg["skip_mri"]
    skip_tdc: bool = cfg["skip_tdc"]
    date_shift_days: int = cfg["date_shift_days"]
    clean_scratch: bool = cfg["clean_scratch"]
    hash_outputs: bool = cfg["hash_outputs"]
    test_mode: bool = cfg["test_mode"]
    allow_workspace_zips: bool = cfg["allow_workspace_zips"]
    legacy_filename_rules: bool = cfg["legacy_filename_rules"]
    run_block = cfg.get("run")
    if not isinstance(run_block, dict):
        run_block = {}
    flags = run_block.get("flags")
    if not isinstance(flags, dict):
        flags = {}
        run_block["flags"] = flags
    flags["test_mode"] = test_mode
    flags["allow_workspace_zips"] = allow_workspace_zips
    flags["legacy_filename_rules"] = legacy_filename_rules
    cfg["run"] = run_block

    logger, log_file, rich_available = init_logger(
        case=case, run_id=run_id, log_dir=log_dir, log_level=log_level
    )
    logger.info("Run start case=%s run_id=%s", case, run_id)
    logger.info("Dry run: %s", dry_run)
    logger.info("Test mode: %s", test_mode)
    logger.info("Allow workspace zips: %s", allow_workspace_zips)
    logger.info("Legacy filename rules: %s", legacy_filename_rules)
    raw_paths = cfg.get("_raw_paths") if isinstance(cfg.get("_raw_paths"), dict) else {}
    logger.info("Resolved inputs: mri=%s tdc=%s", cfg.get("mri_input"), cfg.get("tdc_input"))
    auto_info = cfg.get("auto_discovery") or {}
    for key in ("mri", "tdc"):
        info = auto_info.get(key)
        if not info:
            continue
        candidates = info.get("candidates") or []
        if len(candidates) > 1:
            logger.info(
                "Auto-discovery %s: pick=%s filtered_by_case_id=%s selected=%s",
                key,
                info.get("pick"),
                info.get("filtered_by_case_id"),
                info.get("selected"),
            )
            for cand in candidates:
                logger.info(
                    " - candidate: %s (mtime=%s size=%s)",
                    cand.get("path"),
                    cand.get("mtime"),
                    cand.get("size_bytes"),
                )

    manifest_dir = cfg.get("manifest_dir") or log_dir
    manifest_name = cfg.get("manifest_name") or f"{case}__{run_id}__manifest.json"
    manifest_path = Path(manifest_dir) / manifest_name
    step_results: Dict[str, Any] = {}
    artifacts: Dict[str, Any] = {"inputs": {}, "outputs": {}}
    planned_actions = _build_plan(
        case_dir=case_dir,
        misc_dir=misc_dir,
        scratch=scratch,
        mr_dir=mr_dir,
        tdc_dir=tdc_dir,
        log_dir=log_dir,
        manifest_path=manifest_path,
        run_id=run_id,
        case=case,
        mri_input=Path(cfg["mri_input"]) if cfg["mri_input"] else None,
        tdc_input=Path(cfg["tdc_input"]) if cfg["tdc_input"] else None,
        pdf_input=Path(cfg["pdf_input"]) if cfg.get("pdf_input") else None,
        skip_mri=skip_mri,
        skip_tdc=skip_tdc,
        test_mode=test_mode,
        allow_workspace_zips=allow_workspace_zips,
        clean_scratch=clean_scratch,
        legacy_names=legacy_filename_rules,
    )

    status_mgr = StatusManager() if rich_available else None
    try:
        if status_mgr:
            status_mgr.__enter__()

        with StepTimer(
            logger=logger, step_name="Controller validations", results=step_results, status_mgr=status_mgr
        ):
            expected_case_name = case
            expected_mr_name = "MR DICOM"
            expected_tdc_name = "TDC Sessions"
            expected_misc_name = "Misc"

            legacy_misc = case_dir / f"{case} Misc"
            legacy_mr = case_dir / f"{case} MR DICOM"
            legacy_tdc = case_dir / f"{case} TDC Sessions"
            legacy_dirs = [legacy_misc, legacy_mr, legacy_tdc]
            legacy_present = [p for p in legacy_dirs if p.exists()]
            if legacy_present:
                if all(p.exists() for p in (misc_dir, mr_dir, tdc_dir)):
                    logger.warning(
                        "Legacy case-prefixed folders exist; using unprefixed schema. legacy=%s",
                        [str(p) for p in legacy_present],
                    )
                else:
                    logger.warning(
                        "Legacy case-prefixed folders exist; creating unprefixed folders at %s, %s, %s",
                        misc_dir,
                        mr_dir,
                        tdc_dir,
                    )

            if not case_dir.exists() and dry_run:
                logger.warning(
                    "Case directory does not exist yet (dry-run): %s", case_dir
                )
            if dry_run:
                if not case_dir.exists():
                    logger.info(
                        "Would create output folders under case_dir=%s (Misc=%s, MR DICOM=%s, TDC Sessions=%s)",
                        case_dir,
                        case_dir / expected_misc_name,
                        case_dir / expected_mr_name,
                        case_dir / expected_tdc_name,
                    )
            else:
                case_dir.mkdir(parents=True, exist_ok=True)
                misc_dir.mkdir(parents=True, exist_ok=True)
                mr_dir.mkdir(parents=True, exist_ok=True)
                tdc_dir.mkdir(parents=True, exist_ok=True)
                (tdc_dir / "applog" / "Logs").mkdir(parents=True, exist_ok=True)

            if not skip_mri:
                if not cfg["mri_input"]:
                    raise ValidationError("--mri-input required (or set skip_mri)")
                _validate_zip(
                    Path(cfg["mri_input"]), "MRI input", raw_paths.get("mri_input")
                )
                artifacts["inputs"]["mri_input"] = Path(cfg["mri_input"])

            if not skip_tdc:
                if not cfg["tdc_input"]:
                    raise ValidationError("--tdc-input required (or set skip_tdc)")
                _validate_zip(
                    Path(cfg["tdc_input"]), "TDC input", raw_paths.get("tdc_input")
                )
                artifacts["inputs"]["tdc_input"] = Path(cfg["tdc_input"])

            if cfg.get("pdf_input"):
                pdf_path = Path(cfg["pdf_input"])
                if pdf_path.exists() and pdf_path.is_file():
                    artifacts["inputs"]["pdf_input"] = pdf_path
                else:
                    logger.warning("Treatment report not found: %s", pdf_path)

            if not dry_run:
                scratch.mkdir(parents=True, exist_ok=True)
                if not scratch.exists():
                    raise ValidationError(f"Scratch dir could not be created: {scratch}")

        logger.info("Planned actions:")
        for item in planned_actions:
            logger.info(" - %s", item)

        if dry_run:
            step_results["MRI"] = {"status": "SKIP", "duration_s": 0.0, "error": "dry-run"}
            step_results["TDC"] = {"status": "SKIP", "duration_s": 0.0, "error": "dry-run"}
        else:
            if not skip_mri:
                with StepTimer(
                    logger=logger, step_name="MRI", results=step_results, status_mgr=status_mgr
                ):
                    mri_artifacts = MRI_proc.run(
                        root=root,
                        case=case,
                        input_zip=Path(cfg["mri_input"]),
                        mr_dir=mr_dir,
                        scratch=scratch,
                        logger=logger,
                        dry_run=False,
                        legacy_names=legacy_filename_rules,
                    )
                    artifacts["outputs"]["mri"] = mri_artifacts
                    _assert_exists(mri_artifacts["final_dir"], "MRI final dir")
            else:
                step_results["MRI"] = {"status": "SKIP", "duration_s": 0.0, "error": "skip_mri"}

            if not skip_tdc:
                with StepTimer(
                    logger=logger, step_name="TDC", results=step_results, status_mgr=status_mgr
                ):
                    tdc_artifacts = TDC_proc.run(
                        root=root,
                        case=case,
                        input_zip=Path(cfg["tdc_input"]),
                        tdc_dir=tdc_dir,
                        misc_dir=misc_dir,
                        scratch=scratch,
                        date_shift_days=date_shift_days,
                        logger=logger,
                        dry_run=False,
                        test_mode=test_mode,
                        allow_workspace_zips=allow_workspace_zips,
                        legacy_filename_rules=legacy_filename_rules,
                        step_results=step_results,
                        status_mgr=status_mgr,
                    )
                    artifacts["outputs"]["tdc"] = tdc_artifacts
                    _assert_exists(tdc_artifacts["final_session"], "TDC final session")
                    if tdc_artifacts.get("local_db"):
                        _assert_exists(tdc_artifacts["local_db"], "TDC local.db")
                    for z in tdc_artifacts.get("session_zips", []):
                        _assert_exists(Path(z), "TDC session zip")
            else:
                step_results["TDC"] = {"status": "SKIP", "duration_s": 0.0, "error": "skip_tdc"}

        with StepTimer(
            logger=logger, step_name="Treatment report", results=step_results, status_mgr=status_mgr
        ):
            pdf_input = Path(cfg["pdf_input"]) if cfg.get("pdf_input") else None
            target_pdf = None
            if pdf_input:
                if legacy_filename_rules:
                    target_pdf = misc_dir / f"{case}_TreatmentReport.pdf"
                else:
                    target_pdf = misc_dir / pdf_input.name
            if dry_run:
                if pdf_input and target_pdf:
                    logger.info("Dry-run: would copy report %s -> %s", pdf_input, target_pdf)
                else:
                    logger.info("Dry-run: no treatment report configured.")
            else:
                if pdf_input and target_pdf and pdf_input.exists():
                    target_pdf.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(pdf_input, target_pdf)
                    logger.info("Treatment report copied: %s", target_pdf)
                elif pdf_input:
                    logger.warning("Treatment report missing: %s", pdf_input)

        with StepTimer(
            logger=logger, step_name="structure_guard", results=step_results, status_mgr=status_mgr
        ):
            if dry_run:
                logger.info("Dry-run: structure_guard would validate/fix layout.")
            else:
                import src.structure_guard as sg

                pdf_candidates = []
                if legacy_filename_rules:
                    pdf_expected = misc_dir / f"{case}_TreatmentReport.pdf"
                    if not pdf_expected.exists():
                        pdf_candidates = [
                            p for p in case_dir.rglob("*.pdf") if p != pdf_expected
                        ]
                        if not pdf_candidates:
                            logger.warning(
                                "Treatment report missing; expected %s",
                                pdf_expected,
                            )

                errs = sg.verify(
                    case_dir,
                    case,
                    allow_missing_pdf=True,
                    misc_dir_name=misc_dir.name,
                    mr_dir_name=mr_dir.name,
                    tdc_dir_name=tdc_dir.name,
                    legacy_names=legacy_filename_rules,
                )
                force_fix = bool(pdf_candidates)
                if errs or force_fix:
                    logger.info("structure_guard detected layout issues.")
                    for e in errs:
                        logger.info(" - %s", e)
                    changes = sg.fix(
                        case_dir,
                        case,
                        misc_dir_name=misc_dir.name,
                        mr_dir_name=mr_dir.name,
                        tdc_dir_name=tdc_dir.name,
                        legacy_names=legacy_filename_rules,
                    )
                    if changes:
                        for c in changes:
                            logger.info(" - %s", c)
                    errs2 = sg.verify(
                        case_dir,
                        case,
                        allow_missing_pdf=True,
                        misc_dir_name=misc_dir.name,
                        mr_dir_name=mr_dir.name,
                        tdc_dir_name=tdc_dir.name,
                        legacy_names=legacy_filename_rules,
                    )
                    if errs2:
                        raise ProcessingError(f"structure_guard failed: {errs2}")

        with StepTimer(
            logger=logger, step_name="Finalization", results=step_results, status_mgr=status_mgr
        ):
            if clean_scratch and not dry_run:
                shutil.rmtree(scratch, ignore_errors=True)
                logger.info("Scratch deleted: %s", scratch)

        status = "SUCCESS"
        return_code = 0
    except ValidationError as exc:
        logger.error("Validation error: %s", exc)
        status = "FAILED"
        return_code = exc.code
    except ProcessingError as exc:
        logger.error("Processing error: %s", exc)
        status = "FAILED"
        return_code = exc.code
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        status = "FAILED"
        return_code = UnexpectedError.code
    finally:
        if status_mgr:
            status_mgr.__exit__(None, None, None)

        manifest_payload = {
            "run_id": run_id,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "case": case,
            "status": status,
            "test_mode": test_mode,
            "hostname": socket.gethostname(),
            "user": getpass.getuser(),
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "config": {k: str(v) if isinstance(v, Path) else v for k, v in cfg.items()},
            "steps": step_results,
            "plan": planned_actions,
            "inputs": {},
            "outputs": {},
            "versions": {},
            "log_file": str(log_file),
        }

        try:
            from importlib.metadata import version  # type: ignore

            manifest_payload["versions"]["rich"] = version("rich")
        except Exception:
            manifest_payload["versions"]["rich"] = None
        try:
            from importlib.metadata import version  # type: ignore

            manifest_payload["versions"]["yaml"] = version("PyYAML")
        except Exception:
            manifest_payload["versions"]["yaml"] = None

        for label, path in artifacts.get("inputs", {}).items():
            manifest_payload["inputs"][label] = file_metadata(
                Path(path), compute_hash=hash_outputs
            )

        pdf_output = None
        if cfg.get("pdf_input"):
            pdf_name = (
                f"{case}_TreatmentReport.pdf"
                if legacy_filename_rules
                else Path(cfg["pdf_input"]).name
            )
            pdf_output = misc_dir / pdf_name
        if pdf_output and pdf_output.exists():
            manifest_payload["outputs"]["treatment_report"] = file_metadata(
                pdf_output, compute_hash=hash_outputs
            )

        if "mri" in artifacts.get("outputs", {}):
            mri_out = artifacts["outputs"]["mri"]
            manifest_payload["outputs"]["mri_unzipped_dir"] = file_metadata(
                mri_out["final_dir"], compute_hash=False
            )
            if mri_out.get("backup_info"):
                manifest_payload["inputs"]["mri_backup"] = mri_out["backup_info"]

        if "tdc" in artifacts.get("outputs", {}):
            tdc_out = artifacts["outputs"]["tdc"]
            manifest_payload["outputs"]["tdc_final_session"] = file_metadata(
                tdc_out["final_session"], compute_hash=False
            )
            if tdc_out.get("backup_info"):
                manifest_payload["inputs"]["tdc_backup"] = tdc_out["backup_info"]
            if tdc_out.get("local_db"):
                manifest_payload["outputs"]["tdc_local_db"] = file_metadata(
                    tdc_out["local_db"], compute_hash=hash_outputs
                )
            if tdc_out.get("session_zips"):
                manifest_payload["outputs"]["tdc_session_zips"] = [
                    file_metadata(Path(p), compute_hash=hash_outputs)
                    for p in tdc_out["session_zips"]
                ]

        manifest_payload["outputs"]["log_file"] = file_metadata(
            log_file, compute_hash=False
        )

        if dry_run:
            manifest_payload["outputs"]["note"] = "dry-run: outputs not created"

        try:
            write_manifest(manifest_path, manifest_payload)
        except Exception as exc:
            logger.error("Failed to write manifest: %s", exc)

        try:
            log_dir.mkdir(parents=True, exist_ok=True)
            run_log_copy = log_dir / f"RUN_{run_id}.log"
            if log_file.exists() and log_file.resolve() != run_log_copy.resolve():
                shutil.copy2(log_file, run_log_copy)
            run_manifest = log_dir / f"RUN_{run_id}_manifest.json"
            write_manifest(run_manifest, manifest_payload)
        except Exception as exc:
            logger.error("Failed to write legacy run artifacts: %s", exc)

        logger.info("Run complete: %s", status)
        logger.info("Artifacts:")
        if not skip_mri:
            logger.info(
                " - MRI final dir: %s",
                artifacts.get("outputs", {}).get("mri", {}).get("final_dir", "n/a"),
            )
        if not skip_tdc:
            logger.info(
                " - TDC final session: %s",
                artifacts.get("outputs", {}).get("tdc", {}).get("final_session", "n/a"),
            )
        logger.info(" - Log file: %s", log_file)
        logger.info(" - Manifest: %s", manifest_path)

        manifest_meta = file_metadata(manifest_path, compute_hash=False)

        logger.info("Proof:")
        for key, meta in manifest_payload.get("outputs", {}).items():
            if isinstance(meta, dict) and meta.get("exists") is not False:
                logger.info(
                    " - %s: size=%s mtime=%s sha256=%s",
                    key,
                    meta.get("size_bytes"),
                    meta.get("mtime"),
                    meta.get("sha256"),
                )
        if manifest_meta.get("exists"):
            logger.info(
                " - manifest: size=%s mtime=%s",
                manifest_meta.get("size_bytes"),
                manifest_meta.get("mtime"),
            )

        if status == "SUCCESS":
            logger.info("Canonical schema tree:")
            schema_lines = [
                str(case_dir),
                str(mr_dir),
                str(tdc_dir),
                str(misc_dir),
            ]
            for line in schema_lines:
                logger.info(line)

        sys.stdout.write(f"{status} case={case} run_id={run_id}\n")

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
