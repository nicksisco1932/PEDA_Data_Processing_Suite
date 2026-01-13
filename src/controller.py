# PURPOSE: CLI entrypoint that orchestrates pipeline runs and writes logs/manifests.
# INPUTS: CLI args and resolved config paths (MRI/TDC/PDF, root, scratch, flags).
# OUTPUTS: Case output tree, run log, and manifest JSON.
# NOTES: Handles run status and error reporting for downstream steps.
from __future__ import annotations

import argparse
import sys
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import MRI_proc
import TDC_proc
from src.annon_logs import get_annon_logs_dir
from src.pipeline_steps.applog_step import install_tdc_log
from src.pipeline_steps.cleanup_artifacts import cleanup_artifacts
from src.pipeline_steps.dicom_anon_stub import run_dicom_anon_stub
from src.pipeline_steps.localdb_step import run_localdb_step
from src.pipeline_steps.peda_step import run_peda_step
from src.pipeline_steps.unzip_inputs import expand_archives
from src.phi.dicom_rules import build_dicom_rules
from src.paths import (
    run_logs_dir,
    run_manifest_path,
    assert_no_forbidden_log_dirs,
    delete_forbidden_log_dirs,
)
from src.logutil import (
    init_logger,
    StatusManager,
    StepTimer,
    ValidationError,
    ProcessingError,
    UnexpectedError,
)
from src.pipeline_config import add_bool_arg, resolve_config
from src.policy import RunPolicy, policy_from_args_and_cfg
from src.reporting.manifest import build_manifest_payload, file_metadata, write_manifest
from src.selftest.runner import run_self_test


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
    plan.append(f"Create output dir: {case_dir / 'annon_logs'}")
    plan.append(f"Create output dir: {case_dir / 'run_logs'}")
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




def parse_and_resolve_config(
    argv: Optional[List[str]] = None,
) -> tuple[argparse.Namespace, Dict[str, Any], str, Dict[str, Any]]:
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
    parser.add_argument(
        "--date-shift-days", type=int, help="TDC date shift (anonymization)"
    )
    parser.add_argument("--localdb-path", help="Explicit local.db path for checks")
    parser.add_argument("--peda-version", help="PEDA version string (e.g., v9.1.3)")
    parser.add_argument("--peda-mode", help="PEDA mode (stub or matlab)")
    parser.add_argument(
        "--self-test", action="store_true", help="Run built-in self-test and exit"
    )
    parser.add_argument("--keep-temp", action="store_true", help="Keep self-test temp dirs")
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
    add_bool_arg(parser, "localdb_enabled", "Enable local.db check/anonymization step")
    add_bool_arg(parser, "localdb_check_only", "Check local.db without anonymization")
    add_bool_arg(parser, "localdb_strict", "Fail pipeline on local.db findings")
    add_bool_arg(parser, "peda_enabled", "Enable PEDA step (stub by default)")
    args = parser.parse_args(argv)

    if args.self_test:
        return args, {}, "", {}

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
        "localdb_enabled": args.localdb_enabled,
        "localdb_check_only": args.localdb_check_only,
        "localdb_strict": args.localdb_strict,
        "localdb_path": args.localdb_path,
        "peda_enabled": args.peda_enabled,
        "peda_version": args.peda_version,
        "peda_mode": args.peda_mode,
    }

    cfg, run_id = resolve_config(
        config_path=args.config,
        cli_overrides=cli_overrides,
    )
    raw_paths = cfg.get("_raw_paths") if isinstance(cfg.get("_raw_paths"), dict) else {}
    return args, cfg, run_id, raw_paths


def derive_run_context(cfg: Dict[str, Any]) -> Dict[str, Any]:
    root: Path = cfg["root"]
    case: str = cfg["case"]
    case_dir = cfg.get("case_dir") or (root / case)
    mr_dir = cfg.get("mr_dir") or (case_dir / "MR DICOM")
    tdc_dir = cfg.get("tdc_dir") or (case_dir / "TDC Sessions")
    misc_dir = cfg.get("misc_dir") or (case_dir / "Misc")
    scratch: Path = cfg["scratch"] or (case_dir / "scratch")
    log_dir = run_logs_dir(case_dir)
    return {
        "root": root,
        "case": case,
        "case_dir": case_dir,
        "mr_dir": mr_dir,
        "tdc_dir": tdc_dir,
        "misc_dir": misc_dir,
        "scratch": scratch,
        "log_dir": log_dir,
        "log_level": cfg["log_level"],
    }


def validate_inputs_and_prepare_dirs(
    *,
    logger: Any,
    step_results: Dict[str, Any],
    status_mgr: Optional[StatusManager],
    cfg: Dict[str, Any],
    raw_paths: Dict[str, Any],
    run_ctx: Dict[str, Any],
    inputs: Dict[str, Path],
    policy: RunPolicy,
) -> Optional[Path]:
    pdf_input_path: Optional[Path] = None

    def _require_zip_input(
        cfg_key: str,
        label: str,
        raw_key: str,
        missing_msg: str,
    ) -> None:
        if not cfg[cfg_key]:
            raise ValidationError(missing_msg)
        input_path = Path(cfg[cfg_key])
        _validate_zip(input_path, label, raw_paths.get(raw_key))
        inputs[cfg_key] = input_path

    with StepTimer(
        logger=logger,
        step_name="Controller validations",
        results=step_results,
        status_mgr=status_mgr,
    ):
        expected_mr_name = "MR DICOM"
        expected_tdc_name = "TDC Sessions"
        expected_misc_name = "Misc"

        case = run_ctx["case"]
        case_dir = run_ctx["case_dir"]
        misc_dir = run_ctx["misc_dir"]
        mr_dir = run_ctx["mr_dir"]
        tdc_dir = run_ctx["tdc_dir"]

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

        if not case_dir.exists() and policy.dry_run:
            logger.warning("Case directory does not exist yet (dry-run): %s", case_dir)
        if policy.dry_run:
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
            get_annon_logs_dir(case_dir)
            run_ctx["log_dir"].mkdir(parents=True, exist_ok=True)
            pre_removed = delete_forbidden_log_dirs(case_dir, logger=logger)
            if pre_removed:
                logger.warning(
                    "Removed forbidden log dirs before processing: %s",
                    [str(p) for p in pre_removed],
                )

        if not policy.skip_mri:
            _require_zip_input(
                "mri_input",
                "MRI input",
                "mri_input",
                "--mri-input required (or set skip_mri)",
            )

        if not policy.skip_tdc:
            _require_zip_input(
                "tdc_input",
                "TDC input",
                "tdc_input",
                "--tdc-input required (or set skip_tdc)",
            )

        if cfg.get("pdf_input"):
            pdf_input_path = Path(cfg["pdf_input"])
            if pdf_input_path.exists() and pdf_input_path.is_file():
                inputs["pdf_input"] = pdf_input_path
            else:
                logger.warning("Treatment report not found: %s", pdf_input_path)

        if not policy.dry_run:
            scratch = run_ctx["scratch"]
            scratch.mkdir(parents=True, exist_ok=True)
            if not scratch.exists():
                raise ValidationError(f"Scratch dir could not be created: {scratch}")

    return pdf_input_path


def run_pipeline(
    *,
    logger: Any,
    step_results: Dict[str, Any],
    status_mgr: Optional[StatusManager],
    run_ctx: Dict[str, Any],
    inputs: Dict[str, Path],
    pdf_input_path: Optional[Path],
    artifacts: Dict[str, Any],
    policy: RunPolicy,
    pipeline_cfg: Dict[str, Any],
) -> None:
    logger.info("Run plan ready (dry_run=%s).", policy.dry_run)

    def _record_skip(step_name: str, reason: str) -> None:
        step_results[step_name] = {
            "status": "SKIP",
            "duration_s": 0.0,
            "error": reason,
        }

    if policy.dry_run:
        step_results["MRI"] = {"status": "SKIP", "duration_s": 0.0, "error": "dry-run"}
        step_results["TDC"] = {"status": "SKIP", "duration_s": 0.0, "error": "dry-run"}
        _record_skip("Unzip inputs", "dry-run")
        _record_skip("DICOM anonymization", "dry-run")
        _record_skip("local.db check", "dry-run")
        _record_skip("TDC applog", "dry-run")
        _record_skip("Cleanup forbidden log dirs", "dry-run")
        _record_skip("Cleanup artifacts", "dry-run")
        _record_skip("PEDA step", "dry-run")
    else:
        mri_artifacts: Dict[str, Any] | None = None
        tdc_artifacts: Dict[str, Any] | None = None

        if pipeline_cfg.get("unzip_inputs"):
            with StepTimer(
                logger=logger,
                step_name="Unzip inputs",
                results=step_results,
                status_mgr=status_mgr,
            ):
                input_zips = []
                if not policy.skip_mri and inputs.get("mri_input"):
                    input_zips.append(inputs["mri_input"])
                if not policy.skip_tdc and inputs.get("tdc_input"):
                    input_zips.append(inputs["tdc_input"])
                unzip_root = run_ctx["scratch"] / "unzipped_inputs"
                unzip_summary = expand_archives(input_zips, unzip_root)
                artifacts["outputs"]["unzipped_inputs"] = unzip_summary
        else:
            _record_skip("Unzip inputs", "disabled")

        if not policy.skip_mri:
            with StepTimer(
                logger=logger, step_name="MRI", results=step_results, status_mgr=status_mgr
            ):
                mri_artifacts = MRI_proc.run(
                    root=run_ctx["root"],
                    case=run_ctx["case"],
                    input_zip=inputs["mri_input"],
                    mr_dir=run_ctx["mr_dir"],
                    scratch=run_ctx["scratch"],
                    logger=logger,
                    dry_run=False,
                    legacy_names=policy.legacy_filename_rules,
                )
                artifacts["outputs"]["mri"] = mri_artifacts
                _assert_exists(mri_artifacts["final_dir"], "MRI final dir")
        else:
            step_results["MRI"] = {
                "status": "SKIP",
                "duration_s": 0.0,
                "error": "skip_mri",
            }

        if pipeline_cfg.get("dicom_anon", {}).get("enabled") and not policy.skip_mri:
            with StepTimer(
                logger=logger,
                step_name="DICOM anonymization",
                results=step_results,
                status_mgr=status_mgr,
            ):
                rules = build_dicom_rules(run_ctx["case"])
                mode = pipeline_cfg.get("dicom_anon", {}).get("mode", "stub")
                if mode == "stub":
                    dicom_summary = run_dicom_anon_stub(
                        mri_artifacts["final_dir"] if mri_artifacts else run_ctx["mr_dir"],
                        run_ctx["case"],
                        rules,
                    )
                else:
                    dicom_summary = {
                        "status": "skipped",
                        "reason": "mode_not_supported",
                        "mode": mode,
                    }
                    logger.warning("DICOM anonymization mode not supported: %s", mode)
                artifacts["outputs"]["dicom_anon"] = dicom_summary
        else:
            _record_skip(
                "DICOM anonymization",
                "disabled" if not policy.skip_mri else "skip_mri",
            )

        if not policy.skip_tdc:
            with StepTimer(
                logger=logger, step_name="TDC", results=step_results, status_mgr=status_mgr
            ):
                tdc_artifacts = TDC_proc.run(
                    root=run_ctx["root"],
                    case=run_ctx["case"],
                    input_zip=inputs["tdc_input"],
                    tdc_dir=run_ctx["tdc_dir"],
                    misc_dir=run_ctx["misc_dir"],
                    scratch=run_ctx["scratch"],
                    date_shift_days=policy.date_shift_days,
                    logger=logger,
                    dry_run=False,
                    test_mode=policy.test_mode,
                    allow_workspace_zips=policy.allow_workspace_zips,
                    legacy_filename_rules=policy.legacy_filename_rules,
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
            step_results["TDC"] = {
                "status": "SKIP",
                "duration_s": 0.0,
                "error": "skip_tdc",
            }

        if pipeline_cfg.get("localdb", {}).get("enabled"):
            db_path = pipeline_cfg.get("localdb", {}).get("path")
            if db_path:
                db_path = Path(db_path)
            elif tdc_artifacts:
                db_path = Path(tdc_artifacts["final_session"]) / "local.db"
            else:
                db_path = None

            if db_path and db_path.exists():
                with StepTimer(
                    logger=logger,
                    step_name="local.db check",
                    results=step_results,
                    status_mgr=status_mgr,
                ):
                    out_dir = get_annon_logs_dir(run_ctx["case_dir"])
                    localdb_summary = run_localdb_step(
                        db_path=db_path,
                        case_id=run_ctx["case"],
                        out_dir=out_dir,
                        enable_anon=pipeline_cfg.get("localdb", {}).get("enabled", True),
                        check_only=pipeline_cfg.get("localdb", {}).get("check_only", False),
                        strict=pipeline_cfg.get("localdb", {}).get("strict", True),
                    )
                    artifacts["outputs"]["localdb"] = localdb_summary
            else:
                _record_skip("local.db check", "missing_db")
                logger.warning("local.db missing; skipping localdb step: %s", db_path)
        else:
            _record_skip("local.db check", "disabled")

        if not policy.skip_tdc:
            with StepTimer(
                logger=logger,
                step_name="TDC applog",
                results=step_results,
                status_mgr=status_mgr,
            ):
                search_roots = []
                if tdc_artifacts and tdc_artifacts.get("tdc_logs_root"):
                    search_roots.append(tdc_artifacts["tdc_logs_root"])
                applog_summary = install_tdc_log(
                    run_ctx["case_dir"],
                    run_ctx["case"],
                    search_roots=search_roots or None,
                )
                artifacts["outputs"]["applog"] = applog_summary
                logger.info("TDC applog step: %s", applog_summary)
        else:
            _record_skip("TDC applog", "skip_tdc")

        if pipeline_cfg.get("cleanup", {}).get("enabled"):
            with StepTimer(
                logger=logger,
                step_name="Cleanup artifacts",
                results=step_results,
                status_mgr=status_mgr,
            ):
                cleanup_summary = cleanup_artifacts(
                    run_ctx["scratch"],
                    patterns=pipeline_cfg.get("cleanup", {}).get("patterns", []),
                    dry_run=pipeline_cfg.get("cleanup", {}).get("dry_run", False),
                )
                artifacts["outputs"]["cleanup"] = cleanup_summary
        else:
            _record_skip("Cleanup artifacts", "disabled")

        if pipeline_cfg.get("peda", {}).get("enabled"):
            with StepTimer(
                logger=logger,
                step_name="PEDA step",
                results=step_results,
                status_mgr=status_mgr,
            ):
                peda_summary = run_peda_step(
                    case_id=run_ctx["case"],
                    case_dir=run_ctx["case_dir"],
                    peda_version=pipeline_cfg.get("peda", {}).get("version", "v9.1.3"),
                    enabled=True,
                    mode=pipeline_cfg.get("peda", {}).get("mode", "stub"),
                )
                artifacts["outputs"]["peda"] = peda_summary
        else:
            _record_skip("PEDA step", "disabled")

        with StepTimer(
            logger=logger,
            step_name="Cleanup forbidden log dirs",
            results=step_results,
            status_mgr=status_mgr,
        ):
            removed = delete_forbidden_log_dirs(run_ctx["case_dir"], logger=logger)
            if removed:
                logger.warning(
                    "Forbidden log dirs created during run and removed: %s",
                    [str(p) for p in removed],
                )
            artifacts["outputs"]["forbidden_logs_cleanup"] = {
                "removed": [str(p) for p in removed]
            }
            assert_no_forbidden_log_dirs(run_ctx["case_dir"])

    with StepTimer(
        logger=logger,
        step_name="Treatment report",
        results=step_results,
        status_mgr=status_mgr,
    ):
        target_pdf = None
        if pdf_input_path:
            if policy.legacy_filename_rules:
                target_pdf = run_ctx["misc_dir"] / f"{run_ctx['case']}_TreatmentReport.pdf"
            else:
                target_pdf = run_ctx["misc_dir"] / pdf_input_path.name
        if policy.dry_run:
            if pdf_input_path and target_pdf:
                logger.info(
                    "Dry-run: would copy report %s -> %s", pdf_input_path, target_pdf
                )
            else:
                logger.info("Dry-run: no treatment report configured.")
        else:
            if pdf_input_path and target_pdf and pdf_input_path.exists():
                target_pdf.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(pdf_input_path, target_pdf)
                logger.info("Treatment report copied: %s", target_pdf)
            elif pdf_input_path:
                logger.warning("Treatment report missing: %s", pdf_input_path)

    with StepTimer(
        logger=logger, step_name="structure_guard", results=step_results, status_mgr=status_mgr
    ):
        import src.structure_guard as sg
        peda_version = pipeline_cfg.get("peda", {}).get("version", "v9.1.3")
        peda_enabled = pipeline_cfg.get("peda", {}).get("enabled", False)
        allowed_top_dirs: set[str] = set()
        forbidden_top_files: set[str] = set()
        if peda_enabled:
            allowed_top_dirs.add(f"{run_ctx['case']} PEDA{peda_version}-Video")
        else:
            forbidden_top_files.add(f"{run_ctx['case']} PEDA{peda_version}-Data.zip")

        if policy.legacy_filename_rules:
            pdf_expected = run_ctx["misc_dir"] / f"{run_ctx['case']}_TreatmentReport.pdf"
            if not pdf_expected.exists():
                pdf_candidates = [
                    p for p in run_ctx["case_dir"].rglob("*.pdf") if p != pdf_expected
                ]
                if not pdf_candidates:
                    logger.warning(
                        "Treatment report missing; expected %s",
                        pdf_expected,
                    )

        if policy.dry_run:
            initial_errs, _, _ = sg.enforce(
                run_ctx["case_dir"],
                run_ctx["case"],
                allow_missing_pdf=True,
                misc_dir_name=run_ctx["misc_dir"].name,
                mr_dir_name=run_ctx["mr_dir"].name,
                tdc_dir_name=run_ctx["tdc_dir"].name,
                legacy_names=policy.legacy_filename_rules,
                allowed_top_dirs=allowed_top_dirs,
                forbidden_top_files=forbidden_top_files,
                dry_run=True,
            )
            if initial_errs:
                logger.info("Dry-run: structure_guard would fail with:")
                for e in initial_errs:
                    logger.info(" - %s", e)
            else:
                logger.info("Dry-run: structure_guard would validate/fix layout.")
        else:
            initial_errs, final_errs, changes = sg.enforce(
                run_ctx["case_dir"],
                run_ctx["case"],
                allow_missing_pdf=True,
                misc_dir_name=run_ctx["misc_dir"].name,
                mr_dir_name=run_ctx["mr_dir"].name,
                tdc_dir_name=run_ctx["tdc_dir"].name,
                legacy_names=policy.legacy_filename_rules,
                allowed_top_dirs=allowed_top_dirs,
                forbidden_top_files=forbidden_top_files,
            )
            if initial_errs:
                logger.info("structure_guard detected layout issues.")
                for e in initial_errs:
                    logger.info(" - %s", e)
            if changes:
                for c in changes:
                    logger.info(" - %s", c)
            if final_errs:
                raise ProcessingError(f"structure_guard failed: {final_errs}")

    with StepTimer(
        logger=logger, step_name="Finalization", results=step_results, status_mgr=status_mgr
    ):
        if policy.clean_scratch and not policy.dry_run:
            shutil.rmtree(run_ctx["scratch"], ignore_errors=True)
            logger.info("Scratch deleted: %s", run_ctx["scratch"])


def finalize_and_write_manifest(
    *,
    logger: Any,
    cfg: Dict[str, Any],
    run_flags: Dict[str, Any],
    run_id: str,
    run_ctx: Dict[str, Any],
    manifest_path: Path,
    log_file: Path,
    planned_actions: List[str],
    step_results: Dict[str, Any],
    inputs: Dict[str, Path],
    artifacts: Dict[str, Any],
    status: str,
    policy: RunPolicy,
) -> None:
    config_for_manifest = dict(cfg)
    config_for_manifest["run"] = {"flags": run_flags}
    backups: Dict[str, Any] = {}
    outputs_meta: Dict[str, Any] = {}

    pdf_output = None
    if cfg.get("pdf_input"):
        pdf_name = (
            f"{run_ctx['case']}_TreatmentReport.pdf"
            if policy.legacy_filename_rules
            else Path(cfg["pdf_input"]).name
        )
        pdf_output = run_ctx["misc_dir"] / pdf_name
    if pdf_output and pdf_output.exists():
        outputs_meta["treatment_report"] = file_metadata(
            pdf_output, compute_hash=policy.hash_outputs
        )

    if "mri" in artifacts.get("outputs", {}):
        mri_out = artifacts["outputs"]["mri"]
        outputs_meta["mri_unzipped_dir"] = file_metadata(
            mri_out["final_dir"], compute_hash=False
        )
        if mri_out.get("backup_info"):
            backups["mri_backup"] = mri_out["backup_info"]

    if "tdc" in artifacts.get("outputs", {}):
        tdc_out = artifacts["outputs"]["tdc"]
        outputs_meta["tdc_final_session"] = file_metadata(
            tdc_out["final_session"], compute_hash=False
        )
        if tdc_out.get("backup_info"):
            backups["tdc_backup"] = tdc_out["backup_info"]
        if tdc_out.get("local_db"):
            outputs_meta["tdc_local_db"] = file_metadata(
                tdc_out["local_db"], compute_hash=policy.hash_outputs
            )
        if tdc_out.get("session_zips"):
            outputs_meta["tdc_session_zips"] = [
                file_metadata(Path(p), compute_hash=policy.hash_outputs)
                for p in tdc_out["session_zips"]
            ]

    manifest_payload = build_manifest_payload(
        cfg_for_manifest=config_for_manifest,
        run_id=run_id,
        case=run_ctx["case"],
        status=status,
        test_mode=policy.test_mode,
        log_file=log_file,
        planned_actions=planned_actions,
        step_results=step_results,
        inputs=inputs,
        backups=backups,
        outputs_meta=outputs_meta,
        hash_outputs=policy.hash_outputs,
        dry_run=policy.dry_run,
    )

    try:
        write_manifest(manifest_path, manifest_payload)
    except Exception as exc:
        logger.error("Failed to write manifest: %s", exc)


def main() -> int:
    try:
        args, cfg, run_id, raw_paths = parse_and_resolve_config()
    except ValidationError as exc:
        sys.stderr.write(f"[ERROR] {exc}\n")
        return exc.code

    if args.self_test:
        return run_self_test(keep_temp=args.keep_temp)

    policy = policy_from_args_and_cfg(args, cfg)
    run_flags = {
        "test_mode": policy.test_mode,
        "allow_workspace_zips": policy.allow_workspace_zips,
        "legacy_filename_rules": policy.legacy_filename_rules,
        "unzip_inputs": cfg.get("unzip_inputs"),
        "cleanup_enabled": cfg.get("cleanup_enabled"),
        "cleanup_dry_run": cfg.get("cleanup_dry_run"),
        "dicom_anon_enabled": cfg.get("dicom_anon_enabled"),
        "dicom_anon_mode": cfg.get("dicom_anon_mode"),
        "peda_enabled": cfg.get("peda_enabled"),
        "peda_version": cfg.get("peda_version"),
        "peda_mode": cfg.get("peda_mode"),
        "localdb_enabled": cfg.get("localdb_enabled"),
        "localdb_check_only": cfg.get("localdb_check_only"),
        "localdb_strict": cfg.get("localdb_strict"),
    }
    run_ctx = derive_run_context(cfg)
    pipeline_cfg = {
        "unzip_inputs": cfg.get("unzip_inputs"),
        "cleanup": {
            "enabled": cfg.get("cleanup_enabled"),
            "dry_run": cfg.get("cleanup_dry_run"),
            "patterns": cfg.get("cleanup_patterns"),
        },
        "dicom_anon": {
            "enabled": cfg.get("dicom_anon_enabled"),
            "mode": cfg.get("dicom_anon_mode"),
        },
        "peda": {
            "enabled": cfg.get("peda_enabled"),
            "version": cfg.get("peda_version"),
            "mode": cfg.get("peda_mode"),
        },
        "localdb": {
            "enabled": cfg.get("localdb_enabled"),
            "check_only": cfg.get("localdb_check_only"),
            "strict": cfg.get("localdb_strict"),
            "path": cfg.get("localdb_path"),
        },
    }

    logger, log_file, rich_available = init_logger(
        case=run_ctx["case"],
        run_id=run_id,
        log_dir=run_ctx["log_dir"],
        log_level=run_ctx["log_level"],
    )
    logger.info("Run start case=%s run_id=%s", run_ctx["case"], run_id)
    logger.info("Test mode: %s", policy.test_mode)
    logger.info("Allow workspace zips: %s", policy.allow_workspace_zips)
    logger.info("Legacy filename rules: %s", policy.legacy_filename_rules)
    logger.info(
        "Resolved inputs: mri=%s tdc=%s",
        cfg.get("mri_input"),
        cfg.get("tdc_input"),
    )

    manifest_path = run_manifest_path(run_ctx["case_dir"], run_ctx["case"], run_id)
    step_results: Dict[str, Any] = {}
    inputs: Dict[str, Path] = {}
    artifacts: Dict[str, Any] = {"outputs": {}}
    planned_actions = _build_plan(
        case_dir=run_ctx["case_dir"],
        misc_dir=run_ctx["misc_dir"],
        scratch=run_ctx["scratch"],
        mr_dir=run_ctx["mr_dir"],
        tdc_dir=run_ctx["tdc_dir"],
        log_dir=run_ctx["log_dir"],
        manifest_path=manifest_path,
        run_id=run_id,
        case=run_ctx["case"],
        mri_input=Path(cfg["mri_input"]) if cfg["mri_input"] else None,
        tdc_input=Path(cfg["tdc_input"]) if cfg["tdc_input"] else None,
        pdf_input=Path(cfg["pdf_input"]) if cfg.get("pdf_input") else None,
        skip_mri=policy.skip_mri,
        skip_tdc=policy.skip_tdc,
        test_mode=policy.test_mode,
        allow_workspace_zips=policy.allow_workspace_zips,
        clean_scratch=policy.clean_scratch,
        legacy_names=policy.legacy_filename_rules,
    )

    status_mgr = StatusManager() if rich_available else None
    pdf_input_path: Optional[Path] = None

    try:
        if status_mgr:
            status_mgr.__enter__()
        pdf_input_path = validate_inputs_and_prepare_dirs(
            logger=logger,
            step_results=step_results,
            status_mgr=status_mgr,
            cfg=cfg,
            raw_paths=raw_paths,
            run_ctx=run_ctx,
            inputs=inputs,
            policy=policy,
        )
        run_pipeline(
            logger=logger,
            step_results=step_results,
            status_mgr=status_mgr,
            run_ctx=run_ctx,
            inputs=inputs,
            pdf_input_path=pdf_input_path,
            artifacts=artifacts,
            policy=policy,
            pipeline_cfg=pipeline_cfg,
        )

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

        finalize_and_write_manifest(
            logger=logger,
            cfg=cfg,
            run_flags=run_flags,
            run_id=run_id,
            run_ctx=run_ctx,
            manifest_path=manifest_path,
            log_file=log_file,
            planned_actions=planned_actions,
            step_results=step_results,
            inputs=inputs,
            artifacts=artifacts,
            status=status,
            policy=policy,
        )

        logger.info("Run complete: %s", status)
        logger.info("Artifacts:")
        if not policy.skip_mri:
            logger.info(
                " - MRI final dir: %s",
                artifacts.get("outputs", {}).get("mri", {}).get("final_dir", "n/a"),
            )
        if not policy.skip_tdc:
            logger.info(
                " - TDC final session: %s",
                artifacts.get("outputs", {}).get("tdc", {}).get("final_session", "n/a"),
            )
        logger.info(" - Log file: %s", log_file)
        logger.info(" - Manifest: %s", manifest_path)

        sys.stdout.write(f"{status} case={run_ctx['case']} run_id={run_id}\n")

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
