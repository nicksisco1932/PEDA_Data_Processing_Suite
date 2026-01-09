from __future__ import annotations

import argparse
import getpass
import platform
import socket
import sys
import shutil
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import MRI_proc
import TDC_proc
from logutil import (
    init_logger,
    StatusManager,
    StepTimer,
    ValidationError,
    ProcessingError,
    UnexpectedError,
)
from manifest import file_metadata, write_manifest
from pipeline_config import add_bool_arg, resolve_config


def _validate_zip(path: Path, label: str) -> None:
    if not path.exists() or not path.is_file():
        raise ValidationError(f"{label} not found: {path}")
    if path.suffix.lower() != ".zip":
        raise ValidationError(f"{label} must be a .zip: {path}")


def _assert_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise ProcessingError(f"Missing expected {label}: {path}")


def _cleanup_ingest_dir(ingest_dir: Path, logger) -> None:
    if not ingest_dir.exists():
        return
    for item in ingest_dir.iterdir():
        try:
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            else:
                item.unlink()
        except Exception:
            logger.warning("Failed to delete staged input: %s", item)


def _build_plan(
    *,
    case_dir: Path,
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
    clean_scratch: bool,
    ingest_mode: str,
    ingest_dir: Path,
    ingest_attempts: int,
    ingest_verify: bool,
    ingest_keep_staged: bool,
) -> List[str]:
    plan: List[str] = []
    plan.append(f"Create case dir: {case_dir}")
    plan.append(f"Create output dir: {case_dir / (case + ' Misc')}")
    plan.append(f"Create output dir: {case_dir / (case + ' MR DICOM')}")
    plan.append(f"Create output dir: {case_dir / (case + ' TDC Sessions')}")
    plan.append(f"Create output dir: {case_dir / (case + ' TDC Sessions') / 'applog' / 'Logs'}")
    plan.append(f"TDC Raw output dir created if TDC produces it: {case_dir / (case + ' TDC Sessions') / 'Raw'}")
    plan.append(f"Create scratch dir: {scratch}")
    if ingest_mode == "stage_to_scratch":
        plan.append(f"Create ingest dir: {ingest_dir}")
        plan.append(
            f"Stage inputs to scratch (attempts={ingest_attempts} verify={ingest_verify} keep_staged={ingest_keep_staged})"
        )
    if not skip_mri and mri_input:
        mri_source = str(mri_input)
        if ingest_mode == "stage_to_scratch":
            mri_source = "<staged_mri_zip>"
        plan.append(
            f"Copy MRI zip to scratch backup: {mri_source} -> {scratch / (mri_input.name + '.bak')}"
        )
        plan.append(f"Extract MRI zip into temp under: {scratch}")
        plan.append(f"Create MRI anonymized zip in scratch: {scratch / 'MRI_anonymized.zip'}")
        plan.append(f"Copy MRI final zip to: {mr_dir / (case + '_MRI.zip')}")
    if not skip_tdc and tdc_input:
        tdc_source = str(tdc_input)
        if ingest_mode == "stage_to_scratch":
            tdc_source = "<staged_tdc_zip>"
        plan.append(
            f"Copy TDC zip to scratch backup: {tdc_source} -> {scratch / (tdc_input.name + '.bak')}"
        )
        plan.append("Extract TDC zip into temp under scratch")
        plan.append("Copy Logs/ to case Misc/Logs if present")
        plan.append("Stage TDC session in scratch/TDC_staged as directories (no zips)")
        plan.append(f"Copy staged TDC session to: {tdc_dir / '<session_name>'}")
    if pdf_input:
        plan.append(
            f"Copy treatment report to: {case_dir / (case + ' Misc') / (case + '_TreatmentReport.pdf')}"
        )
    plan.append(f"Write log file to: {log_dir / (case + '__' + run_id + '.log')}")
    plan.append(f"Write manifest to: {manifest_path}")
    if clean_scratch:
        plan.append(f"Delete scratch dir: {scratch}")
    return plan


def _build_pre_peda_plan(
    *,
    case_root: Path,
    mri_input: Path,
    tdc_input: Path,
    forbid_archives: bool,
) -> List[str]:
    plan: List[str] = []
    plan.append(f"Pre-PEDA case_root: {case_root}")
    plan.append(f"Unzip MRI -> normalize to: {case_root / 'MR DICOM'}")
    plan.append(f"Unzip TDC -> normalize to: {case_root / 'TDC Sessions'}")
    if forbid_archives:
        plan.append("Fail if any archive exists under workspace_dir")
    plan.append("Run Pre-PEDA validator and stop on success/failure")
    return plan


def _normalize_unzip_root(
    extracted_root: Path,
    target_dir: Path,
    expected_name: str,
    logger,
) -> None:
    if target_dir.exists():
        shutil.rmtree(target_dir, ignore_errors=True)
    target_dir.mkdir(parents=True, exist_ok=True)

    items = list(extracted_root.iterdir())
    match = next(
        (p for p in items if p.is_dir() and p.name.lower() == expected_name.lower()),
        None,
    )
    if match:
        shutil.rmtree(target_dir, ignore_errors=True)
        shutil.move(str(match), str(target_dir))
        items = [p for p in extracted_root.iterdir()]

    for item in items:
        dest = target_dir / item.name
        shutil.move(str(item), str(dest))

    logger.info("Normalized %s into %s", expected_name, target_dir)


def _unzip_and_normalize(
    zip_path: Path,
    case_root: Path,
    expected_name: str,
    logger,
) -> Path:
    with tempfile.TemporaryDirectory(dir=case_root, prefix=f"{expected_name}_unzipped_") as tmpdir:
        tmp = Path(tmpdir)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmp)
        target_dir = case_root / expected_name
        _normalize_unzip_root(tmp, target_dir, expected_name, logger)
        return target_dir


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
    parser.add_argument(
        "--ingest-mode",
        choices=["direct", "stage_to_scratch"],
        help="Input ingest mode (default: direct)",
    )
    parser.add_argument("--ingest-attempts", type=int, help="Staging attempts")
    add_bool_arg(parser, "ingest_verify", "Verify staged copy after each attempt")
    add_bool_arg(parser, "ingest_keep_staged", "Keep staged zips after success")
    add_bool_arg(
        parser,
        "ingest_source_stability_check",
        "Hash source twice before staging; fail if mismatch",
    )
    add_bool_arg(parser, "clean_scratch", "Delete scratch after success")
    add_bool_arg(parser, "skip_mri", "Skip MRI step")
    add_bool_arg(parser, "skip_tdc", "Skip TDC step")
    add_bool_arg(parser, "dry_run", "Only validate and log planned actions")
    add_bool_arg(parser, "hash_outputs", "Compute SHA-256 hashes for outputs")
    add_bool_arg(parser, "pre_peda_validate", "Run Pre-PEDA validator and exit")
    add_bool_arg(parser, "pre_peda_forbid_archives", "Fail if archives exist under workspace")
    add_bool_arg(parser, "tdc_allow_archives", "Allow .zip files under TDC workspace")
    args = parser.parse_args()

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
        "pre_peda_validate": args.pre_peda_validate,
        "pre_peda_forbid_archives": args.pre_peda_forbid_archives,
        "tdc_allow_archives": args.tdc_allow_archives,
        "ingest_mode": args.ingest_mode,
        "ingest_attempts": args.ingest_attempts,
        "ingest_verify": args.ingest_verify,
        "ingest_keep_staged": args.ingest_keep_staged,
        "ingest_source_stability_check": args.ingest_source_stability_check,
    }

    try:
        cfg, run_id = resolve_config(
            config_path=Path(args.config) if args.config else None,
            cli_overrides=cli_overrides,
        )
    except ValidationError as exc:
        sys.stderr.write(f"[ERROR] {exc}\n")
        return exc.code

    root: Path = cfg["root"]
    case: str = cfg["case"]
    case_dir = cfg.get("case_dir") or (root / case)
    mr_dir = cfg.get("mr_dir") or (case_dir / f"{case} MR DICOM")
    tdc_dir = cfg.get("tdc_dir") or (case_dir / f"{case} TDC Sessions")
    misc_dir = cfg.get("misc_dir") or (case_dir / f"{case} Misc")
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
    pre_peda_validate: bool = cfg["pre_peda_validate"]
    pre_peda_forbid_archives: bool = cfg["pre_peda_forbid_archives"]
    tdc_allow_archives: bool = cfg["tdc_allow_archives"]
    ingest_mode: str = cfg["ingest_mode"]
    ingest_attempts: int = cfg["ingest_attempts"]
    ingest_verify: bool = cfg["ingest_verify"]
    ingest_keep_staged: bool = cfg["ingest_keep_staged"]
    ingest_source_stability_check: bool = cfg["ingest_source_stability_check"]
    ingest_dir = scratch / "ingest"

    logger, log_file, rich_available = init_logger(
        case=case, run_id=run_id, log_dir=log_dir, log_level=log_level
    )
    logger.info("Run start case=%s run_id=%s", case, run_id)
    logger.info("Dry run: %s", dry_run)
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

    manifest_dir = cfg.get("manifest_dir") or (case_dir / "run_manifests")
    manifest_name = cfg.get("manifest_name") or f"{case}__{run_id}__manifest.json"
    manifest_path = Path(manifest_dir) / manifest_name
    step_results: Dict[str, Any] = {}
    artifacts: Dict[str, Any] = {"inputs": {}, "outputs": {}}
    pre_peda_case_root = scratch / "pre_peda_case"
    if pre_peda_validate:
        planned_actions = _build_pre_peda_plan(
            case_root=pre_peda_case_root,
            mri_input=Path(cfg["mri_input"]) if cfg.get("mri_input") else Path(),
            tdc_input=Path(cfg["tdc_input"]) if cfg.get("tdc_input") else Path(),
            forbid_archives=pre_peda_forbid_archives,
        )
    else:
        planned_actions = _build_plan(
            case_dir=case_dir,
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
            clean_scratch=clean_scratch,
            ingest_mode=ingest_mode,
            ingest_dir=ingest_dir,
            ingest_attempts=ingest_attempts,
            ingest_verify=ingest_verify,
            ingest_keep_staged=ingest_keep_staged,
        )

    status_mgr = StatusManager() if rich_available else None
    pre_peda_result: Optional[Dict[str, Any]] = None
    pre_peda_mode = pre_peda_validate
    emit_final_status = not pre_peda_mode
    ingest_results: Dict[str, Any] = {}
    mri_input_path = Path(cfg["mri_input"]) if cfg.get("mri_input") else None
    tdc_input_path = Path(cfg["tdc_input"]) if cfg.get("tdc_input") else None
    try:
        if status_mgr:
            status_mgr.__enter__()

        with StepTimer(
            logger=logger, step_name="Controller validations", results=step_results, status_mgr=status_mgr
        ):
            expected_case_name = case
            expected_mr_name = f"{case} MR DICOM"
            expected_tdc_name = f"{case} TDC Sessions"
            expected_misc_name = f"{case} Misc"

            if not case_dir.exists() and dry_run and not pre_peda_validate:
                logger.warning(
                    "Case directory does not exist yet (dry-run): %s", case_dir
                )
            if dry_run and not pre_peda_validate:
                if not case_dir.exists():
                    logger.info(
                        "Would create output folders under case_dir=%s (%s, %s, %s)",
                        case_dir,
                        expected_misc_name,
                        expected_mr_name,
                        expected_tdc_name,
                    )
            elif not pre_peda_validate:
                case_dir.mkdir(parents=True, exist_ok=True)
                misc_dir.mkdir(parents=True, exist_ok=True)
                mr_dir.mkdir(parents=True, exist_ok=True)
                tdc_dir.mkdir(parents=True, exist_ok=True)
                (tdc_dir / "applog" / "Logs").mkdir(parents=True, exist_ok=True)

            if pre_peda_validate:
                if not cfg.get("mri_input") or not cfg.get("tdc_input"):
                    raise ValidationError("Pre-PEDA mode requires both --mri-input and --tdc-input")
                _validate_zip(Path(cfg["mri_input"]), "MRI input")
                _validate_zip(Path(cfg["tdc_input"]), "TDC input")
                artifacts["inputs"]["mri_input"] = Path(cfg["mri_input"])
                artifacts["inputs"]["tdc_input"] = Path(cfg["tdc_input"])
            else:
                if not skip_mri:
                    if not cfg["mri_input"]:
                        raise ValidationError("--mri-input required (or set skip_mri)")
                    _validate_zip(Path(cfg["mri_input"]), "MRI input")
                    artifacts["inputs"]["mri_input"] = Path(cfg["mri_input"])

                if not skip_tdc:
                    if not cfg["tdc_input"]:
                        raise ValidationError("--tdc-input required (or set skip_tdc)")
                    _validate_zip(Path(cfg["tdc_input"]), "TDC input")
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

        if ingest_mode == "stage_to_scratch" and (not dry_run or pre_peda_validate):
            from ingest import stage_input_zip

            if (pre_peda_validate or not skip_mri) and mri_input_path:
                res = stage_input_zip(
                    mri_input_path,
                    ingest_dir,
                    attempts=ingest_attempts,
                    verify=ingest_verify,
                    source_stability_check=ingest_source_stability_check,
                    logger=logger,
                )
                ingest_results["mri"] = res
                if not res.get("ok"):
                    raise ValidationError(res["errors"][-1])
                logger.info(
                    "STAGED INPUT: %s -> %s (sha256=%s)",
                    res["source_zip"],
                    res["staged_zip"],
                    res.get("dst_sha256"),
                )
                mri_input_path = Path(res["staged_zip"])

            if (pre_peda_validate or not skip_tdc) and tdc_input_path:
                res = stage_input_zip(
                    tdc_input_path,
                    ingest_dir,
                    attempts=ingest_attempts,
                    verify=ingest_verify,
                    source_stability_check=ingest_source_stability_check,
                    logger=logger,
                )
                ingest_results["tdc"] = res
                if not res.get("ok"):
                    raise ValidationError(res["errors"][-1])
                logger.info(
                    "STAGED INPUT: %s -> %s (sha256=%s)",
                    res["source_zip"],
                    res["staged_zip"],
                    res.get("dst_sha256"),
                )
                tdc_input_path = Path(res["staged_zip"])

        if pre_peda_validate:
            if dry_run:
                logger.warning("Pre-PEDA validate ignores dry_run; inputs will be unzipped.")

            with StepTimer(
                logger=logger, step_name="Pre-PEDA unzip/normalize", results=step_results, status_mgr=status_mgr
            ):
                if pre_peda_case_root.exists():
                    shutil.rmtree(pre_peda_case_root, ignore_errors=True)
                pre_peda_case_root.mkdir(parents=True, exist_ok=True)

                _unzip_and_normalize(
                    mri_input_path,
                    pre_peda_case_root,
                    "MR DICOM",
                    logger,
                )
                _unzip_and_normalize(
                    tdc_input_path,
                    pre_peda_case_root,
                    "TDC Sessions",
                    logger,
                )

            with StepTimer(
                logger=logger, step_name="Pre-PEDA validate", results=step_results, status_mgr=status_mgr
            ):
                from pre_peda_validator import validate_pre_peda

                pre_peda_result = validate_pre_peda(
                    pre_peda_case_root,
                    forbid_archives=pre_peda_forbid_archives,
                    logger=logger,
                )
                if pre_peda_result["pre_peda_ready"]:
                    logger.info("pre_peda_ready=true")
                    logger.info("Pre-PEDA READY: %s", pre_peda_result.get("workspace_dir"))
                    status = "SUCCESS"
                    return_code = 0
                else:
                    status = "FAILED"
                    return_code = ValidationError.code
                sys.stdout.write("pre_peda_ready=true\n" if pre_peda_result["pre_peda_ready"] else "pre_peda_ready=false\n")
                return return_code

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
                        input_zip=mri_input_path,
                        scratch=scratch,
                        logger=logger,
                        dry_run=False,
                    )
                    artifacts["outputs"]["mri"] = mri_artifacts
                    _assert_exists(mri_artifacts["final_zip"], "MRI final zip")
            else:
                step_results["MRI"] = {"status": "SKIP", "duration_s": 0.0, "error": "skip_mri"}

            if not skip_tdc:
                with StepTimer(
                    logger=logger, step_name="TDC", results=step_results, status_mgr=status_mgr
                ):
                    tdc_artifacts = TDC_proc.run(
                        root=root,
                        case=case,
                        input_zip=tdc_input_path,
                        scratch=scratch,
                        date_shift_days=date_shift_days,
                        allow_archives=tdc_allow_archives,
                        logger=logger,
                        dry_run=False,
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
            target_pdf = misc_dir / f"{case}_TreatmentReport.pdf"
            if dry_run:
                if pdf_input:
                    logger.info("Dry-run: would copy report %s -> %s", pdf_input, target_pdf)
                else:
                    logger.info("Dry-run: no treatment report configured.")
            else:
                if pdf_input and pdf_input.exists():
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
                import structure_guard as sg

                pdf_expected = misc_dir / f"{case}_TreatmentReport.pdf"
                pdf_candidates = []
                if not pdf_expected.exists():
                    pdf_candidates = [
                        p for p in case_dir.rglob("*.pdf") if p != pdf_expected
                    ]
                    if not pdf_candidates:
                        logger.warning(
                            "Treatment report missing; expected %s",
                            pdf_expected,
                        )

                errs = sg.verify(case_dir, case, allow_missing_pdf=True)
                force_fix = bool(pdf_candidates)
                if errs or force_fix:
                    logger.info("structure_guard detected layout issues.")
                    for e in errs:
                        logger.info(" - %s", e)
                    changes = sg.fix(case_dir, case)
                    if changes:
                        for c in changes:
                            logger.info(" - %s", c)
                    errs2 = sg.verify(case_dir, case, allow_missing_pdf=True)
                    if errs2:
                        raise ProcessingError(f"structure_guard failed: {errs2}")

        with StepTimer(
            logger=logger, step_name="Finalization", results=step_results, status_mgr=status_mgr
        ):
            if clean_scratch and not dry_run:
                if ingest_mode == "stage_to_scratch" and ingest_keep_staged:
                    logger.warning(
                        "clean_scratch requested but ingest_keep_staged=true; skipping scratch cleanup."
                    )
                else:
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

        if (
            status == "SUCCESS"
            and ingest_mode == "stage_to_scratch"
            and not ingest_keep_staged
            and ingest_results
        ):
            _cleanup_ingest_dir(ingest_dir, logger)
            logger.info("Staged inputs removed: %s", ingest_dir)
        elif ingest_mode == "stage_to_scratch" and ingest_keep_staged and ingest_results:
            logger.info("Keeping staged inputs: %s", ingest_dir)

        manifest_payload = {
            "run_id": run_id,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "case": case,
            "status": status,
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
        if ingest_mode == "stage_to_scratch" and ingest_results:
            manifest_payload["inputs_staged"] = {}
            if ingest_results.get("mri"):
                m = ingest_results["mri"]
                manifest_payload["inputs_staged"]["mri"] = {
                    "source_path": m.get("source_zip"),
                    "staged_path": m.get("staged_zip"),
                    "sha256": m.get("dst_sha256"),
                    "source_sha256": m.get("src_sha256"),
                }
            if ingest_results.get("tdc"):
                t = ingest_results["tdc"]
                manifest_payload["inputs_staged"]["tdc"] = {
                    "source_path": t.get("source_zip"),
                    "staged_path": t.get("staged_zip"),
                    "sha256": t.get("dst_sha256"),
                    "source_sha256": t.get("src_sha256"),
                }
        if pre_peda_result is not None:
            manifest_payload["pre_peda"] = pre_peda_result

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
            compute_hash = hash_outputs
            if ingest_mode == "stage_to_scratch" and label in ("mri_input", "tdc_input"):
                compute_hash = False
            manifest_payload["inputs"][label] = file_metadata(
                Path(path), compute_hash=compute_hash
            )

        pdf_output = misc_dir / f"{case}_TreatmentReport.pdf"
        if pdf_output.exists():
            manifest_payload["outputs"]["treatment_report"] = file_metadata(
                pdf_output, compute_hash=hash_outputs
            )

        if "mri" in artifacts.get("outputs", {}):
            mri_out = artifacts["outputs"]["mri"]
            manifest_payload["outputs"]["mri_final_zip"] = file_metadata(
                mri_out["final_zip"], compute_hash=hash_outputs
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

        logger.info("Run complete: %s", status)
        logger.info("Artifacts:")
        if not skip_mri:
            logger.info(
                " - MRI final zip: %s",
                artifacts.get("outputs", {}).get("mri", {}).get("final_zip", "n/a"),
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

        if status == "SUCCESS" and not pre_peda_mode:
            logger.info("Canonical schema tree:")
            schema_lines = [
                str(case_dir),
                str(misc_dir),
                str(misc_dir / f"{case}_TreatmentReport.pdf"),
                str(mr_dir),
                str(mr_dir / f"{case}_MRI.zip"),
                f"{case_dir / f'{case} PEDAv9.1.3-Data.zip'} (placeholder)",
                str(tdc_dir),
                str(tdc_dir / "applog" / "Logs"),
                str(tdc_dir / "Raw"),
            ]
            for line in schema_lines:
                logger.info(line)

        if emit_final_status:
            sys.stdout.write(f"{status} case={case} run_id={run_id}\n")

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
