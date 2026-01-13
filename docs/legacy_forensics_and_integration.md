# Legacy Forensics and Integration

## Current State
- Entrypoint: `src/controller.py` orchestrates validation, MRI/TDC processing, treatment report, layout guard, and finalization.
- MRI staging: `src/MRI_proc.py` copies the input zip to scratch with SHA-256 verification, then extracts into `<case>\MR DICOM\<zip_stem>`.
- TDC staging: `src/TDC_proc.py` copies the input zip to scratch, extracts into temp, stages a session in scratch, anonymizes `local.db`, and copies to `<case>\TDC Sessions\<session>`.
- Logs: controller log + manifest are written to `<case>\run_logs`. TDC logs are selected from the extracted TDC root and consolidated into `<case>\Misc\Logs\<case_id> Tdc.<YYYY_MM_DD>.log`, and anonymization/check artifacts live in `<case>\annon_logs`. `assert_no_forbidden_log_dirs` forbids `Logs__*` and any `TDC Sessions\**\applog`.
- Zip handling: `src/archive_utils.py` provides deterministic extraction with optional 7-Zip; used by MRI/TDC steps and the new unzip staging utility.
- DICOM anonymization: a rule map exists in `src/phi/dicom_rules.py` with a stub step (`src/pipeline_steps/dicom_anon_stub.py`); no file rewriting yet.
- local.db: anonymization occurs in `src/localdb_anon.py` inside `TDC_proc`. The check+anonymize step in `src/pipeline_steps/localdb_step.py` is wired after TDC staging and reports into `<case>\annon_logs`.
- Cleanup/pruning: `structure_guard` removes empty `MR DICOM\DICOM` and merges Logs. `cleanup_artifacts` optionally removes known junk files under scratch only.

## Legacy Ideas Worth Keeping
- Deterministic unzip of archives into same-named folders for traceable staging.
- Explicit DICOM tag rewrite policy map (clear, testable anonymization intent).
- Targeted cleanup of known junk artifacts after staging (e.g., `.mat`, sqlite sidecars).

## Legacy Hazards to Avoid
- Interactive prompts (`Read-Host`) and manual entry of PHI values.
- `Set-Location` side effects and hard-coded absolute paths.
- Suppressing stderr/stdout for external tools (loss of auditability).
- Destructive deletes without guardrails or root checks.
- Variable name mismatches and unvalidated inputs.

## Integration Plan
- Add a deterministic unzip utility to expand input zips into a scratch subfolder before downstream steps.
- Introduce a DICOM anonymization rules module and a stub step (no-op) to reserve the pipeline hook.
- Wire the existing local.db check/anonymization step after TDC staging, writing reports to `<case>\annon_logs`.
- Add a guarded cleanup step that only deletes known artifacts inside scratch/working directories.
- Keep PEDA disabled; include a stub hook for future integration only.

## Config/CLI Knobs
- `pipeline.unzip_inputs` (bool): enable deterministic unzip into scratch.
- `pipeline.cleanup.enabled` (bool), `pipeline.cleanup.dry_run` (bool), `pipeline.cleanup.patterns` (list[str]).
- `pipeline.dicom_anon.enabled` (bool), `pipeline.dicom_anon.mode` ("existing" | "stub").
- `pipeline.peda.enabled` (bool): reserved; no execution in current pipeline.
