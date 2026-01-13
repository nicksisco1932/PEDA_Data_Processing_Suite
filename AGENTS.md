# AGENTS.md

This repository has a canonical case directory layout that all code and tools
must validate against and preserve. Do not invent new top-level folders under a
case directory (except scratch, run_manifests, and the configured Logs output).

Canonical case directory layout (example)
Root: D:\Data_Clean
Case ID format: NNN_NN-NNN (e.g., 017_01-474)

D:\Data_Clean\017_01-474
|
+-- Misc
|   +-- 017_01-474_TreatmentReport.pdf
|   +-- Logs\
|       +-- 017_01-474 Tdc.2025_11_05.log
|
+-- MR DICOM
|   +-- 017_01-474_MRI.zip
|
+-- 017_01-474 PEDAv9.1.3-Data.zip (placeholder)
|
+-- run_logs
|   +-- 017_01-474__20260113_123000.log
|   +-- 017_01-474__20260113_123000__manifest.json
|
+-- annon_logs
|   +-- localdb_check_pre.json
|   +-- localdb_check_post.json
|   +-- PEDA_run_log.txt
|
+-- TDC Sessions
    +-- Raw\2025-09-29\

Requirements
1) Layout derivation:
   - case_dir = <root>\<case_id>
   - misc_dir = <case_dir>\Misc
   - mr_dir   = <case_dir>\MR DICOM
   - tdc_dir  = <case_dir>\TDC Sessions

2) Output targets:
   - MRI final zip MUST land at:
     <mr_dir>\<case_id>_MRI.zip
   - TDC output MUST land under:
     <tdc_dir>\<session_name>\...
     and MUST preserve or produce:
       - Raw\YYYY-MM-DD\... (or whatever the session content dictates)
   - TDC log MUST land at:
     <case_dir>\Misc\Logs\<case_id> Tdc.<YYYY_MM_DD>.log
   - Run log + manifest MUST land at:
     <case_dir>\run_logs\<case_id>__<run_id>.log
     <case_dir>\run_logs\<case_id>__<run_id>__manifest.json

3) No new top-level folders under case_dir except:
   - scratch (transient)
   - run_manifests
   - run_logs
   - annon_logs

4) Auto-discovery (if used) should search:
   - <case_dir>\incoming\
   - <case_dir>\
   - <root>\incoming\<case_id>\
   Output locations are fixed to the canonical layout above.

5) MRI archive naming synonyms:
   - Treat "MR" and "MRI" as equivalent labels for MRI archives.
   - Auto-discovery must include patterns covering both MR and MRI, e.g.:
     "*MRI*.zip", "*MR*.zip", "MR_*.zip", "MRI_*.zip".
   - If multiple MRI candidates exist, resolve via the configured tie-break
     (newest/largest/first) without penalizing MR vs MRI.
   - Case-id aliases must match filenames containing:
     "NNN_NN-NNN", "NNN-NN-NNN", "NNN_NN_NNN", "NNN-NN_NNN" (case-insensitive).
   - When multiple MRI candidates are found, log the top candidates and the
     tie-break rule used.

6) Validation messages:
   - Always name the expected folder and show the computed full path.

Notes
- The pipeline must tolerate existing applog content in TDC Sessions and remove it during cleanup.
- On rerun, avoid overwriting existing session output; use suffixing behavior.
- Preserve Windows path compatibility and prefer pathlib.

### Canonical Case Layout (Post-Staging)

After pipeline staging and normalization, agents may assume the following:

<CaseID>\
|-- Misc\
|   |-- <Treatment Report PDF>
|   |-- Logs\
|       |-- <CaseID> Tdc.<YYYY_MM_DD>.log
|
|-- MR DICOM\
|   |-- <CaseID>_MRI.zip
|
|-- run_logs\
|   |-- <CaseID>__<RunID>.log
|   |-- <CaseID>__<RunID>__manifest.json
|
|-- annon_logs\
|   |-- localdb_check_pre.json
|   |-- localdb_check_post.json
|   |-- PEDA_run_log.txt
|
|-- TDC Sessions\
    |-- <Session_Name>\
        |-- Raw\

Notes:
- All TDC log files (.log or .txt) are consolidated into `Misc\Logs\` as `<CaseID> Tdc.<YYYY_MM_DD>.log`.
- Run logs and manifests live under `run_logs\`.
- Anonymization/check artifacts live under `annon_logs\`.
- Agents must not expect any `applog\Logs` directories under `TDC Sessions`.

Invariant:
Agents must treat `Misc\Logs\` as the sole authoritative location for TDC logs.
