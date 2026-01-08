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
|
+-- MR DICOM
|   +-- 017_01-474_MRI.zip
|
+-- 017_01-474 PEDAv9.1.3-Data.zip (placeholder)
|
+-- TDC Sessions
    +-- applog\Logs\
    |   +-- 017-01-474_log.txt
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
       - applog\Logs\... (if present or created by upstream steps)
       - Raw\YYYY-MM-DD\... (or whatever the session content dictates)

3) No new top-level folders under case_dir except:
   - scratch (transient)
   - run_manifests
   - Logs output directory as configured

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
- The pipeline must tolerate existing applog/Raw content in TDC Sessions.
- On rerun, avoid overwriting existing session output; use suffixing behavior.
- Preserve Windows path compatibility and prefer pathlib.
