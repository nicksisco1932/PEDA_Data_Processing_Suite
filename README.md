# PEDA Data Processing Suite

![CI](https://github.com/nicksisco1932/PEDA_Data_Processing_Suite/blob/main/.github/workflows/ci.yml/badge.svg)

End-to-end automation for TDC session triage, MRI package normalization, PDF cleanup, and SQLite anonymization.
The pipeline is structure-enforced and designed to produce consistent, downstream-ready outputs.

This repository consolidates preprocessing steps required to generate standardized, downstream-ready outputs for R&D, QC, and clinical engineering work.

---

## Quick Start

Example run (PowerShell):
```
python .\src\controller.py --config .\config\pipeline.yaml
```

If you use the PowerShell helper that prompts for inputs:
```
.\tools\run_case.ps1 -CaseDir "D:\Data_Clean\017_01-474"
```

Notes:
- Quoted Windows paths (Copy as path) are accepted in YAML and CLI.
- 7-Zip is optional but preferred; set SEVEN_ZIP or install to C:\Program Files\7-Zip\7z.exe.

---

## Tests

Generate tiny fixtures and run schema tests:
```
python tools/generate_fixtures.py
pytest -q
```

Run the built-in self-test (no pytest required):
```
python src/controller.py --self-test
```

Self-test notes:
- Uses deterministic input name permutations under tests/_tmp/TEST_MODE/perm_XX/.
- Runs half permutations via CLI args and half via a temp YAML config.
- Includes quoted Windows "Copy as path" strings.
- Asserts outputs land only in <case_num> MR DICOM, <case_num> TDC Sessions, <case_num> Misc.
- Writes artifacts to logs/ (including TEST_CASE__TEST_MODE.log, RUN_TEST_MODE.log, RUN_TEST_MODE_manifest.json).

---

## Incoming Data Expectations

Canonical case directory layout (example):
```
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
    +-- _2025-11-05--07-05-25 122867438\
    +-- applog\Logs\
    |   +-- 017-01-474_log.txt
    +-- Raw\2025-09-29\

```

Incoming discovery (if used) checks:
- <case_dir>\incoming\
- <case_dir>\
- <root>\incoming\<case_id>\

MRI archive naming rules:
- Treat "MR" and "MRI" as equivalent labels.
- Patterns include *MRI*.zip, *MR*.zip, MR_*.zip, MRI_*.zip.
- Case-id aliases in filenames: NNN_NN-NNN, NNN-NN-NNN, NNN_NN_NNN, NNN-NN_NNN (case-insensitive).

---

## Output Layout (Post-Run)

Required output targets:
- MRI zip must land at <case_dir>\MR DICOM\<case_id>_MRI.zip.
- TDC output must land under <case_dir>\TDC Sessions\<session_name>\... and preserve:
  - applog\Logs\... (if present or created)
  - Raw\YYYY-MM-DD\... (or session content dictates)

No new top-level folders under <case_dir> except:
- scratch (transient)
- run_manifests
- configured Logs output directory

---

## Components

### 1) src/controller.py
CLI entrypoint that orchestrates the pipeline and writes logs/manifests.

Pipeline steps (when enabled):
1. TDC archive staging and normalization (`src/TDC_proc.py`)
2. MRI archive normalization (`src/MRI_proc.py`)
3. Treatment report placement (PDF)
4. SQLite anonymization (`src/localdb_anon.py`)
5. Structure guard (`src/structure_guard.py`)

---

### 2) src/TDC_proc.py
Normalizes and stages TDC session archives into the canonical `TDC Sessions` layout.

Highlights:
- Preserves or creates `applog\Logs` and `Raw\YYYY-MM-DD` content
- Stages into scratch, then copies into `TDC Sessions\<session_name>`

---

### 3) src/MRI_proc.py
Normalizes MRI archives into a deterministic zip.

Behavior:
- Normalizes MRI packaging into `MR DICOM\<CASEID>_MRI.zip`
- Removes empty `DICOM` folders

---

### 4) src/localdb_anon.py
Deterministic, in-place anonymization for `local.db`.

---

### 5) src/structure_guard.py
Verifies and repairs post-run layout.

Enforces:
- Normal treatment report locations
- No stray `DICOM` folders
- Merged logs
- Removal of empty directories
- Safe re-runs until canonical

---
