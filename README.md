# 📂 PEDA Data Processing Suite

A modular Python toolkit for **TULSA PEDA case data processing**.
This suite handles **unzipping, organizing, anonymizing MRI data, orchestrating pipelines, and invoking PEDA** (simulated or MATLAB), plus an **archive-only** mode to package existing PEDA output.

---

## 📌 Components

### 1) `clean_tdc_data.py`

Unpacks and organizes TDC case `.zip` archives into the canonical directory layout.

**Highlights**

* Python unzip with optional 7‑Zip fallback for huge archives.
* Fixes accidental `Logs/Logs` nesting; prefixes moved log files with case ID.
* Final layout roots:

  ```
  <norm_id> Misc
  <norm_id> MR DICOM
  <norm_id> TDC Sessions
  ```
* Removes staging folders after sort.

**Typical usage (direct)**

```powershell
# Create/target canonical case dir and feed a TDC zip
python clean_tdc_data.py "D:\Data_Clean\017_01-479" `
  --norm-id "017_01-479" `
  --input  "D:\Database_project\test_data\test_data\017-01_479_TDC.zip" `
  --log-root "D:\Data_Clean\017_01-479\applog\Logs"
```

---

### 2) `process_mri_package.py`

Processes MRI `.zip` or an MR DICOM directory:

* Stages input
* Calls anonymizer (`anonymize_dicom.py`)
* Normalizes to `<norm_id> MR DICOM\<norm_id>_MRI.zip`
* Logs under `<logs_root>\Logs`

**Dry‑run (no writes)**

```powershell
python process_mri_package.py `
  --input "D:\Database_project\test_data\test_data\MRI-017-01_479.zip" `
  --birthdate 19000101 `
  --out-root "D:\Data_Clean" `
  --logs-root "D:\Data_Clean\017_01-479\applog\Logs"
```

**Apply changes (write anonymized tags)**

```powershell
python process_mri_package.py `
  --input "D:\Database_project\test_data\test_data\MRI-017-01_479.zip" `
  --birthdate 19000101 `
  --apply `
  --out-root "D:\Data_Clean" `
  --logs-root "D:\Data_Clean\017_01-479\applog\Logs"
```

---

### 3) `anonymize_dicom.py`

Robust DICOM metadata anonymizer built on `pydicom`.

**Features**

* **Dry‑run by default**; pass `--apply` to commit changes.
* JSONL audit (and optional CSV) per file, with atomic writes and optional `.bak` backups.
* Minimal default Tag Plan + optional extended fields via `--write-extras` or `--plan-json`.

**Usage**

```powershell
# Direct folder; logs root can be the case dir (tool appends /Logs if needed)
python anonymize_dicom.py `
  --site-dir "D:\Data_Clean\017_01-479\017_01-479 MR DICOM" `
  --birthdate 19000101 `
  --apply `
  --logs-root "D:\Data_Clean\017_01-479"
```

---

### 4) `run_peda.py`

Runs or simulates **PEDA**.

**Modes**

* **Real**: call MATLAB (e.g., `MAIN_PEDA`), writing under the case directory.
* **Simulated**: create placeholder artifacts and logs when MATLAB is missing.

**Usage**

```powershell
# Simulate a PEDA run for a case directory
python run_peda.py "D:\Data_Clean\017_01-479" --simulate
```

---

### 5) `master_run.py`

The **pipeline orchestrator** (merged). Can run full pipeline or **archive-only**.

**Pipeline (default)**

1. `clean_tdc_data.py`
2. `process_mri_package.py`
3. `run_peda.py` (or `--simulate-peda`)
4. Archive newest PEDAv* folder → `<norm_id> PEDAv*-Data.zip` (unless `--no-archive`)

**Archive‑only**

* Skip all stages and simply package an existing PEDA folder.
* Accepts explicit `--peda-path` (e.g., `D:\PEDAv9.1.3`).
* **Never** archives a folder ending with `-Video`.

**Full pipeline (apply anonymization; simulate PEDA; archive explicit PEDA)**

```powershell
python master_run.py "D:\Database_project\test_data\test_data\017-01_479_TDC.zip" `
  --mri-input "D:\Database_project\test_data\test_data\MRI-017-01_479.zip" `
  --patient-birthdate 19000101 `
  --mri-apply `
  --out-root "D:\Data_Clean" `
  --simulate-peda `
  --peda-path "D:\PEDAv9.1.3"
```

**Archive‑only (package an existing PEDAv folder into a case)**

```powershell
python master_run.py "D:\Data_Clean\017_01-479" `
  --out-root "D:\Data_Clean" `
  --archive-only `
  --peda-path "D:\PEDAv9.1.3"
```

**Skip sub‑stages on demand**

```powershell
# Example: Run MRI only (assumes case dir exists), then package a PEDA folder at the end
python master_run.py "D:\Data_Clean\017_01-479" `
  --skip-tdc `
  --mri-input "D:\Database_project\test_data\test_data\MRI-017-01_479.zip" `
  --patient-birthdate 19000101 --mri-apply `
  --out-root "D:\Data_Clean" `
  --simulate-peda `
  --peda-path "D:\PEDAv9.1.3"
```

> **Note:** The orchestrator normalizes log paths to avoid `Logs/Logs`, and it auto‑rehomes swapped MRI output IDs (e.g., `017-01_XXX` → `017_01-XXX`).

---

## 🚀 Quick Start Recipes

**A) End‑to‑end with real anonymization (no MATLAB), then archive existing PEDA folder**

```powershell
python master_run.py "D:\Database_project\test_data\test_data\017-01_479_TDC.zip" `
  --mri-input "D:\Database_project\test_data\test_data\MRI-017-01_479.zip" `
  --patient-birthdate 19000101 --mri-apply `
  --out-root "D:\Data_Clean" `
  --simulate-peda `
  --peda-path "D:\PEDAv9.1.3"
```

**B) Archive only (zip PEDAv into the case)**

```powershell
python master_run.py "D:\Data_Clean\017_01-479" `
  --out-root "D:\Data_Clean" `
  --archive-only `
  --peda-path "D:\PEDAv9.1.3"
```

**C) Run modules separately**

```powershell
# 1) TDC
python clean_tdc_data.py "D:\Data_Clean\017_01-479" `
  --norm-id "017_01-479" `
  --input "D:\Database_project\test_data\test_data\017-01_479_TDC.zip" `
  --log-root "D:\Data_Clean\017_01-479\applog\Logs"

# 2) MRI (apply)
python process_mri_package.py `
  --input "D:\Database_project\test_data\test_data\MRI-017-01_479.zip" `
  --birthdate 19000101 --apply `
  --out-root "D:\Data_Clean" `
  --logs-root "D:\Data_Clean\017_01-479\applog\Logs"

# 3) Package PEDA
python master_run.py "D:\Data_Clean\017_01-479" `
  --out-root "D:\Data_Clean" `
  --archive-only `
  --peda-path "D:\PEDAv9.1.3"
```

---

## 📂 Example Output Layout (current)

```
D:\Data_Clean\017_01-474\
│
├── 017_01-474 Misc\
├── 017_01-474 MR DICOM\
│   └── 017_01-474_MRI.zip
├── 017_01-474 PEDAv9.1.3-Data.zip
└── 017_01-474 TDC Sessions\
    ├── applog\Logs\
    │   └── 017-01-474_log.txt
    └── Raw\2025-09-29\
```

*(No `*-Video` folder is created or archived by the orchestrator.)*

---

## ⚙️ Installation

```powershell
# Clone and set up venv
git clone https://github.com/<your-repo>/peda-data-proc.git
cd peda-data-proc
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
```

* If using **7‑Zip**, ensure `7z.exe` is on PATH (or configure your scripts accordingly).
* For **real PEDA** runs, MATLAB must be installed and your `MAIN_PEDA` entrypoint available.

---

## 🧰 CLI Reference (highlights)

**`master_run.py` (merged)**

| Argument              | Description                                    |
| --------------------- | ---------------------------------------------- |
| `case_dir`            | Case dir or a related file (e.g., `*_TDC.zip`) |
| `--out-root`          | Root where canonical `<norm_id>` folder lives  |
| `--tdc-input`         | Explicit TDC zip (optional)                    |
| `--mri-input`         | MRI zip                                        |
| `--mri-dir`           | MR DICOM directory                             |
| `--patient-birthdate` | DOB in `YYYYMMDD`                              |
| `--mri-apply`         | Apply anonymization writes                     |
| `--simulate-peda`     | Simulate PEDA instead of calling MATLAB        |
| `--skip-tdc/mri/peda` | Skip selected stages                           |
| `--archive-only`      | Only package a PEDA folder (no stages run)     |
| `--peda-path`         | Explicit PEDA folder to zip                    |
| `--peda-name`         | Override label in zip filename                 |
| `--no-archive`        | Disable end-of-run packaging                   |
| `--clean-peda`        | Delete PEDA source folder after archiving      |
| `--log-root`          | Log root (tool appends `\Logs` if needed)      |
| `--allow-id-mismatch` | Continue even if name patterns differ          |

**Modules**

* `clean_tdc_data.py` → `case_dir --norm-id --input --log-root`
* `process_mri_package.py` → `--input --birthdate [--apply] --out-root --logs-root`
* `anonymize_dicom.py` → `--site-dir|--site-id --birthdate [--apply] --logs-root [--csv-audit] [--plan-json]`
* `run_peda.py` → `case_dir [--simulate] [--force-matlab]`

---

## 📝 Notes

* Use PowerShell backticks `` ` `` for line continuations (as shown). In CMD, use `^`; in bash, use `\`.
* Paths are shown for Windows; adapt separators for Linux/Mac.
* Logs are centralized under `<case>\applog\Logs`.
* The orchestrator prevents `Logs/Logs` duplication and auto‑corrects swapped‑ID MRI output trees.
