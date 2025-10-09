# 📂 PEDA Data Proc — WORK

![CI](https://github.com/nicksisco1932/PEDA_Data_Processing_Suite/tree/main/actions/workflows/ci.yml/badge.svg)



End-to-end pipeline for **TDC session triage**, **MRI package normalization**, **PDF cleanup**, and **PEDA archiving** — all fully automated and structure-enforced.

---

## 🧩 Components

### 1) `clean_tdc_data.py` — *v1.5.1*
Unpacks and organizes TDC session archives into canonical folders.

**Highlights**
- Merges stray `Logs` → `applog\Logs`
- Moves timestamped session folders (e.g., `_2025-08-12--12-02-57 1255595998`) into  
  `<CASEID> TDC Sessions\`
- Creates consistent subfolder set:
  ```
  <CASEID> TDC Sessions\
  <CASEID> Misc\
  <CASEID> MR DICOM\
  applog\Logs\
  ```

```powershell
python clean_tdc_data.py "D:\cases\017_01-479" `
  --norm-id "017_01-479" `
  --input "D:\cases\017-01_479_TDC.zip"
```

---

### 2) `process_mri_package.py` — *v1.4.0*
Handles MRI archive packaging and normalization.

**Behavior**
- Auto-detects `case_dir` and `norm-id` from `--input` (e.g., `MRI-017-01_479.zip`)
- Moves or zips MRI data into canonical form:
  ```
  <CASEID> MR DICOM\<CASEID>_MRI.zip
  ```
- Removes any empty `DICOM/` subfolder automatically
- Logs to `applog\Logs\process_mri_package.log`

```powershell
python process_mri_package.py --input "D:\cases\MRI-017-01_479.zip" `
  --out-root "D:\Data_Clean" --apply
```

---

### 3) `master_run.py` — *v2.3.2-compat+autoid*
Central orchestrator that runs all stages in sequence and self-corrects layout.

**Pipeline**
1. **TDC cleanup** → `clean_tdc_data.py`
2. **MRI packaging** → `process_mri_package.py`
3. **PDF normalization** → finds `.pdf.pdf`, fixes naming, moves to `Misc\`
4. **SQLite anonymization** → `localdb_anon.py`
5. **PEDA run/simulate** → `run_peda`
6. **PEDA archive** → `_archive_pedav_dir`
7. **Structure guard** → `structure_guard.py --fix` (auto-enforced)

**Example**
```powershell
python master_run.py "D:\cases\017-01_479_TDC.zip" `
  --mri-input "D:\cases\MRI-017-01_479.zip" `
  --patient-birthdate 19000101 `
  --mri-apply `
  --out-root "D:\Data_Clean" `
  --simulate-peda `
  --peda-path "D:\PEDAv9.1.3"
```

---

### 4) `structure_guard.py` — *v0.2*
Automatic post-run verifier and fixer called by `master_run.py`.

**Enforces**
- Canonical layout (no `DICOM/` folder)
- Moves any stray session or log folders
- Normalizes TreatmentReport name/location
- Removes empty directories and merges Logs
- Exits cleanly when structure is canonical

---

## 🗂 Canonical Directory Layout (post-run)

```
<Data_Clean>\
  └─ 017_01-479\
      ├─ 017_01-479 Misc\
      │    └─ 017_01-479_TreatmentReport.pdf
      │
      ├─ 017_01-479 MR DICOM\
      │    └─ 017_01-479_MRI.zip
      │
      ├─ 017_01-479 TDC Sessions\
      │    └─ _2025-08-12--12-02-57 1255595998\
      │         └─ local.db
      │
      ├─ PEDAv9.1.3-Data.zip
      └─ applog\
          ├─ master_run.log
          └─ Logs\<other run logs>
```

---

## ⚙️ Quick Validation Example

```powershell
python master_run.py "C:\Users\nicks\Desktop\WORK_Delete_boring\Database_project\test_data\test_data\017-01_478_TDC.zip" `
  --mri-input "C:\Users\nicks\Desktop\WORK_Delete_boring\Database_project\test_data\test_data\MRI-017-01_478.zip" `
  --patient-birthdate 19000101 `
  --mri-apply `
  --out-root "C:\Users\nicks\Desktop\WORK_Delete_boring\Data_Clean" `
  --simulate-peda `
  --peda-path "C:\Users\nicks\Desktop\WORK_Delete_boring\PEDAv9.1.3"
```

**Expected log flow**
```
==== MASTER START ====
TDC: Unpacking ...
MRI: Packaging ...
PDF: Found TreatmentReport.pdf.pdf → normalized → moved
ANON: local.db anonymized
PEDA: Simulated run complete
ARCHIVE: PEDAv9.1.3 → 017_01-479 PEDAv9.1.3-Data.zip
GUARD: Final layout is canonical.
==== MASTER COMPLETE ====
```

---

## 🧠 Notes
- Requires **Python 3.9 or higher**
- Pure-Python: no DCMTK or MATLAB dependencies
- Optional: **7-Zip** in PATH for large archive handling
- All logs are stored under `applog\Logs\`
- Idempotent — safe to re-run on already-processed cases

---

## 🧾 Version Summary

| Script | Version | Description |
|---------|----------|-------------|
| `clean_tdc_data.py` | v1.5.1 | Normalizes TDC sessions and merges logs |
| `process_mri_package.py` | v1.4.0 | Auto-detects and packages MRI zip |
| `structure_guard.py` | v0.2 | Post-run verifier and fixer |
| `master_run.py` | **v2.3.2** | Full orchestrator with auto-ID and guard enforcement |

---

## 🗒️ CHANGELOG

### v2.3.2 (2025-10-07)
- Added **auto-ID** support in `master_run.py`
- Integrated **structure_guard** post-step (automatic layout enforcement)
- Canonical layout simplified (no `DICOM/` folder)
- Full cross-platform venv bootstrap
- Improved log consistency

### v1.4.0 (2025-10-07)
- `process_mri_package.py` auto-detects case ID and directory from inputs  
- Removes empty DICOM folders automatically

### v1.5.1 (2025-10-07)
- `clean_tdc_data.py` merges Logs, moves stray session dirs, guarantees canonical folders

