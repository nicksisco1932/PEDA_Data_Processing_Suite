# 📂 PEDA Data Proc — WORK

End-to-end pipeline for **TDC session triage**, **MRI package anonymization**, **PDF normalization**, and **PEDA archiving**.

---

## 🧩 Components

### 1) `clean_tdc_data.py` — *v0.49*
Unzips and organizes TDC session archives. Moves nested `Logs` contents into  
`<case> TDC Sessions\applog\Logs`, prefixing each with the case ID.

```powershell
python clean_tdc_data.py "D:\cases\TDC_017-01-474.zip" `
  --dest-root "D:\Data_Clean" --init-skeleton
```

---

### 2) `process_mri_package.py` — *v2.1*
Unzips MRI package → anonymizes DICOM headers → re-zips with strict canonical naming.

```powershell
python process_mri_package.py --input "D:\...\mri-017-01_474.zip" `
  --birthdate 19600101 --out-root "D:\Data_Clean" --apply --backup --csv-audit
```

**Creates:**  
`D:\Data_Clean\017_01-474\017_01-474 MR DICOM\017_01-474_MRI.zip`

---

### 3) `master_run.py` — *v2.3.0*
Orchestrates all stages in sequence:

1. **TDC cleanup** (`clean_tdc_data.py`)  
2. **MRI anonymization/packaging** (`process_mri_package.py`)  
3. **PDF normalization** — finds, fixes `.pdf.pdf`, and moves report to `<CASEID> Misc/<CASEID>_TreatmentReport.pdf`  
4. **SQLite anonymization** (`localdb_anon.py`)  
5. **PEDA run or simulation** (`run_peda`)  
6. **PEDA archive** (`_archive_pedav_dir`)

```powershell
python master_run.py "D:\cases\TDC_017-01-474.zip" `
  --mri-input "D:\cases\MRI-017-01_474.zip" `
  --patient-birthdate 19000101 `
  --mri-apply `
  --out-root "D:\Data_Clean" `
  --simulate-peda `
  --peda-path "D:\PEDAv9.1.3"
```

---

### 🧾 PDF Normalization Step

Automatically detects and fixes PDF reports such as `TreatmentReport.pdf` or `017_01-474_TreatmentReport.pdf.pdf`.

**Behavior:**
- Scans all subfolders for `.pdf` files  
- Corrects duplicated extensions (`.pdf.pdf → .pdf`) and case (`.PDF → .pdf`)  
- Picks the best candidate using heuristics:
  - Case ID present in filename (+3)
  - “treatment/report/summary” keywords (+2)
  - Shallower directory depth (+1)
  - Newest modification date as tiebreaker  
- Moves to:  
  ```
  <CASEID> Misc\<CASEID>_TreatmentReport.pdf
  ```
- Automatically versions if duplicates exist (`_2`, `_3`, …)
- Optional skip or rename:
  ```powershell
  --skip-pdf
  --pdf-dest-name "ProcedureSummary"
  ```

---

## 🗂 Directory Layout (after full run)

```
<Data_Clean>\
  └─ 017_01-479\
      ├─ 017_01-479 Misc\
      │    └─ 017_01-479_TreatmentReport.pdf
      │
      ├─ 017_01-479 MR DICOM\
      │    ├─ DICOM\
      │    └─ 017_01-479_MRI.zip
      │
      ├─ 017_01-479 TDC Sessions\
      │    ├─ _2025-08-12--12-02-57 1255595998\
      │    │    └─ local.db
      │    └─ applog\Logs\
      │
      ├─ PEDAv9.1.3-Data.zip
      └─ applog\
          └─ master_run.log
```

---

## ⚙️ Quick Test Example

Validated full-pipeline command:

```powershell
python master_run.py "C:\Users\nicks\Desktop\WORK_Delete_boring\Database_project\test_data\test_data\017-01_479_TDC.zip" `
  --mri-input "C:\Users\nicks\Desktop\WORK_Delete_boring\Database_project\test_data\test_data\MRI-017-01_479.zip" `
  --patient-birthdate 19000101 `
  --mri-apply `
  --out-root "C:\Users\nicks\Desktop\WORK_Delete_boring\Data_Clean" `
  --simulate-peda `
  --peda-path "C:\Users\nicks\Desktop\WORK_Delete_boring\PEDAv9.1.3"
```

Expected log sequence:

```
==== MASTER START ====
TDC: Unpacking ...
MRI: Anonymizing ...
PDF: Found TreatmentReport.pdf.pdf → normalized → moved
ANON: local.db anonymized
PEDA: Simulated run complete
ARCHIVE: PEDAv9.1.3 → 017_01-479 PEDAv9.1.3-Data.zip
==== MASTER COMPLETE ====
```

---

## 🧠 Notes
- Works on **Python 3.9+**
- Uses only `pydicom` (no DCMTK required)
- Optional: **7-Zip** in `PATH` for large archives

---

## 🧾 Version Summary

| Script | Version | Description |
|--------|----------|-------------|
| `clean_tdc_data.py` | v0.49 | Unpacks and structures TDC zips |
| `process_mri_package.py` | v2.1 | DICOM anonymization and packaging |
| `master_run.py` | **v2.3.0** | Full orchestrator incl. PDF normalization, anonymization, and archiving |

---

## 🗒️ CHANGELOG

### v2.3.0 (2025-10-07)
- Added integrated **PDF normalization** inside `master_run.py`
- Detects `.pdf.pdf` and normalizes filenames
- Moves report PDFs to `<CASEID> Misc/<CASEID>_TreatmentReport.pdf`
- Added flags `--skip-pdf` and `--pdf-dest-name`
- Updated README, test command, and log behavior

### v2.2.0 (previous)
- Added anonymization step using `localdb_anon.py`
- Enhanced logging and cleanup options

### v2.1.x and earlier
- Core pipeline: TDC + MRI anonymization and packaging
