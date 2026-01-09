# 📂 PEDA Data Proc — WORK  

![CI](https://github.com/nicksisco1932/PEDA_Data_Processing_Suite/blob/main/.github/workflows/ci.yml/badge.svg)

End-to-end automation for **TDC session triage**, **MRI package normalization**, **PDF cleanup**, **SQLite anonymization**, and **PEDA reconstruction**.  
The pipeline is fully structure-enforced, versioned, and now includes a **Python-native PEDA engine (v0.9)** aligned with the legacy MATLAB v9.1.3 workflow.

This repository consolidates all preprocessing and PEDA steps required to generate standardized, downstream-ready thermal and controller outputs for R&D, QC, and clinical engineering work.

---

## Fast Dev Loop

Generate tiny fixtures and run schema tests:
```
python tools/generate_fixtures.py
pytest -q
```

Notes:
- Quoted Windows paths (Copy as path) are accepted in YAML and CLI.
- 7-Zip is optional but preferred; set `SEVEN_ZIP` or install to `C:\Program Files\7-Zip\7z.exe`.

---

## 🧩 Components

### **1) `clean_tdc_data.py` — v1.5.1**  
Normalizes and unpacks TDC case archives into a canonical filesystem.

**Highlights**
- Merges stray `Logs` → `applog/Logs`  
- Moves timestamped session folders into `TDC Sessions/`  
- Guarantees stable subfolders:
  ```
  TDC Sessions/
  Misc/
  MR DICOM/
  applog/Logs/
  ```

---

### **2) `process_mri_package.py` — v1.4.0**  
Normalizes MRI archives into a deterministic zip.

**Behavior**
- Auto-detects case_dir and norm-id from filenames  
- Normalizes MRI packaging into `MR DICOM/<CASEID>_MRI.zip`  
- Removes empty `DICOM/` folders  
- Logs to `applog/Logs/process_mri_package.log`

---

### **3) `master_run.py` — v2.3.2-compat+autoid**  
Primary orchestrator coordinating the entire pipeline.

**Pipeline**
1. TDC cleanup  
2. MRI packaging  
3. PDF normalization  
4. SQLite anonymization  
5. PEDA run/simulate (Python PEDA Engine)  
6. PEDA archive packaging  
7. Structure guard (`structure_guard.py`)

---

### **4) `src/structure_guard.py` — v0.2**  
Verifies and repairs post-run layout.

**Enforces**
- Normal treatment report locations  
- No stray `DICOM/` folders  
- Merged logs  
- Removal of empty directories  
- Safe re-runs until canonical

---

### **5) Python PEDA Engine — *v0.9* (New)**  
Modern, modular replacement for MATLAB PEDA v9.1.3.

**Modules include**
- Thermal: `CreateTMaxTDose.py`, `CreateIsotherms.py`  
- Raw parsing: `ParseRawDataFolder.py`, `ReadData.py`  
- Logs: `TreatmentControllerSummary.py`, `AnalyzeHardwareLogs.py`  
- Sx parameters: `RetrieveSxParameters.py`  
- QA: `GenerateMovies.py`, `PlotTmax.py`  
- Masking: `CalculateDynamicMasks.py`, `AdditionalImageMasking.py`  
- Validation: `task_master.py`

**Highlights**
- Deterministic PEDA folder per session  
- TMap, TMax, TDose, MaxTemperatureTime, 55°C isotherms  
- JSON summaries + hardware/controller analytics  
- Optional PNG/MP4 QA artifacts  
- Naming aligned with MATLAB `.mat` but output in `.npy/.csv/.json`

---

# ✔️ Recent Updates

- Added **Python PEDA Engine** for reconstruction and analytics  
- Standardized **PEDA output schema** mirroring MATLAB SEGMENT 1  
- Added canonical folders: `PEDA/`, `HWAnalysis/`, `Masks/`, `Movies/`, `TreatmentController/`  
- Implemented `task_master.py` and shadow detection  
- Unified staging for Raw + local.db  
- Eliminated MATLAB dependencies entirely  
- Strengthened segment discovery and case verification pathways  
