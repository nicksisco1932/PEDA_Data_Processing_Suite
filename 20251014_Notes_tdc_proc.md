# Plan (no code changes yet)

1. **Unzip input** into:

```
<SCRATCH>\<ZIP_STEM>\
  extracted\
  output\
```

2. **Immediately back up `local.db` inside `extracted/`**

* Create: `extracted/local.db.bak` (byte-for-byte copy of `extracted/local.db`)
* Purpose: `local.db.bak` is the **anonymization input**, while `local.db` stays untouched as the ground truth.

3. **Identify contents** (top-level under `extracted/`):

* Session directories (e.g., `2025-10-21--09-14-33`, `2025-10-21--09-14-50`)
* `Raw/` directory (if present)
* `local.db` (original)
* `local.db.bak` (new, created in step 2)

4. **Zip each session directory + Raw** → into `output/`

* `output/<session>.zip` for each session dir
* `output/Raw.zip` if `Raw/` exists

5. **Local DB anonymization (later)**

* Future step will read **`extracted/local.db.bak`** → produce anonymized DB as `output/local_anon.db`
* **Do not** move or touch `extracted/local.db` during anonymization
* If anonymization fails, **leave `output/local_anon.db` absent** (so controller can detect failure)

6. **Print final file structure** (rooted at `<SCRATCH>\<ZIP_STEM>\`), showing:

* `extracted/` with `local.db` (original) and `local.db.bak` (backup for anon input)
* `output/` with `*.zip` (sessions + Raw) and (in the future) `local_anon.db` if/when anon completes

# Example resulting tree

```
<SCRATCH>\<ZIP_STEM>\
  ├─ extracted\
  │    ├─ local.db              ← original, untouched
  │    ├─ local.db.bak          ← backup; input to anonymizer
  │    ├─ Raw\...
  │    ├─ 2025-10-21--09-14-33\...
  │    └─ 2025-10-21--09-14-50\...
  │
  └─ output\
       ├─ Raw.zip
       ├─ 2025-10-21--09-14-33.zip
        └─ 2025-10-21--09-14-50.zip
       # (later) local_anon.db  ← only after anonymization succeeds
```

# Console prints I’ll add

* “Unzipped: …/extracted”
* “Found: N session dirs, Raw: yes/no, local.db: yes”
* “Created backup: …/extracted/local.db.bak”
* “Zipped: …/output/<session>.zip (count files=…, size=…)”
* “Zipped: …/output/Raw.zip (count files=…, size=…) [if present]”
* “(Placeholder) Next step will anonymize extracted/local.db.bak → output/local_anon.db”
* Final tree print (dirs/files with sizes) + minimal JSON summary:

  * `workspace`, `extracted_root`, `output_dir`
  * `created_zips`
  * `local_db_original` (path)
  * `local_db_bak` (path)
  * `local_anon_db` (None until implemented)
