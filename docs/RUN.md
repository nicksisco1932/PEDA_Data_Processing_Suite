# Running the PEDA Mini-Pipeline

Use the config file to make runs reproducible, and override values on the CLI when needed.

Command
```
python .\src\controller.py --config .\config\pipeline.yaml
```

Editing the config
- Update `config/pipeline.yaml` with your `root`, `case`, and input zip paths.
- Use `null` to keep defaults (e.g., `scratch` or `run_id`).
- Set `dry_run: true` to validate and print the plan without modifying files.

CLI overrides
- Any CLI argument provided overrides the config value.
- Example:
```
python .\src\controller.py --config .\config\pipeline.yaml --case 101_01-010 --dry-run
```

Outputs and locations
- Canonical case directory layout (required):
```
<root>\<case_id>
|-- Misc
|   |-- <case_id>_TreatmentReport.pdf
|   |-- Logs
|       |-- <case_id> Tdc.<YYYY_MM_DD>.log
|
|-- MR DICOM
|   |-- <case_id>_MRI.zip
|
|-- <case_id> PEDAv9.1.3-Data.zip (placeholder)
|
|-- run_logs
|   |-- <case_id>__<run_id>.log
|   |-- <case_id>__<run_id>__manifest.json
|
|-- annon_logs
|   |-- localdb_check_pre.json
|   |-- localdb_check_post.json
|   |-- PEDA_run_log.txt
|
|-- TDC Sessions
    |-- <session_name>
        |-- Raw\<YYYY-MM-DD>\
```
- Inputs can live anywhere (explicit paths or auto-discovery); outputs are always written to the canonical folders above.
- MRI final zip: `<root>/<case>/MR DICOM/<case>_MRI.zip`
- TDC final session: `<root>/<case>/TDC Sessions/<session_name>`
- Treatment report (if provided): `<root>/<case>/Misc/<case>_TreatmentReport.pdf`
- PEDA artifacts (stub): `<root>/<case>/<case> PEDAv9.1.3-Video`, `<root>/<case>/<case> PEDAv9.1.3-Data.zip`, `<root>/<case>/annon_logs/PEDA_run_log.txt`
- Run logs: `<root>/<case>/run_logs/<case>__<run_id>.log`
- Manifests: `<root>/<case>/run_logs/<case>__<run_id>__manifest.json`

Dry-run behavior
- Performs validations only and logs a planned actions list.
- Does not copy, extract, zip, or delete data.
- Manifest still records the plan and resolved config.

Scratch location
- By default, scratch uses a local temp path: `%TEMP%\PEDA\<case_id>\<run_id>`.
- To keep scratch under the case folder, set in config:
  `run.scratch.policy: "case_root"` or use `--scratch-policy case_root`.

Failure handling
- Check the log file first for the actionable error message.
- Common causes: missing case directory, missing MR/TDC folders, wrong zip paths, invalid zip extension.

Exit codes
- `2`: validation/config error (missing paths, bad config)
- `3`: processing/runtime error (expected failure)
- `4`: unexpected exception
