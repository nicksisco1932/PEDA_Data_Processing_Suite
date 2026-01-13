# Controller invariants (PEDA_Data_Processing_Suite)

This document defines the behavioral invariants that `controller.py` must preserve. Refactors are allowed as long as these invariants hold. The self-test suite is the oracle.

## Inputs and resolution

1. **Input identity is argument-driven, not basename-driven.**
   - MRI/TDC/PDF semantics are determined solely by the CLI/YAML fields they are supplied to.
   - Filenames may be arbitrary, misleading, or contain conflicting tokens (e.g., an MRI zip named `tdc.zip`) without affecting routing.

2. **Windows "Copy as path" strings are accepted verbatim.**
   - Any path may arrive with surrounding quotes and/or whitespace padding.
   - Inputs are sanitized deterministically (trim whitespace, strip surrounding quotes) before filesystem use.

3. **Zip validity is content-driven.**
   - Valid zip archives are accepted even if the filename extension is non-standard (e.g., `.zip.zip`, `.ZIP.ZIP`), as long as the underlying content is a valid zip.
   - If an input is invalid, failures are early, explicit, and actionable.

4. **Required vs optional inputs**
   - MRI and TDC inputs are required unless their corresponding skip flag is enabled.
   - The treatment PDF is optional; missing/invalid PDF produces a warning (not a hard failure) unless explicitly required by configuration.

## Output schema

5. **Canonical case directory layout is invariant.**
   Under the case directory, the controller must produce these top-level output folders:
   - `<Case_ID> MR DICOM`
   - `<Case_ID> TDC Sessions`
   - `<Case_ID> Misc`
   - `run_logs`
   - `annon_logs`
   If PEDA is enabled, these top-level artifacts are also allowed:
   - `<Case_ID> PEDA<version>-Video`
   - `<Case_ID> PEDA<version>-Data.zip`

6. **No zip artifacts under outputs.**
   - No `.zip` or `.zip.zip` files are permitted anywhere inside the canonical output tree, except the PEDA data zip when PEDA is enabled.

7. **Output naming is canonical, not derived from input stems.**
   - Output folder names do not depend on input basenames.

## Safety and side effects

8. **Backups are created deterministically for inputs.**
   - For each supplied input file, a single `.bak` backup is created according to the current backup policy.
   - Backup behavior is stable under filename permutations.

9. **Dry-run semantics are strict.**
   - In dry-run mode, no filesystem mutations occur beyond what is explicitly defined as permissible for dry-run (and those rules must remain stable).

10. **Structure enforcement is strict and deterministic.**
   - The controller enforces the canonical output schema.
   - If canonicalization cannot be performed unambiguously, the run fails with a clear reason and an inventory of offending paths.

## Manifest contract

11. **Manifest schema is a compatibility contract.**
   - Manifest keys, nesting, and meanings must remain stable across refactors.
   - `config.run.flags` must exist and reflect the effective run flags (e.g., `test_mode`, `allow_workspace_zips`, `legacy_filename_rules`), without relying on in-place mutation of the loaded config.

12. **Manifest is emitted once per run as the authoritative record.**
   - The manifest is built from finalized run state (inputs, outputs, timings, status) and written exactly once to its canonical location.

## Test oracle

13. **Self-test is the behavioral lock.**
   - `python .\src\controller.py --self-test` must pass unchanged.
   - The self-test permutation matrix covers:
     - CLI vs YAML parity
     - raw vs quoted/padded paths
     - misleading basenames
     - multi-dot names
     - `.zip.zip` variants
     - negative cases (wrong extension, missing file, empty string, directory path)

Any change that breaks a self-test invariant is a behavior change and must be treated as such (either revert or explicitly revise the invariant and update tests accordingly).
