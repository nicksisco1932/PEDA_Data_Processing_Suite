# Legacy PowerShell References

These scripts are historical references only. They are not executed by the Python pipeline.

## 1. Commercial Treatments.ps1
- Prompts for Site ID and Treatment number, creates a case folder, and copies template folders.
- Moves files from the user's Downloads folder into the case folder.
- Expands each `.zip` to a same-named folder and opens Explorer.

Hazards:
- Interactive prompts and hard-coded paths.
- `Set-Location` side effects and forced Moves from Downloads.
- GUI automation (`Start-Process explorer.exe`) not suitable for non-interactive runs.

## 2. Commercial Treatment Processing.ps1
- Uses external tools (`dcmftest.exe`, `dcmodify.exe`) to rewrite DICOM tags.
- Builds a tag map for common identifiers (name, IDs, institution, etc.).

Hazards:
- Interactive entry of birthdate and other PHI values.
- Variable name mismatch (`$Filelocation` vs `$FileLocation`).
- Suppresses stderr/stdout and relies on current working directory.

## 3. Create PEDA Video.ps1
- Recursively deletes `*.mat` files under a user-specified folder.

Hazards:
- Destructive deletes without guardrails.
- Interactive prompt and misleading script name.
