# PURPOSE: Parse explicit paths from incoming.txt or prompt for case inputs, then run the controller.
# INPUTS: incoming.txt (optional), case_num, out_root, mri_path, tdc_path, optional pdf_path and run_id.
# OUTPUTS: Pipeline run with logs/manifest in repo logs/.
# NOTES: Accepts quoted Windows "Copy as path" strings.

param(
    [string]$InputFile = (Join-Path $PSScriptRoot "incoming.txt"),
    [switch]$SelfTest
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
$Controller = Join-Path $RepoRoot "src\controller.py"
$LogDir = Join-Path $RepoRoot "logs"

function Normalize-PathInput {
    param([string]$Value)
    if ($null -eq $Value) { return $null }
    $t = $Value.Trim()
    if ($t.Length -ge 2) {
        if (($t.StartsWith('"') -and $t.EndsWith('"')) -or ($t.StartsWith("'") -and $t.EndsWith("'"))) {
            $t = $t.Substring(1, $t.Length - 2).Trim()
        }
    }
    return $t
}

function Resolve-RelativePath {
    param(
        [string]$Value,
        [string]$BaseDir
    )
    if (-not $Value) { return $null }
    $val = Normalize-PathInput $Value
    if ($val -match '^[A-Za-z]:\\') {
        return $val
    }
    try {
        if ([System.IO.Path]::IsPathRooted($val)) {
            return $val
        }
    } catch {
        return $val
    }
    if ($BaseDir) {
        return (Join-Path $BaseDir $val)
    }
    return $val
}

function Get-IncomingValue {
    param(
        [string[]]$Lines,
        [string]$Key
    )
    $pattern = "^\s*$Key\s*[:=]\s*(.+)$"
    foreach ($line in $Lines) {
        $trim = $line.Trim()
        $match = [regex]::Match($trim, $pattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
        if ($match.Success) {
            return Normalize-PathInput $match.Groups[1].Value
        }
    }
    return $null
}

function Parse-IncomingFile {
    param([string]$InputFilePath)
    $result = [ordered]@{
        CaseDir = $null
        MriPath = $null
        TdcPath = $null
        PdfPath = $null
    }

    if (-not (Test-Path $InputFilePath)) {
        return [PSCustomObject]$result
    }

    $lines = Get-Content $InputFilePath | Where-Object {
        $_ -and $_.Trim() -ne "" -and -not $_.Trim().StartsWith("#")
    }
    if (-not $lines) {
        return [PSCustomObject]$result
    }

    $result.CaseDir = Get-IncomingValue $lines "case_dir"
    $result.MriPath = Get-IncomingValue $lines "mri_path"
    $result.TdcPath = Get-IncomingValue $lines "tdc_path"
    $result.PdfPath = Get-IncomingValue $lines "pdf_path"

    if (-not $result.CaseDir) {
        $result.CaseDir = Normalize-PathInput $lines[0]
    }

    return [PSCustomObject]$result
}

function Validate-InputPath {
    param(
        [string]$Value,
        [string]$Label,
        [string]$ExpectedExtension,
        [switch]$AllowMissing
    )
    if ([string]::IsNullOrWhiteSpace($Value)) {
        if ($AllowMissing) { return $null }
        throw "$Label is required."
    }
    $pathValue = Normalize-PathInput $Value
    $extIndex = $pathValue.ToLower().LastIndexOf($ExpectedExtension.ToLower())
    if ($extIndex -ge 0 -and ($extIndex + $ExpectedExtension.Length) -lt $pathValue.Length) {
        $pathValue = $pathValue.Substring(0, $extIndex + $ExpectedExtension.Length)
    }
    if (-not (Test-Path -LiteralPath $pathValue -PathType Leaf)) {
        throw "$Label not found or not a file: $pathValue"
    }
    $ext = [System.IO.Path]::GetExtension($pathValue)
    if ($ext -ne $ExpectedExtension) {
        throw "$Label must be ${ExpectedExtension}: $pathValue"
    }
    return $pathValue
}

function Show-CaseListing {
    param([string]$CaseDir)
    if (-not $CaseDir) { return }
    if (-not (Test-Path $CaseDir)) {
        Write-Warning "case_dir does not exist: $CaseDir"
        return
    }
    Write-Host "($($CaseDir):"
    Get-ChildItem -File -Name $CaseDir | Sort-Object | ForEach-Object { Write-Host $_ }
    Write-Host ")"
}

$caseNum = $null
$outRoot = $null
$mriPath = $null
$tdcPath = $null
$pdfPath = $null

if ($SelfTest) {
    Write-Host "Running: python $Controller --self-test"
    & python $Controller --self-test
    exit $LASTEXITCODE
}

$parsed = Parse-IncomingFile $InputFile
$caseDir = $parsed.CaseDir
if ($caseDir) {
    $caseNum = Split-Path $caseDir -Leaf
    $outRoot = Split-Path $caseDir -Parent
}
$mriPath = Resolve-RelativePath $parsed.MriPath $caseDir
$tdcPath = Resolve-RelativePath $parsed.TdcPath $caseDir
$pdfPath = Resolve-RelativePath $parsed.PdfPath $caseDir

$parseOk = $true
$failReason = $null
try {
    if (-not $caseDir) {
        throw "case_dir missing in $InputFile"
    }
    if (-not (Test-Path $caseDir)) {
        throw "case_dir does not exist: $caseDir"
    }
    if (-not $mriPath) {
        throw "mri_path missing in $InputFile (no filename inference)"
    }
    if (-not $tdcPath) {
        throw "tdc_path missing in $InputFile (no filename inference)"
    }
    $mriPath = Validate-InputPath $mriPath "mri_path" ".zip"
    $tdcPath = Validate-InputPath $tdcPath "tdc_path" ".zip"
    if ($pdfPath) {
        $pdfPath = Validate-InputPath $pdfPath "pdf_path" ".pdf" -AllowMissing
    }
} catch {
    $parseOk = $false
    $failReason = $_.Exception.Message
}

if ($parseOk) {
    Write-Host "Using incoming.txt: $InputFile"
    Write-Host "case_dir: $caseDir"
    Write-Host "case_num: $caseNum"
    Write-Host "out_root: $outRoot"
    Write-Host "mri_path: $mriPath"
    Write-Host "tdc_path: $tdcPath"
    if ($pdfPath) { Write-Host "pdf_path: $pdfPath" }
} else {
    Write-Host "FAIL: $failReason"
    Write-Host "Paste full paths verbatim (Copy as path) when prompted."
    Show-CaseListing $caseDir
}

if (-not $caseNum) {
    $caseNum = (Read-Host "case_num (e.g., 093_01-098)").Trim()
}
if (-not $outRoot) {
    $outRoot = Normalize-PathInput (Read-Host "out_root (e.g., E:\Data_Clean)")
}

if (-not $parseOk) {
    $null = Read-Host "Press Enter to paste inputs manually"
    $mriPath = Normalize-PathInput (Read-Host "mri_path (.zip, Copy as path ok)")
    $tdcPath = Normalize-PathInput (Read-Host "tdc_path (.zip, Copy as path ok)")
    $pdfPath = Normalize-PathInput (Read-Host "pdf_path (optional; leave blank to skip)")
    try {
        $mriPath = Validate-InputPath $mriPath "mri_path" ".zip"
        $tdcPath = Validate-InputPath $tdcPath "tdc_path" ".zip"
        if ($pdfPath) {
            $pdfPath = Validate-InputPath $pdfPath "pdf_path" ".pdf" -AllowMissing
        } else {
            $pdfPath = $null
        }
    } catch {
        Write-Error $_.Exception.Message
        exit 2
    }
}

$runId = (Read-Host "run_id (optional; leave blank for auto)").Trim()

if ([string]::IsNullOrWhiteSpace($caseNum)) {
    Write-Error "case_num is required."
    exit 2
}
if ([string]::IsNullOrWhiteSpace($outRoot)) {
    Write-Error "out_root is required."
    exit 2
}
if ([string]::IsNullOrWhiteSpace($mriPath)) {
    Write-Error "mri_path is required."
    exit 2
}
if ([string]::IsNullOrWhiteSpace($tdcPath)) {
    Write-Error "tdc_path is required."
    exit 2
}
if ([string]::IsNullOrWhiteSpace($pdfPath)) {
    $pdfPath = $null
}
if ([string]::IsNullOrWhiteSpace($runId)) {
    $runId = $null
}

$args = @(
    "--root", $outRoot,
    "--case", $caseNum,
    "--mri-input", $mriPath,
    "--tdc-input", $tdcPath,
    "--log-dir", $LogDir
)
if ($pdfPath) { $args += @("--pdf-input", $pdfPath) }
if ($runId) { $args += @("--run-id", $runId) }

Write-Host "Running: python $Controller $($args -join ' ')"
& python $Controller @args
exit $LASTEXITCODE
