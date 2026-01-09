# PURPOSE: Parse a case directory pasted into a text file.
# INPUTS: tools/incoming.txt (first non-empty line), optional -InputFile override.
# OUTPUTS: case_dir, case_num, out_root, and explicit mri/tdc/pdf paths (if provided).
# NOTES: Accepts quoted Windows paths; trims trailing slashes; prints candidate full paths if missing.

param(
    [string]$InputFile = (Join-Path $PSScriptRoot "incoming.txt")
)

function Normalize-PathInput {
    param([string]$Value)
    if ($null -eq $Value) { return $null }
    $t = $Value.Trim()
    if ($t.Length -ge 2) {
        if (($t.StartsWith('"') -and $t.EndsWith('"')) -or ($t.StartsWith("'") -and $t.EndsWith("'"))) {
            $t = $t.Substring(1, $t.Length - 2).Trim()
        }
    }
    $t = $t.TrimEnd('\', '/')
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

function Trim-After-Extension {
    param(
        [string]$Value,
        [string]$Extension
    )
    if (-not $Value) { return $null }
    $val = $Value
    $extIndex = $val.ToLower().LastIndexOf($Extension.ToLower())
    if ($extIndex -ge 0 -and ($extIndex + $Extension.Length) -lt $val.Length) {
        return $val.Substring(0, $extIndex + $Extension.Length)
    }
    return $val
}

function Parse-IncomingFile {
    param([string]$InputFilePath)
    $result = [ordered]@{
        CaseDir = $null
        CaseNum = $null
        OutRoot = $null
        MriPath = $null
        TdcPath = $null
        PdfPath = $null
    }

    if (-not (Test-Path $InputFilePath)) {
        Write-Error "Input file not found: $InputFilePath"
        exit 2
    }

    $lines = Get-Content $InputFilePath | Where-Object {
        $_ -and $_.Trim() -ne "" -and -not $_.Trim().StartsWith("#")
    }
    if (-not $lines) {
        Write-Error "No path found in: $InputFilePath"
        exit 2
    }

    foreach ($line in $lines) {
        $trim = $line.Trim()
        if ($trim -match '^\s*([A-Za-z_]+)\s*[:=]\s*(.+)$') {
            $key = $matches[1].ToLower()
            $val = Normalize-PathInput $matches[2]
            switch ($key) {
                "case_dir" { $result.CaseDir = $val }
                "case_num" { $result.CaseNum = $val }
                "out_root" { $result.OutRoot = $val }
                "mri_path" { $result.MriPath = $val }
                "tdc_path" { $result.TdcPath = $val }
                "pdf_path" { $result.PdfPath = $val }
            }
        } elseif (-not $result.CaseDir) {
            $result.CaseDir = Normalize-PathInput $trim
        }
    }

    return [PSCustomObject]$result
}

$parsed = Parse-IncomingFile $InputFile
$caseDir = $parsed.CaseDir
$caseNum = $parsed.CaseNum
$outRoot = $parsed.OutRoot

$mriRaw = Trim-After-Extension $parsed.MriPath ".zip"
$tdcRaw = Trim-After-Extension $parsed.TdcPath ".zip"
$pdfRaw = Trim-After-Extension $parsed.PdfPath ".pdf"

$mriPath = Resolve-RelativePath $mriRaw $caseDir
$tdcPath = Resolve-RelativePath $tdcRaw $caseDir
$pdfPath = Resolve-RelativePath $pdfRaw $caseDir

if (-not $caseDir) {
    $parents = @()
    foreach ($p in @($mriPath, $tdcPath, $pdfPath)) {
        if ($p) {
            $parent = Split-Path $p -Parent
            if ($parent) { $parents += $parent }
        }
    }
    if ($parents.Count -gt 0) {
        $first = $parents[0]
        $allSame = $true
        foreach ($p in $parents) {
            if ((Normalize-PathInput $p) -ne (Normalize-PathInput $first)) {
                $allSame = $false
                break
            }
        }
        if ($allSame) {
            $caseDir = $first
        } elseif ($mriPath) {
            $caseDir = Split-Path $mriPath -Parent
        } elseif ($tdcPath) {
            $caseDir = Split-Path $tdcPath -Parent
        } elseif ($pdfPath) {
            $caseDir = Split-Path $pdfPath -Parent
        }
    }
}

if (-not $outRoot -and $caseDir) {
    $outRoot = Split-Path $caseDir -Parent
}

if (-not $caseNum -and $caseDir) {
    $caseNum = Split-Path $caseDir -Leaf
}

$mriSuggestion = $null
$tdcSuggestion = $null
$pdfSuggestion = $null
if (Test-Path $caseDir) {
    $files = Get-ChildItem -File $caseDir
    $zipFiles = $files | Where-Object { $_.Extension -ieq ".zip" }
    $pdfFiles = $files | Where-Object { $_.Extension -ieq ".pdf" }
    if (-not $mriPath) {
        $mriCandidates = $zipFiles | Where-Object { $_.Name -match '(?i)(^|[^A-Za-z0-9])MR([^A-Za-z0-9]|$)|(?i)MRI' }
        if ($mriCandidates.Count -eq 1) {
            $mriSuggestion = $mriCandidates[0].Name
        }
    }
    if (-not $tdcPath) {
        $tdcCandidates = $zipFiles | Where-Object { $_.Name -match '(?i)TDC' }
        if ($tdcCandidates.Count -eq 1) {
            $tdcSuggestion = $tdcCandidates[0].Name
        }
    }
    if (-not $pdfPath) {
        if ($pdfFiles.Count -eq 1) {
            $pdfSuggestion = $pdfFiles[0].Name
        }
    }
}

Write-Host ("case_dir: " + ($(if ($caseDir) { $caseDir } else { "<missing>" })))
Write-Host ("case_num: " + ($(if ($caseNum) { $caseNum } else { "<missing>" })))
Write-Host ("out_root: " + ($(if ($outRoot) { $outRoot } else { "<missing>" })))

$mriCandidatePath = $null
if ($mriPath) {
    $mriCandidatePath = $mriPath
} elseif ($mriSuggestion) {
    $mriCandidatePath = Join-Path $caseDir $mriSuggestion
}

$tdcCandidatePath = $null
if ($tdcPath) {
    $tdcCandidatePath = $tdcPath
} elseif ($tdcSuggestion) {
    $tdcCandidatePath = Join-Path $caseDir $tdcSuggestion
}

$pdfCandidatePath = $null
if ($pdfPath) {
    $pdfCandidatePath = $pdfPath
} elseif ($pdfSuggestion) {
    $pdfCandidatePath = Join-Path $caseDir $pdfSuggestion
}

$mriOut = if ($mriCandidatePath -and (Test-Path -LiteralPath $mriCandidatePath -PathType Leaf)) { $mriCandidatePath } else { "<missing>" }
$tdcOut = if ($tdcCandidatePath -and (Test-Path -LiteralPath $tdcCandidatePath -PathType Leaf)) { $tdcCandidatePath } else { "<missing>" }
$pdfOut = if ($pdfCandidatePath -and (Test-Path -LiteralPath $pdfCandidatePath -PathType Leaf)) { $pdfCandidatePath } else { "<missing>" }
Write-Host "mri_path: $mriOut"
Write-Host "tdc_path: $tdcOut"
Write-Host "pdf_path: $pdfOut"
