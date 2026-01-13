Clear-Host
# Prompt user for folder path
$folderPath = Read-Host "Enter the full path to the folder"

# Check if the folder exists
if (Test-Path -Path $folderPath -PathType Container) {
    # Get all .mat files in the folder and subfolders
    $matFiles = Get-ChildItem -Path $folderPath -Filter *.mat -File -Recurse

    if ($matFiles.Count -eq 0) {
        Write-Host "No .mat files found in the folder or its subfolders."
    } else {
        # Remove each .mat file
        foreach ($file in $matFiles) {
            try {
                Remove-Item -Path $file.FullName -Force
                Write-Host "Deleted: $($file.FullName)"
            } catch {
                Write-Host "Failed to delete: $($file.FullName) - $_"
            }
        }
    }
} else {
    Write-Host "The specified folder does not exist."
}
