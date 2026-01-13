Clear-Host 
# Input the treatment data and create the required folders
$SiteID = Read-Host "Enter the Site ID"
$Treatment = Read-Host "Enter the Treatment #"

095
# Validate inputs
if (-not $SiteID -or -not $Treatment) {
    Write-Host "All inputs are required. Please try again." -ForegroundColor Red
    exit
}

$Folder = "C:\Data_Clean\" + $SiteID + "_01" + "-" + $Treatment
$TreatmentFolders = "C:\Data_Clean\TDC Folders\"

# Create the folder and copy the required folders
try {
    New-Item -Path $Folder -ItemType Directory -ErrorAction Stop
    Copy-Item -Path "$TreatmentFolders*" -Destination $Folder -Recurse -ErrorAction Stop
    Write-Host "Folders created and copied successfully." -ForegroundColor Green
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
    exit
}

# Set the default location to the treatment folder
Set-Location $Folder

# Get the path to the current user's Downloads folder
$DownloadsFolder = "$env:USERPROFILE\Downloads"

# Move the files from the Downloads folder to the destination folder
try {
    ##Copy-item -Path "$DownloadsFolder\*" -Destination $Folder -Force -ErrorAction Stop
    Move-Item -Path "$DownloadsFolder\*" -Destination $Folder -Force -ErrorAction Stop
    Write-Host "Files moved successfully." -ForegroundColor Green
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
}

# Unzip any .zip files in the folder to a folder of the same name
$zipFiles = Get-ChildItem -Path $Folder -Filter *.zip

foreach ($zipFile in $zipFiles) {
    $destinationFolder = Join-Path -Path $Folder -ChildPath ($zipFile.BaseName)
    try {
        Expand-Archive -Path $zipFile.FullName -DestinationPath $destinationFolder -Force
        Write-Host "Unzipped $($zipFile.Name) to $destinationFolder" -ForegroundColor Green
    } catch {
        Write-Host "Error unzipping $($zipFile.Name): $_" -ForegroundColor Red
    }
}

# Open File Explorer to the specified folder
Start-Process -FilePath explorer.exe -ArgumentList $Folder