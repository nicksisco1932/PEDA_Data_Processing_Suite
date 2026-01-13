Clear-Host
#Attemtping to automate the processing using passed variables.

$SiteID = Read-Host -prompt "Enter Treatment #:"
$Birth = Read-Host -prompt "Enter Birthday"

$Filelocation = "c:\data_Clean\" + $SiteID

Set-Location $FileLocation

$newName = $SiteID
$NewStudyID = "1"
$NewBirthday = $Birth
$NewAccessionNumber = "Accession"
$NewInstitutionName = "Institution"
$NewInstitutionAddress = "Address"
$NewReferringPhysicianName = "ProfoundMedical"
##$NewOperatorsName = "PMI"
$NewPatientSex = "M"
##$NewPatientAge = "65Y"
$NewPatientSize = "1.8"
$NewPatientWeight = "80"
$CountryOfResidence = "Country"
$EthnicGroup = "Group"
$Occupation = "Occupation"
$SmokingStatus = " "
$PregnancyStatus = "Status"
$PatientReligiousPreference = "ReligiousPreference"


$names = @(gci -force -recurse -name)
foreach($name in $names) {
    if ($name.EndsWith("Raw.dat") -or $name.EndsWith("Anatomy.dat") -or $name.EndsWith("CurrentTemperature.dat") -or $name.EndsWith("MaximumTemperature.dat")) {
        continue;
    }
    $out = c:\Data_Clean\dcmftest.exe $name | Out-String
    if ($out.StartsWith("yes")) {
        Write-Host "Anonymizing $name"
        C:\Data_Clean\dcmodify.exe -gin -nb -ie `
        -m "(0010,0010)=$newName" `
        -i "(0010,0020)=123456" `
        -i "(0010,0030)=$NewBirthday"`
        -i "(0010,0040)=$NewPatientSex"`
        -i "(0020,4000)=PMI" `
        -i "(0010,1001)=$newName"`
        -i "(0008,0080)=$NewInstitutionName"`
        -i "(0008,0081)=$NewInstitutionAddress"`
        -i "(0008,0050)=$NewAccessionNumber"`
        -i "(0008,0090)=$NewReferringPhysicianName"`
        -i "(0010,1020)=$NewPatientSize"`
        -i "(0010,1030)=$NewPatientWeight"`
        -i "(0010,2150)=$CountryOfResidence"`
        -i "(0010,2160)=$EthnicGroup"`
        -i "(0010,2180)=$Occupation"`
        -i "(0010,21A0)=$SmokingStatus"`
        -i "(0010,21C0)=$PregnancyStatus"`
        -i "(0010,21F0)=$PatientReligiousPreference"`
        -i "(0020,0010)=$NewStudyID" $name 1>$null 2>$null
    }
}