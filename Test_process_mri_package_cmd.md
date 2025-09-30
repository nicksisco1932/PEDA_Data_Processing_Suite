# Paths
$zip = "D:\Database_project\test_data\test_data\mri-017-01_474.zip"
$root = "C:\Data_Clean"   # or a D:\staging folder if you prefer
$dob  = "19600101"        # test date

# Dry run (no writes to DICOMs)
python process_mri_package.py --input $zip --birthdate $dob --root $root

# Apply + backups + extras (recommended on second pass)
python process_mri_package.py --input $zip --birthdate $dob --root $root --apply --backup --write-extras --csv-audit
