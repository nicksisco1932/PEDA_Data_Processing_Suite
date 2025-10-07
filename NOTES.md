## NOTES
A lot things are changing quickly in this, and it's hard to take notes in Windows b/c history > history barely works.

Sept. 29, 2025
More or less working.

# Wrapper: end-to-end, writing to D:\Data_Clean (logs there too)
python process_mri_package.py --input "D:\Database_project\test_data\test_data\mri-017-01_474.zip" `
  --birthdate 19600101 --out-root "D:\Data_Clean" --apply --backup --csv-audit


## master_run.py
# 1) Clean/organize TDC zips
# 2) Anonymize MRI and package to D:\Data_Clean\017_01-474\017_01-474 MR DICOM\017_01-474_MRI.zip
python master_run.py `
   --tdc-items ".\test_data\test_data\017-01_474_TDC.zip" `        
   --mri-input "D:\Database_project\test_data\test_data\mri-017-01_474.zip" `  
   --birthdate 19600101 `
   --out-root "D:\Data_Clean" `
   --apply --backup --csv-audit


# Working as of 9/30
python master_run.py "D:\Database_project\test_data\test_data\017-01_479_TDC.zip" --mri-input "D:\Database_project\test_data\test_data\MRI-017-01_479.zip" --out-root "D:\Data_Clean" --simulate-peda > tmp


# Newest 9/30 1354
python master_run.py "C:\Users\nicks\Desktop\WORK_Delete_boring\Database_project\test_data\test_data\017-01_479_TDC.zip" `
>>  --mri-input "C:\Users\nicks\Desktop\WORK_Delete_boring\Database_project\test_data\test_data\MRI-017-01_479.zip" `
>>  --patient-birthdate 19000101 `
>>  --mri-apply `
>>  --out-root "C:\Users\nicks\Desktop\WORK_Delete_boring\Data_Clean" `
>>  --simulate-peda `
>>   --peda-path "C:\Users\nicks\Desktop\WORK_Delete_boring\PEDAv9.1.3"

# Code test run Oct. 7, 2025
python master_run.py "C:\Users\nicks\Desktop\WORK_Delete_boring\Database_project\test_data\test_data\017-01_479_TDC.zip" `
  --mri-input "C:\Users\nicks\Desktop\WORK_Delete_boring\Database_project\test_data\test_data\MRI-017-01_479.zip" `
  --patient-birthdate 19000101 `
  --mri-apply `
  --out-root "C:\Users\nicks\Desktop\WORK_Delete_boring\Data_Clean" `
  --simulate-peda `
  --peda-path "C:\Users\nicks\Desktop\WORK_Delete_boring\PEDAv9.1.3"
