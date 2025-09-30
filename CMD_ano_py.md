# Install dependency (once)
pip install pydicom

# Dry run (default)
python anonymize_dicom.py --site-id 017-01_474 --birthdate 1960-01-01

# Apply, with backups and extended fields
python anonymize_dicom.py --site-id 017-01_474 --birthdate 19600101 --apply --backup --write-extras

# Add custom plan at runtime (overrides)
python anonymize_dicom.py --site-id 017-01_474 --birthdate 01/01/1960 --apply \
  --plan-json .\my_plan.json

# Skip additional telemetry types
python anonymize_dicom.py --site-id 017-01_474 --birthdate 19600101 --apply \
  --skip-suffix Stats.dat --skip-suffix Diagnostics.dat

# Produce a CSV audit in addition to JSONL
python anonymize_dicom.py --site-id 017-01_474 --birthdate 19600101 --csv-audit


my_plan.json example
{
  "PatientName": "017-01_474",
  "PatientBirthDate": "19600101",
  "StudyID": "1",
  "ReferringPhysicianName": "ProfoundMedical",
  "InstitutionName": "PMI Training Center"
}