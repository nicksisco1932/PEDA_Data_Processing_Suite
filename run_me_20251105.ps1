$Args = @(
  '.\tests\master_proc.py',
  '--scratch-root','D:\scratch',
  '--case-id','101_01-010',
  '--tdc-zip','D:\101-01-010\TDC-101-01-110.zip',
  '--mri-zip','D:\101-01-010\MRI-101-01-110.zip',
  '--treatment-pdf','D:\101-01-010\TreatmentReport-101-01-110.pdf',
  '--tdc-anon','--mri-anon','--peda-simulate',
  '--matlab-exe','C:\Program Files\MATLAB\R2024b\bin\matlab.exe',
  '--peda-home','C:\Users\NicholasSisco\Local_apps\PEDA'
)

python @Args
