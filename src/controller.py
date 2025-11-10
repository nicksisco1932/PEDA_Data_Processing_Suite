# controller.py
from pathlib import Path
import argparse, sys, shutil
import MRI_proc, TDC_proc

def fail(msg, code=1):
    print(f"[ERROR] {msg}", file=sys.stderr); sys.exit(code)

def main():
    p = argparse.ArgumentParser(description="PEDA mini-pipeline controller")
    p.add_argument("--root", default=r"E:\Data_Clean", help="Root output folder")
    p.add_argument("--case", required=True, help="Case ID, e.g., 101_01-010")
    p.add_argument("--mri-input", help=r'MRI zip, e.g., E:\101-01-010\MRI-101-01-110.zip')
    p.add_argument("--tdc-input", help=r'TDC zip, e.g., E:\101-01-010\TDC-101-01-110.zip')
    p.add_argument("--scratch", help="Scratch dir (default <root>/<case>/scratch)")
    p.add_argument("--clean-scratch", action="store_true", help="Delete scratch after success")
    p.add_argument("--date-shift-days", type=int, default=137, help="TDC date shift (anonymization)")
    p.add_argument("--skip-mri", action="store_true", help="Skip MRI step")
    p.add_argument("--skip-tdc", action="store_true", help="Skip TDC step")
    args = p.parse_args()

    root = Path(args.root); case = args.case
    case_dir = root / case
    mr_dir = case_dir / f"{case} MR DICOM"
    tdc_dir = case_dir / f"{case} TDC Sessions"
    scratch = Path(args.scratch) if args.scratch else (case_dir / "scratch")

    # minimal validations
    if not case_dir.exists(): fail(f"Case dir not found: {case_dir}", 2)
    if not mr_dir.exists():   fail(f"MR DICOM folder not found: {mr_dir}", 2)
    if not tdc_dir.exists():  fail(f"TDC Sessions folder not found: {tdc_dir}", 2)
    scratch.mkdir(parents=True, exist_ok=True)

    print(f"🧹 scratch: {scratch}")

    # MRI
    if not args.skip_mri:
        if not args.mri_input: fail("--mri-input is required (or use --skip-mri)", 2)
        MRI_proc.run(
            root=root, case=case,
            input_zip=Path(args.mri_input),
            scratch=scratch
        )

    # TDC
    if not args.skip_tdc:
        if not args.tdc_input: fail("--tdc-input is required (or use --skip-tdc)", 2)
        TDC_proc.run(
            root=root, case=case,
            input_zip=Path(args.tdc_input),
            scratch=scratch,
            date_shift_days=args.date_shift_days
        )

    if args.clean_scratch:
        shutil.rmtree(scratch, ignore_errors=True)
        print("🧽 scratch deleted.")

if __name__ == "__main__":
    main()
