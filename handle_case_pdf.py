# save as: handle_case_pdf.py
# usage:
#   python handle_case_pdf.py "D:\Data_Clean\017_01-479" --case-id "017_01-479" --dry-run
#   python handle_case_pdf.py "D:\Data_Clean\017_01-479" --case-id "017_01-479"

from __future__ import annotations
import argparse
import logging
import re
import shutil
from pathlib import Path
from typing import List, Optional

KEYWORDS = ["treatment", "report", "treatmentreport", "summary"]
PDF_EXT_RE = re.compile(r"(?i)\.pdf(?:\.pdf)+$")  # matches .pdf.pdf (and longer chains)

def setup_logger(log_dir: Path):
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "applog.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()]
    )

def normalize_pdf_suffix(name: str) -> str:
    """Collapse trailing .pdf.pdf… → .pdf (case-insensitive)."""
    if PDF_EXT_RE.search(name):
        return PDF_EXT_RE.sub(".pdf", name)
    # If wrong case like .PDF, normalize to lowercase .pdf
    if name.lower().endswith(".pdf") and not name.endswith(".pdf"):
        return name[: -4] + ".pdf"
    return name

def score_candidate(p: Path, case_id: str) -> int:
    """
    Higher is better. Heuristics:
    +3 if filename contains case_id (case-insensitive)
    +2 if any keyword present
    +1 if file is in top-level case folder (not deep)
    - length penalty (longer names slightly penalized)
    """
    name_low = p.name.lower()
    score = 0
    if case_id.lower() in name_low:
        score += 3
    if any(k in name_low for k in KEYWORDS):
        score += 2
    if len(p.parts) <= 3:  # shallow under case root
        score += 1
    score -= int(len(p.name) / 50)  # tiny penalty for very long names
    return score

def find_pdfs(case_root: Path) -> List[Path]:
    return [p for p in case_root.rglob("*") if p.is_file() and p.suffix.lower() == ".pdf"]

def best_pdf(pdfs: List[Path], case_id: str) -> Optional[Path]:
    if not pdfs:
        return None
    # Rank by heuristic score, then by modified time (newest first)
    ranked = sorted(
        pdfs,
        key=lambda p: (score_candidate(p, case_id), p.stat().st_mtime),
        reverse=True,
    )
    return ranked[0]

def main():
    ap = argparse.ArgumentParser(description="Find/fix/normalize case PDF into <CASEID> Misc/<CASEID>_TreatmentReport.pdf")
    ap.add_argument("case_root", help="Path to the case root folder (e.g., .../017_01-479)")
    ap.add_argument("--case-id", required=True, help="Normalized case ID (e.g., 017_01-479)")
    ap.add_argument("--dry-run", action="store_true", help="Show actions without modifying files")
    args = ap.parse_args()

    case_root = Path(args.case_root).resolve()
    if not case_root.exists():
        print(f"ERROR: case_root not found: {case_root}")
        return

    # Logging into top-level applog
    setup_logger(case_root / "applog")

    logging.info(f"Scanning for PDFs under: {case_root}")
    pdfs = find_pdfs(case_root)
    if not pdfs:
        logging.warning("No PDFs found. Nothing to do.")
        return

    candidate = best_pdf(pdfs, args.case_id)
    if candidate is None:
        logging.warning("No suitable PDF candidate identified.")
        return

    logging.info(f"Selected PDF: {candidate}")

    # Normalize name (fix .pdf.pdf, normalize case)
    fixed_name = normalize_pdf_suffix(candidate.name)
    if fixed_name != candidate.name:
        logging.info(f"Normalizing extension: '{candidate.name}' → '{fixed_name}'")
        if not args.dry_run:
            new_path = candidate.with_name(fixed_name)
            try:
                candidate.rename(new_path)
                candidate = new_path
            except Exception as e:
                logging.error(f"Rename failed: {e}")

    # Destination path
    misc_dir = case_root / f"{args.case_id} Misc"
    misc_dir.mkdir(parents=True, exist_ok=True)
    dest = misc_dir / f"{args.case_id}_TreatmentReport.pdf"

    # If destination exists, version safely
    if dest.exists():
        i = 2
        while True:
            alt = misc_dir / f"{args.case_id}_TreatmentReport_{i}.pdf"
            if not alt.exists():
                dest = alt
                break
            i += 1

    logging.info(f"→ Target: {dest}")

    if args.dry_run:
        logging.info("[DRY-RUN] Would move file.")
    else:
        try:
            # Use move (across disks ok)
            shutil.move(str(candidate), str(dest))
            logging.info(f"Moved PDF to: {dest}")
        except Exception as e:
            logging.error(f"Move failed: {e}")
            return

    # Optional: remove empty parent folders after move
    try:
        parent = candidate.parent
        while parent != case_root and parent.exists() and not any(parent.iterdir()):
            if args.dry_run:
                logging.info(f"[DRY-RUN] Would remove empty folder: {parent}")
                break
            logging.info(f"Removing empty folder: {parent}")
            parent.rmdir()
            parent = parent.parent
    except Exception as e:
        logging.warning(f"Could not prune empty directories: {e}")

    logging.info("PDF handling complete.")

if __name__ == "__main__":
    main()
