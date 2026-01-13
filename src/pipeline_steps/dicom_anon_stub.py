# PURPOSE: Placeholder hook for DICOM anonymization.
# INPUTS: MR DICOM directory, case ID, and optional rule map.
# OUTPUTS: Structured status dict; no file changes.
# NOTES: This is a stub only and does not claim compliance.
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Any, Optional


def run_dicom_anon_stub(
    mr_dir: Path, case_id: str, rules: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    log = logging.getLogger(__name__)
    mr_dir = Path(mr_dir)
    rules = rules or {}
    if not mr_dir.exists():
        log.warning("DICOM anonymization stub skipped; MR dir missing: %s", mr_dir)
        return {
            "status": "skipped",
            "reason": "missing_mr_dir",
            "mr_dir": str(mr_dir),
            "case_id": case_id,
            "rules_count": len(rules),
        }
    log.info(
        "DICOM anonymization stub: not yet implemented (case=%s mr_dir=%s rules=%s)",
        case_id,
        mr_dir,
        len(rules),
    )
    return {
        "status": "stub",
        "mr_dir": str(mr_dir),
        "case_id": case_id,
        "rules_count": len(rules),
    }
