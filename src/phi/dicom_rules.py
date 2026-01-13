# PURPOSE: Define a clear, testable DICOM tag rewrite policy for future anonymization.
# INPUTS: Case ID for rule materialization.
# OUTPUTS: Dict of tag -> replacement value placeholders.
# NOTES: This is a policy map only; no DICOM rewriting occurs here.
from __future__ import annotations

from typing import Dict


RULE_TEMPLATES: Dict[str, str] = {
    "(0010,0010)": "{case_id}",  # PatientName
    "(0010,0020)": "{case_id}",  # PatientID
    "(0010,0030)": "19000101",   # PatientBirthDate
    "(0010,0040)": "O",          # PatientSex
    "(0020,0010)": "1",          # StudyID
    "(0008,0050)": "Accession",  # AccessionNumber
    "(0008,0080)": "Institution",
    "(0008,0081)": "Address",
    "(0008,0090)": "ReferringPhysician",
    "(0020,4000)": "PMI",
}


def build_dicom_rules(case_id: str) -> Dict[str, str]:
    """
    Materialize tag rewrite rules for a given case ID.
    This does not modify files; it is a policy map only.
    """
    return {tag: value.replace("{case_id}", case_id) for tag, value in RULE_TEMPLATES.items()}
