from __future__ import annotations

from typing import Any, Dict, List

PERM_CASES: List[Dict[str, Any]] = [
    {
        "mri": "MRI_093_01-098.zip",
        "tdc": "TDC_093_01-098.zip",
        "pdf": "Treatment Report (v2) 093_01-098.pdf",
        "method": "cli",
    },
    {
        "mri": "MR_093-01-098.ZIP",
        "tdc": "tdc-093-01_098.ZIP.ZIP",
        "pdf": "Treatment Report (v2) 093_01-098.pdf",
        "method": "cli",
    },
    {
        "mri": "scan.export.MR_093_01_098.v2.zip",
        "tdc": "MR_TDC_093_01-098.zip",
        "pdf": "Treatment Report (v2) 093_01-098.pdf",
        "method": "cli",
    },
    {
        "mri": "tdc_MR_confuser_093_01-098.zip.zip",
        "tdc": "TDC_093_01-098.zip",
        "pdf": "Treatment Report (v2) 093_01-098.pdf",
        "method": "cli",
    },
    {
        "mri": "MRI_093_01-098.zip",
        "tdc": "tdc-093-01_098.ZIP.ZIP",
        "pdf": "Treatment Report (v2) 093_01-098.pdf",
        "method": "yaml",
    },
    {
        "mri": "MR_093-01-098.ZIP",
        "tdc": "MR_TDC_093_01-098.zip",
        "pdf": "Treatment Report (v2) 093_01-098.pdf",
        "method": "yaml",
    },
    {
        "mri": "scan.export.MR_093_01_098.v2.zip",
        "tdc": "TDC_093_01-098.zip",
        "pdf": "Treatment Report (v2) 093_01-098.pdf",
        "method": "yaml",
    },
    {
        "mri": "tdc_MR_confuser_093_01-098.zip.zip",
        "tdc": "MR_TDC_093_01-098.zip",
        "pdf": "Treatment Report (v2) 093_01-098.pdf",
        "method": "yaml",
    },
]

PATH_VARIANTS = ["raw", "quoted_padded"]

NEG_CASES: List[Dict[str, Any]] = [
    {
        "name": "wrong_extension",
        "expect": ["must be a .zip"],
        "mri_name": "bad_mri.rar",
        "path_type": "file",
    },
    {
        "name": "missing_file",
        "expect": ["not found"],
        "mri_name": "missing.zip",
        "path_type": "missing",
    },
    {
        "name": "empty_string",
        "expect": ["empty path", "invalid path"],
        "mri_name": "mri_ok.zip",
        "path_type": "empty",
    },
    {
        "name": "directory_path",
        "expect": ["not found", "directory"],
        "mri_name": "mri_ok.zip",
        "path_type": "dir",
    },
]
