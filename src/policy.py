from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class RunPolicy:
    test_mode: bool
    allow_workspace_zips: bool
    legacy_filename_rules: bool
    dry_run: bool
    skip_mri: bool
    skip_tdc: bool
    clean_scratch: bool
    hash_outputs: bool
    date_shift_days: int


def policy_from_args_and_cfg(args: Any, cfg: Dict[str, Any]) -> RunPolicy:
    return RunPolicy(
        test_mode=cfg["test_mode"],
        allow_workspace_zips=cfg["allow_workspace_zips"],
        legacy_filename_rules=cfg["legacy_filename_rules"],
        dry_run=cfg["dry_run"],
        skip_mri=cfg["skip_mri"],
        skip_tdc=cfg["skip_tdc"],
        clean_scratch=cfg["clean_scratch"],
        hash_outputs=cfg["hash_outputs"],
        date_shift_days=cfg["date_shift_days"],
    )
