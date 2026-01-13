# PURPOSE: Tests for TDC log discovery/copy behavior.
# INPUTS: tmp_path case trees with fake log files.
# OUTPUTS: Assertions on copied log location, hash verification, and cleanup.
from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from src.logutil import ProcessingError
from src.pipeline_steps import applog_step
from src.pipeline_steps.applog_step import install_tdc_log, sha256_file


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_extracted_logs_copied_and_pruned(tmp_path: Path) -> None:
    case_id = "093_01-098"
    case_root = tmp_path / case_id
    search_root = tmp_path / "tdc_unzipped"
    log_dir = search_root / "Logs"
    log_file = log_dir / "Tdc.2025_11_05.log"
    _write_file(log_file, "alpha")
    old_log = case_root / "Misc" / "Logs" / "Tdc.2025_11_01.log"
    _write_file(old_log, "old")

    src_hash = sha256_file(log_file)
    result = install_tdc_log(case_root, case_id, search_roots=[search_root])

    dest_path = case_root / "Misc" / "Logs" / f"{case_id} Tdc.2025_11_05.log"
    assert dest_path.is_file()
    assert sha256_file(dest_path) == src_hash
    assert result["src_sha256"] == result["dst_sha256"]
    assert not old_log.exists()
    assert not (case_root / "TDC Sessions" / "applog").exists()


def test_txt_log_accepted(tmp_path: Path) -> None:
    case_id = "093_01-098"
    case_root = tmp_path / case_id
    search_root = tmp_path / "tdc_unzipped"
    log_dir = search_root / "Logs"
    log_file = log_dir / "Tdc.2025_11_05.txt"
    _write_file(log_file, "bravo")

    result = install_tdc_log(case_root, case_id, search_roots=[search_root])
    dest_path = case_root / "Misc" / "Logs" / f"{case_id} Tdc.2025_11_05.log"
    assert dest_path.is_file()
    assert sha256_file(dest_path) == result["src_sha256"]
    assert result["status"] == "copied"


def test_search_root_wins_over_case_logs(tmp_path: Path) -> None:
    case_id = "093_01-098"
    case_root = tmp_path / case_id
    search_root = tmp_path / "tdc_unzipped"
    extracted_log = search_root / "Logs" / "Tdc.2025_11_06.log"
    case_log = case_root / "Misc" / "Logs" / "Tdc.2025_11_05.log"
    _write_file(extracted_log, "extracted")
    _write_file(case_log, "case")

    result = install_tdc_log(case_root, case_id, search_roots=[search_root])
    dest_path = Path(result["dest_path"])
    assert dest_path.name == f"{case_id} Tdc.2025_11_06.log"
    assert not case_log.exists()


def test_candidate_preference_log_and_mtime(tmp_path: Path) -> None:
    case_id = "093_01-098"
    case_root = tmp_path / case_id
    search_root = tmp_path / "tdc_unzipped"
    log_dir = search_root / "Logs"
    log_old = log_dir / "Tdc.2025_11_01.log"
    log_new = log_dir / "Tdc.2025_11_02.log"
    txt_new = log_dir / "Tdc.2025_11_03.txt"
    _write_file(log_old, "old")
    _write_file(log_new, "new")
    _write_file(txt_new, "txt")

    now = time.time()
    os.utime(log_old, (now - 300, now - 300))
    os.utime(log_new, (now - 200, now - 200))
    os.utime(txt_new, (now - 100, now - 100))

    result = install_tdc_log(case_root, case_id, search_roots=[search_root])
    dest_path = Path(result["dest_path"])
    assert dest_path.name == f"{case_id} Tdc.2025_11_02.log"


def test_hash_mismatch_aborts_and_keeps_source_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    case_id = "093_01-098"
    case_root = tmp_path / case_id
    search_root = tmp_path / "tdc_unzipped"
    log_dir = search_root / "Logs"
    log_file = log_dir / "Tdc.2025_11_05.log"
    _write_file(log_file, "data")

    dest_path = case_root / "Misc" / "Logs" / f"{case_id} Tdc.2025_11_05.log"
    original_sha256 = applog_step.sha256_file

    def fake_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
        if path.resolve() == dest_path.resolve():
            return "bad"
        return original_sha256(path, chunk_size)

    monkeypatch.setattr(applog_step, "sha256_file", fake_sha256)

    with pytest.raises(ProcessingError):
        applog_step.install_tdc_log(case_root, case_id, search_roots=[search_root])

    assert not dest_path.exists()
    assert log_dir.exists()


def test_no_logs_skips(tmp_path: Path) -> None:
    case_id = "093_01-098"
    case_root = tmp_path / case_id
    search_root = tmp_path / "tdc_unzipped"
    search_root.mkdir(parents=True, exist_ok=True)

    result = install_tdc_log(case_root, case_id, search_roots=[search_root])
    assert result["status"] == "skipped"
    assert result["reason"] == "no_log_found"
