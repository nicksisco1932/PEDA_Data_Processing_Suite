# PURPOSE: Logging setup, timing helpers, and pipeline error types.
# INPUTS: Case/run identifiers, log directory, log level.
# OUTPUTS: Configured logger, log file path, timing status records.
# NOTES: Uses Rich console logging when available.
from __future__ import annotations

import hashlib
import logging
import shutil
from logging.handlers import RotatingFileHandler
from pathlib import Path
from time import monotonic
from typing import Any, Dict, Optional, Tuple


class PipelineError(Exception):
    code = 3


class ValidationError(PipelineError):
    code = 2


class ProcessingError(PipelineError):
    code = 3


class UnexpectedError(PipelineError):
    code = 4


def init_logger(
    *,
    case: str,
    run_id: str,
    log_dir: Path,
    log_level: str = "INFO",
) -> Tuple[logging.Logger, Path, bool]:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{case}__{run_id}.log"

    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False

    rich_available = False
    try:
        from rich.logging import RichHandler  # type: ignore

        console_handler = RichHandler(level=log_level, show_time=False, show_path=False)
        rich_available = True
    except Exception:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))

    file_handler = RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )

    if not isinstance(console_handler, logging.Handler) or console_handler.formatter is None:
        console_handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.debug("Logger initialized for case=%s run_id=%s", case, run_id)
    return logger, log_file, rich_available


class StepTimer:
    def __init__(
        self,
        *,
        logger: logging.Logger,
        step_name: str,
        results: Dict[str, Any],
        status_mgr: Optional["StatusManager"] = None,
    ):
        self.logger = logger
        self.step_name = step_name
        self.results = results
        self.status_mgr = status_mgr
        self.start = 0.0

    def __enter__(self):
        self.start = monotonic()
        self.logger.info("START %s", self.step_name)
        if self.status_mgr:
            self.status_mgr.update(self.step_name)
        return self

    def __exit__(self, exc_type, exc, tb):
        duration = monotonic() - self.start
        if exc is None:
            self.logger.info("PASS  %s (%.2fs)", self.step_name, duration)
            self.results[self.step_name] = {
                "status": "PASS",
                "duration_s": round(duration, 2),
                "error": None,
            }
            return False
        self.logger.error("FAIL  %s (%.2fs) %s", self.step_name, duration, exc)
        self.results[self.step_name] = {
            "status": "FAIL",
            "duration_s": round(duration, 2),
            "error": str(exc),
        }
        return False


class StatusManager:
    def __init__(self):
        self._status = None

    def __enter__(self):
        try:
            from rich.console import Console  # type: ignore
            from rich.status import Status  # type: ignore

            console = Console()
            self._status = Status("", console=console)
            self._status.__enter__()
        except Exception:
            self._status = None
        return self

    def update(self, text: str) -> None:
        if self._status:
            self._status.update(f"{text}...")

    def __exit__(self, exc_type, exc, tb):
        if self._status:
            self._status.__exit__(exc_type, exc, tb)
        self._status = None


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def copy_with_integrity(
    src: Path,
    dst: Path,
    *,
    retries: int = 2,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    log = logger or logging.getLogger(__name__)
    attempts = 0
    last: Dict[str, Any] = {}
    for attempt in range(retries + 1):
        attempts = attempt + 1
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            try:
                dst.unlink()
            except Exception:
                pass
        shutil.copy2(src, dst)
        src_size = src.stat().st_size
        dst_size = dst.stat().st_size if dst.exists() else None
        src_hash = sha256_file(src)
        dst_hash = sha256_file(dst) if dst.exists() else None
        ok = (src_size == dst_size) and (src_hash == dst_hash)
        last = {
            "src": str(src),
            "dst": str(dst),
            "src_size": src_size,
            "dst_size": dst_size,
            "src_sha256": src_hash,
            "dst_sha256": dst_hash,
            "attempts": attempts,
            "ok": ok,
        }
        if ok:
            log.info(
                "Copy verified: %s -> %s (size=%s sha256=%s)",
                src,
                dst,
                src_size,
                src_hash,
            )
            return last
        log.warning(
            "Copy integrity mismatch (attempt %s/%s): src=%s dst=%s src_sha256=%s dst_sha256=%s",
            attempts,
            retries + 1,
            src,
            dst,
            src_hash,
            dst_hash,
        )

    raise ValidationError(
        "Integrity check failed; use local scratch or re-copy inputs; storage I/O unstable. "
        f"src={last.get('src')} dst={last.get('dst')} "
        f"src_sha256={last.get('src_sha256')} dst_sha256={last.get('dst_sha256')}"
    )
