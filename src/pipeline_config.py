# PURPOSE: Load YAML/CLI config, normalize paths, and derive case layout defaults.
# INPUTS: Config file path and CLI arguments.
# OUTPUTS: Resolved config dict used by controller.
# NOTES: Records raw path strings for diagnostics.
from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

from src.logutil import ValidationError
from src.path_utils import sanitize_path_str

PATH_KEYS = {
    "root",
    "case_dir",
    "mri_input",
    "tdc_input",
    "pdf_input",
    "scratch",
    "log_dir",
    "mr_dir",
    "tdc_dir",
    "misc_dir",
    "manifest_dir",
    "localdb_path",
}

DEFAULTS: Dict[str, Any] = {
    "root": r"E:\Data_Clean",
    "case": None,
    "mri_input": None,
    "tdc_input": None,
    "pdf_input": None,
    "scratch": None,
    "scratch_policy": "local_temp",
    "clean_scratch": False,
    "date_shift_days": 137,
    "skip_mri": False,
    "skip_tdc": False,
    "log_dir": None,
    "log_level": "INFO",
    "run_id": None,
    "dry_run": False,
    "hash_outputs": False,
    "test_mode": False,
    "allow_workspace_zips": False,
    "legacy_filename_rules": False,
    "localdb_enabled": True,
    "localdb_check_only": False,
    "localdb_strict": True,
    "localdb_path": None,
    "unzip_inputs": False,
    "cleanup_enabled": True,
    "cleanup_dry_run": False,
    "cleanup_patterns": [
        "*.mat",
        "local.db-wal",
        "local.db-shm",
        "*.db-wal",
        "*.db-shm",
    ],
    "dicom_anon_enabled": False,
    "dicom_anon_mode": "stub",
    "peda_enabled": False,
    "peda_version": "v9.1.3",
    "peda_mode": "stub",
}

CANONICAL_LAYOUT = {
    "mr_dir_name": "MR DICOM",
    "tdc_dir_name": "TDC Sessions",
    "misc_dir_name": "Misc",
}

CASE_ID_RE = re.compile(r"(\d{3})[-_](\d{2})[-_](\d{3,})", re.IGNORECASE)


def _replace_tokens(value: str, mapping: Dict[str, str]) -> str:
    out = value
    for key, val in mapping.items():
        out = out.replace(f"{{{key}}}", val)
    return out


def _case_id_aliases(case_id: Optional[str]) -> List[str]:
    if not case_id:
        return []
    m = CASE_ID_RE.search(case_id)
    if not m:
        return [case_id]
    p1, p2, p3 = m.group(1), m.group(2), m.group(3)
    aliases = []
    for sep1 in ("_", "-"):
        for sep2 in ("_", "-"):
            aliases.append(f"{p1}{sep1}{p2}{sep2}{p3}")
    return list(dict.fromkeys(aliases))


def _sanitize_path_value(value: Optional[str], key: str, raw_paths: Dict[str, str]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        raw_paths.setdefault(key, value)
        try:
            return sanitize_path_str(value)
        except ValueError as exc:
            raise ValidationError(
                f"Invalid path for {key}: raw={value!r} error={exc}"
            ) from exc
    return str(value)


def _sanitize_paths(cfg: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(cfg)
    raw_paths = out.get("_raw_paths")
    raw_paths = dict(raw_paths) if isinstance(raw_paths, dict) else {}
    for key in PATH_KEYS:
        val = out.get(key)
        if val is None:
            continue
        if isinstance(val, Path):
            continue
        if isinstance(val, str):
            out[key] = _sanitize_path_value(val, key, raw_paths)
    if raw_paths:
        out["_raw_paths"] = raw_paths
    return out


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception as exc:
        raise ValidationError(f"YAML support not available: {exc}") from exc
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_config_file(path: Path) -> Dict[str, Any]:
    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        return _load_yaml(path)
    if suffix == ".json":
        return _load_json(path)

    try:
        return _load_yaml(path)
    except ValidationError:
        return _load_json(path)


def _apply_overrides(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for k, v in overrides.items():
        if v is not None:
            merged[k] = v
    return merged


def _flatten_nested(cfg: Dict[str, Any]) -> Dict[str, Any]:
    raw_paths = cfg.get("_raw_paths")
    raw_paths = dict(raw_paths) if isinstance(raw_paths, dict) else {}

    if not isinstance(cfg.get("case"), dict):
        flat = dict(cfg)
        for key in PATH_KEYS:
            val = flat.get(key)
            if isinstance(val, str):
                flat[key] = _sanitize_path_value(val, key, raw_paths)
        if raw_paths:
            flat["_raw_paths"] = raw_paths
        return flat

    case_block = cfg.get("case", {})
    inputs_block = cfg.get("inputs", {})
    run_block = cfg.get("run", {})
    logging_block = cfg.get("logging", {})
    localdb_block = cfg.get("localdb", {}) if isinstance(cfg.get("localdb"), dict) else {}
    metadata_block = cfg.get("metadata", {})
    pipeline_block = cfg.get("pipeline", {}) if isinstance(cfg.get("pipeline"), dict) else {}

    case_id = case_block.get("id")
    root = _sanitize_path_value(case_block.get("root"), "root", raw_paths)
    case_dir = _sanitize_path_value(case_block.get("dir"), "case_dir", raw_paths)

    mapping = {}
    if case_id:
        mapping["case_id"] = str(case_id)
        mapping["case.id"] = str(case_id)
    if root:
        mapping["case.root"] = str(root)

    if root and case_id:
        canonical_case_dir = str(Path(root) / str(case_id))
    else:
        canonical_case_dir = None

    if case_dir:
        case_dir = _replace_tokens(str(case_dir), mapping)
        if canonical_case_dir and Path(case_dir) != Path(canonical_case_dir):
            raise ValidationError(
                f"case.dir must match canonical layout: expected {canonical_case_dir}, got {case_dir}"
            )
    elif canonical_case_dir:
        case_dir = canonical_case_dir

    if case_dir:
        mapping["case_dir"] = str(case_dir)

    layout = case_block.get("layout", {}) if isinstance(case_block.get("layout"), dict) else {}
    for key, canonical in CANONICAL_LAYOUT.items():
        if key in layout and layout.get(key) != canonical:
            raise ValidationError(
                f"layout.{key} must be '{canonical}' for canonical layout"
            )
    mr_dir_name = CANONICAL_LAYOUT["mr_dir_name"]
    tdc_dir_name = CANONICAL_LAYOUT["tdc_dir_name"]
    misc_dir_name = CANONICAL_LAYOUT["misc_dir_name"]

    if case_dir:
        mr_dir = str(Path(case_dir) / _replace_tokens(str(mr_dir_name), mapping))
        tdc_dir = str(Path(case_dir) / _replace_tokens(str(tdc_dir_name), mapping))
        misc_dir = str(Path(case_dir) / _replace_tokens(str(misc_dir_name), mapping))
    else:
        mr_dir = tdc_dir = misc_dir = None

    log_dir = logging_block.get("dir")
    if log_dir:
        log_dir = _replace_tokens(str(log_dir), mapping)
        log_dir = _sanitize_path_value(log_dir, "log_dir", raw_paths)

    manifest_dir = logging_block.get("manifest_dir")
    if manifest_dir:
        manifest_dir = _replace_tokens(str(manifest_dir), mapping)
        manifest_dir = _sanitize_path_value(manifest_dir, "manifest_dir", raw_paths)

    manifest_name = logging_block.get("manifest_name")
    if manifest_name:
        manifest_name = _replace_tokens(str(manifest_name), mapping)

    run_id = metadata_block.get("run_id")
    if run_id == "auto":
        run_id = None

    scratch_policy = run_block.get("scratch", {}).get(
        "policy", cfg.get("scratch_policy", DEFAULTS["scratch_policy"])
    )
    scratch_val = run_block.get("scratch", {}).get("dir", cfg.get("scratch"))
    if isinstance(scratch_val, str):
        scratch_val = _sanitize_path_value(scratch_val, "scratch", raw_paths)

    localdb_path = localdb_block.get("path", cfg.get("localdb_path"))
    if isinstance(localdb_path, str):
        localdb_path = _sanitize_path_value(localdb_path, "localdb_path", raw_paths)

    cleanup_block = pipeline_block.get("cleanup", {}) if isinstance(pipeline_block.get("cleanup"), dict) else {}
    dicom_block = pipeline_block.get("dicom_anon", {}) if isinstance(pipeline_block.get("dicom_anon"), dict) else {}
    peda_block = pipeline_block.get("peda", {}) if isinstance(pipeline_block.get("peda"), dict) else {}

    cleanup_patterns = cleanup_block.get("patterns", cfg.get("cleanup_patterns"))
    if cleanup_patterns is None:
        cleanup_patterns = DEFAULTS["cleanup_patterns"]

    flat = {
        **cfg,
        "root": root or cfg.get("root"),
        "case": case_id or cfg.get("case"),
        "case_dir": case_dir,
        "mr_dir": mr_dir,
        "tdc_dir": tdc_dir,
        "misc_dir": misc_dir,
        "log_dir": log_dir or cfg.get("log_dir"),
        "manifest_dir": manifest_dir,
        "manifest_name": manifest_name,
        "run_id": run_id or cfg.get("run_id"),
        "scratch_policy": scratch_policy,
        "date_shift_days": run_block.get("anonymization", {}).get(
            "date_shift_days", cfg.get("date_shift_days", DEFAULTS["date_shift_days"])
        ),
        "clean_scratch": run_block.get("scratch", {}).get(
            "clean_on_success", cfg.get("clean_scratch", DEFAULTS["clean_scratch"])
        ),
        "scratch": scratch_val,
        "skip_mri": run_block.get("flags", {}).get(
            "skip_mri", cfg.get("skip_mri", DEFAULTS["skip_mri"])
        ),
        "skip_tdc": run_block.get("flags", {}).get(
            "skip_tdc", cfg.get("skip_tdc", DEFAULTS["skip_tdc"])
        ),
        "dry_run": run_block.get("flags", {}).get(
            "dry_run", cfg.get("dry_run", DEFAULTS["dry_run"])
        ),
        "test_mode": run_block.get("flags", {}).get(
            "test_mode", cfg.get("test_mode", DEFAULTS["test_mode"])
        ),
        "allow_workspace_zips": run_block.get("flags", {}).get(
            "allow_workspace_zips",
            cfg.get("allow_workspace_zips", DEFAULTS["allow_workspace_zips"]),
        ),
        "legacy_filename_rules": run_block.get("flags", {}).get(
            "legacy_filename_rules",
            cfg.get("legacy_filename_rules", DEFAULTS["legacy_filename_rules"]),
        ),
        "hash_outputs": run_block.get(
            "hash_outputs", cfg.get("hash_outputs", DEFAULTS["hash_outputs"])
        ),
        "localdb_enabled": localdb_block.get(
            "enabled", cfg.get("localdb_enabled", DEFAULTS["localdb_enabled"])
        ),
        "localdb_check_only": localdb_block.get(
            "check_only", cfg.get("localdb_check_only", DEFAULTS["localdb_check_only"])
        ),
        "localdb_strict": localdb_block.get(
            "strict", cfg.get("localdb_strict", DEFAULTS["localdb_strict"])
        ),
        "localdb_path": localdb_path,
        "unzip_inputs": pipeline_block.get(
            "unzip_inputs", cfg.get("unzip_inputs", DEFAULTS["unzip_inputs"])
        ),
        "cleanup_enabled": cleanup_block.get(
            "enabled", cfg.get("cleanup_enabled", DEFAULTS["cleanup_enabled"])
        ),
        "cleanup_dry_run": cleanup_block.get(
            "dry_run", cfg.get("cleanup_dry_run", DEFAULTS["cleanup_dry_run"])
        ),
        "cleanup_patterns": cleanup_patterns,
        "dicom_anon_enabled": dicom_block.get(
            "enabled", cfg.get("dicom_anon_enabled", DEFAULTS["dicom_anon_enabled"])
        ),
        "dicom_anon_mode": dicom_block.get(
            "mode", cfg.get("dicom_anon_mode", DEFAULTS["dicom_anon_mode"])
        ),
        "peda_enabled": peda_block.get(
            "enabled", cfg.get("peda_enabled", DEFAULTS["peda_enabled"])
        ),
        "peda_version": peda_block.get(
            "version", cfg.get("peda_version", DEFAULTS["peda_version"])
        ),
        "peda_mode": peda_block.get(
            "mode", cfg.get("peda_mode", DEFAULTS["peda_mode"])
        ),
        "log_level": logging_block.get(
            "level_console", cfg.get("log_level", DEFAULTS["log_level"])
        ),
        "inputs": inputs_block,
        "metadata": metadata_block,
        "logging": logging_block,
        "_raw_paths": raw_paths if raw_paths else cfg.get("_raw_paths"),
    }
    return flat


def _expand_vars(cfg: Dict[str, Any]) -> Dict[str, Any]:
    pattern = re.compile(r"\$(\w+)|\$\{([^}]+)\}")
    out = dict(cfg)

    def repl(match: re.Match) -> str:
        key = match.group(1) or match.group(2)
        val = out.get(key)
        return str(val) if val is not None else match.group(0)

    for k, v in out.items():
        if isinstance(v, str):
            out[k] = pattern.sub(repl, v)
    return out


def _expand_templates(cfg: Dict[str, Any]) -> Dict[str, Any]:
    mapping = {
        "case_id": str(cfg.get("case")) if cfg.get("case") is not None else "",
        "case.id": str(cfg.get("case")) if cfg.get("case") is not None else "",
        "case.root": str(cfg.get("root")) if cfg.get("root") is not None else "",
        "case_dir": str(cfg.get("case_dir")) if cfg.get("case_dir") is not None else "",
        "run_id": str(cfg.get("run_id")) if cfg.get("run_id") is not None else "",
    }
    out = dict(cfg)
    for k, v in out.items():
        if isinstance(v, str):
            out[k] = _replace_tokens(v, mapping)
    return out


def _candidate_info(paths: List[Path]) -> List[Dict[str, Any]]:
    info = []
    for p in paths:
        try:
            st = p.stat()
            info.append(
                {
                    "path": str(p),
                    "size_bytes": st.st_size,
                    "mtime": datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds"),
                }
            )
        except Exception:
            info.append({"path": str(p), "size_bytes": None, "mtime": None})
    return info


def _rank_candidates(matches: List[Path], pick: str) -> List[Path]:
    if pick == "newest":
        return sorted(matches, key=lambda p: p.stat().st_mtime, reverse=True)
    if pick == "largest":
        return sorted(matches, key=lambda p: p.stat().st_size, reverse=True)
    return sorted(matches, key=lambda p: str(p).lower())


def _resolve_auto_inputs(cfg: Dict[str, Any]) -> Dict[str, Any]:
    inputs = cfg.get("inputs") or {}
    if not isinstance(inputs, dict):
        return cfg

    mode = inputs.get("mode", "auto")
    legacy = bool(cfg.get("legacy_filename_rules"))
    if mode == "auto" and not legacy:
        if cfg.get("mri_input") or cfg.get("tdc_input") or cfg.get("pdf_input"):
            return cfg
        raise ValidationError(
            "Auto-discovery is disabled when legacy_filename_rules is false; "
            "use inputs.mode=explicit or set run.flags.legacy_filename_rules=true"
        )
    if mode == "explicit":
        explicit = inputs.get("explicit", {}) if isinstance(inputs.get("explicit"), dict) else {}
        if cfg.get("mri_input") is None:
            cfg["mri_input"] = _sanitize_path_value(
                explicit.get("mri_zip"), "mri_input", cfg.setdefault("_raw_paths", {})
            )
        if cfg.get("tdc_input") is None:
            cfg["tdc_input"] = _sanitize_path_value(
                explicit.get("tdc_zip"), "tdc_input", cfg.setdefault("_raw_paths", {})
            )
        if cfg.get("pdf_input") is None:
            pdf_val = (
                explicit.get("pdf_input")
                or explicit.get("pdf")
                or explicit.get("treatment_report")
            )
            cfg["pdf_input"] = _sanitize_path_value(
                pdf_val, "pdf_input", cfg.setdefault("_raw_paths", {})
            )
        return cfg

    search = inputs.get("search", {}) if isinstance(inputs.get("search"), dict) else {}
    roots = search.get("roots", [])
    mri_globs = search.get("mri_zip_globs", [])
    tdc_globs = search.get("tdc_zip_globs", [])
    pick = search.get("pick", "newest")

    case_dir = Path(cfg["case_dir"]) if cfg.get("case_dir") else None
    case_aliases = [a.lower() for a in _case_id_aliases(cfg.get("case"))] if legacy else []
    resolved_roots: List[Path] = []
    if not roots and case_dir:
        roots = [
            "{case_dir}\\incoming",
            "{case_dir}",
            "{case.root}\\incoming\\{case_id}",
        ]
    if legacy and not mri_globs:
        mri_globs = ["*MRI*.zip", "MRI_*.zip", "MR_*.zip", "*MR*.zip", "*DICOM*.zip"]
    for r in roots:
        if not isinstance(r, str):
            continue
        r = _replace_tokens(
            r,
            {
                "case_id": str(cfg.get("case") or ""),
                "case.id": str(cfg.get("case") or ""),
                "case.root": str(cfg.get("root") or ""),
                "case_dir": str(cfg.get("case_dir") or ""),
            },
        )
        try:
            r = _sanitize_path_value(r, "search_root", cfg.setdefault("_raw_paths", {}))
        except ValidationError:
            raise
        rp = Path(r)
        if not rp.is_absolute() and case_dir:
            rp = case_dir / rp
        resolved_roots.append(rp)

    def find_match(globs: List[str]) -> Tuple[Optional[Path], Dict[str, Any]]:
        all_matches: List[Path] = []
        for root in resolved_roots:
            if not root.exists():
                continue
            for pat in globs:
                pat = _replace_tokens(
                    pat,
                    {
                        "case_id": str(cfg.get("case") or ""),
                        "case.id": str(cfg.get("case") or ""),
                    },
                )
                all_matches.extend(sorted(root.glob(pat)))

        filtered = all_matches
        filtered_by_case = False
        if case_aliases:
            alias_matches = [
                p
                for p in all_matches
                if any(alias in p.name.lower() for alias in case_aliases)
            ]
            if alias_matches:
                filtered = alias_matches
                filtered_by_case = True

        ranked = _rank_candidates(filtered, pick) if filtered else []
        selected = ranked[0] if ranked else None
        info = {
            "pick": pick,
            "candidates": _candidate_info(ranked[:5]),
            "selected": str(selected) if selected else None,
            "filtered_by_case_id": filtered_by_case,
            "case_aliases": case_aliases,
        }
        return selected, info

    if cfg.get("mri_input") is None:
        mri_match, mri_info = find_match(mri_globs)
        cfg.setdefault("auto_discovery", {})
        cfg["auto_discovery"]["mri"] = mri_info
        if mri_match:
            cfg["mri_input"] = mri_match
    if cfg.get("tdc_input") is None:
        tdc_match, tdc_info = find_match(tdc_globs)
        cfg.setdefault("auto_discovery", {})
        cfg["auto_discovery"]["tdc"] = tdc_info
        if tdc_match:
            cfg["tdc_input"] = tdc_match

    return cfg


def _resolve_scratch(cfg: Dict[str, Any]) -> Dict[str, Any]:
    scratch = cfg.get("scratch")
    policy = cfg.get("scratch_policy", DEFAULTS["scratch_policy"])
    if scratch is None:
        if policy == "local_temp":
            base = Path(os.environ.get("TEMP") or tempfile.gettempdir())
            scratch = base / "PEDA" / str(cfg.get("case")) / str(cfg.get("run_id"))
        elif policy == "case_root":
            case_dir = cfg.get("case_dir")
            if case_dir:
                scratch = Path(case_dir) / "scratch"
        else:
            raise ValidationError(
                f"scratch_policy must be 'local_temp' or 'case_root', got {policy}"
            )
    cfg["scratch"] = scratch
    cfg["scratch_policy"] = policy
    return cfg


def _normalize_paths(cfg: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(cfg)
    for key in PATH_KEYS:
        val = out.get(key)
        if val is None:
            continue
        out[key] = Path(val)
    return out


def _validate_config(cfg: Dict[str, Any]) -> None:
    if not cfg.get("case"):
        raise ValidationError("Config missing required value: case")
    if not isinstance(cfg.get("case"), str):
        raise ValidationError("case must be a string")
    if not cfg.get("root"):
        raise ValidationError("Config missing required value: root")
    if not isinstance(cfg.get("date_shift_days"), int):
        raise ValidationError("date_shift_days must be an integer")
    for key in (
        "clean_scratch",
        "skip_mri",
        "skip_tdc",
        "dry_run",
        "hash_outputs",
        "test_mode",
        "allow_workspace_zips",
        "legacy_filename_rules",
        "localdb_enabled",
        "localdb_check_only",
        "localdb_strict",
        "unzip_inputs",
        "cleanup_enabled",
        "cleanup_dry_run",
        "dicom_anon_enabled",
        "peda_enabled",
    ):
        val = cfg.get(key)
        if not isinstance(val, bool):
            raise ValidationError(f"{key} must be a boolean")
    if not isinstance(cfg.get("cleanup_patterns"), list):
        raise ValidationError("cleanup_patterns must be a list")
    if not isinstance(cfg.get("dicom_anon_mode"), str):
        raise ValidationError("dicom_anon_mode must be a string")
    if cfg.get("dicom_anon_mode") not in ("existing", "stub"):
        raise ValidationError("dicom_anon_mode must be 'existing' or 'stub'")
    if not isinstance(cfg.get("peda_version"), str):
        raise ValidationError("peda_version must be a string")
    if not isinstance(cfg.get("peda_mode"), str):
        raise ValidationError("peda_mode must be a string")
    if cfg.get("peda_mode") not in ("stub", "matlab"):
        raise ValidationError("peda_mode must be 'stub' or 'matlab'")
    if cfg.get("log_level") is None:
        raise ValidationError("log_level must be set")
    policy = cfg.get("scratch_policy", DEFAULTS["scratch_policy"])
    if policy not in ("local_temp", "case_root"):
        raise ValidationError("scratch_policy must be 'local_temp' or 'case_root'")


def resolve_config(
    *,
    config_path: Optional[Path | str],
    cli_overrides: Dict[str, Any],
) -> Tuple[Dict[str, Any], str]:
    cli_raw_paths: Dict[str, str] = {}
    if cli_overrides:
        sanitized_overrides = dict(cli_overrides)
        for key, val in cli_overrides.items():
            if key in PATH_KEYS and isinstance(val, str):
                cli_raw_paths[key] = val
                sanitized_overrides[key] = _sanitize_path_value(val, key, cli_raw_paths)
        cli_overrides = sanitized_overrides

    if config_path:
        cfg_path = Path(sanitize_path_str(str(config_path)))
        if not cfg_path.exists():
            raise ValidationError(f"Config file not found: {cfg_path}")
        cfg_data = _load_config_file(cfg_path)
    else:
        cfg_data = {}

    cfg_data = _flatten_nested(cfg_data)
    merged = _apply_overrides(DEFAULTS, cfg_data)
    merged = _apply_overrides(merged, cli_overrides)

    if merged.get("case_dir") is None and merged.get("root") and merged.get("case"):
        merged["case_dir"] = str(Path(merged["root"]) / str(merged["case"]))

    if merged.get("run_id") in ("", None):
        merged["run_id"] = datetime.now().strftime("%Y%m%d_%H%M%S")

    merged = _expand_vars(merged)
    merged = _expand_templates(merged)
    merged = _resolve_scratch(merged)
    merged = _resolve_auto_inputs(merged)
    if cli_raw_paths:
        raw_paths = merged.get("_raw_paths")
        raw_paths = dict(raw_paths) if isinstance(raw_paths, dict) else {}
        for key, val in cli_raw_paths.items():
            raw_paths.setdefault(key, val)
        merged["_raw_paths"] = raw_paths
    merged = _normalize_paths(merged)

    _validate_config(merged)
    return merged, str(merged["run_id"])


def add_bool_arg(parser: argparse.ArgumentParser, name: str, help_text: str) -> None:
    flag = f"--{name.replace('_', '-')}"
    neg_flag = f"--no-{name.replace('_', '-')}"
    parser.add_argument(flag, dest=name, action="store_true")
    parser.add_argument(neg_flag, dest=name, action="store_false")
    parser.set_defaults(**{name: None})
