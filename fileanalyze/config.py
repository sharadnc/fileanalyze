"""Environment-backed configuration for File Analyze."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import dotenv_values


@dataclass(slots=True)
class AppConfig:
    """
    Purpose:
        Hold validated runtime configuration loaded from `../venv/.env`.

    Internal Logic:
        1. Reads all required keys from a single environment file.
        2. Optionally allows OS environment overrides if enabled.
        3. Exposes strongly typed values to every service/callback.

    Example invocation:
        config = load_config()
    """

    host: str
    port: int
    debug: bool
    output_root: Path
    state_root: Path
    cache_root: Path
    temp_root: Path
    max_upload_mb: int
    worker_count: int
    log_level: str
    profile_topn: int
    max_preview_rows: int
    enable_env_override: bool
    session_ttl_hours: int
    run_ttl_hours: int
    retention_sweep_minutes: int


def _to_bool(value: str) -> bool:
    """
    Purpose:
        Convert common string values into booleans.

    Internal Logic:
        1. Trims and lowercases the source value.
        2. Matches accepted true/false tokens.
        3. Raises clear errors when conversion is not valid.

    Example invocation:
        enabled = _to_bool("true")
    """

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value}")


def _required(config_map: dict[str, str], key: str) -> str:
    """
    Purpose:
        Fetch required key values with fail-fast validation.

    Internal Logic:
        1. Looks up the provided key in the merged config map.
        2. Ensures the value exists and is non-empty.
        3. Raises a descriptive error when missing.

    Example invocation:
        host = _required(config_map, "FILEANALYZE_HOST")
    """

    value = config_map.get(key, "").strip()
    if not value:
        raise ValueError(f"Missing required config key: {key}")
    return value


def _project_root() -> Path:
    """
    Purpose:
        Resolve repository root path from package location.

    Internal Logic:
        1. Starts from the current file location.
        2. Moves one level up to the workspace root.
        3. Returns an absolute normalized path.

    Example invocation:
        root = _project_root()
    """

    return Path(__file__).resolve().parent.parent


def load_config() -> AppConfig:
    """
    Purpose:
        Load and validate all runtime configuration values.

    Internal Logic:
        1. Reads `../venv/.env` from the project root.
        2. Optionally overlays OS environment variables.
        3. Validates and casts all required values into `AppConfig`.
        4. Ensures runtime folders exist for cross-platform execution.

    Example invocation:
        cfg = load_config()
    """

    root = _project_root()
    env_path = root / "venv" / ".env"
    if not env_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {env_path}")

    file_map = {key: (value or "") for key, value in dotenv_values(env_path).items()}
    enable_override = _to_bool(file_map.get("FILEANALYZE_ENABLE_ENV_OVERRIDE", "false"))
    merged = dict(file_map)
    if enable_override:
        for key, value in os.environ.items():
            if key.startswith("FILEANALYZE_"):
                merged[key] = value

    config = AppConfig(
        host=_required(merged, "FILEANALYZE_HOST"),
        port=int(_required(merged, "FILEANALYZE_PORT")),
        debug=_to_bool(_required(merged, "FILEANALYZE_DEBUG")),
        output_root=(root / _required(merged, "FILEANALYZE_OUTPUT_ROOT")).resolve(),
        state_root=(root / _required(merged, "FILEANALYZE_STATE_ROOT")).resolve(),
        cache_root=(root / _required(merged, "FILEANALYZE_CACHE_ROOT")).resolve(),
        temp_root=(root / _required(merged, "FILEANALYZE_TEMP_ROOT")).resolve(),
        max_upload_mb=int(_required(merged, "FILEANALYZE_MAX_UPLOAD_MB")),
        worker_count=int(_required(merged, "FILEANALYZE_WORKER_COUNT")),
        log_level=_required(merged, "FILEANALYZE_LOG_LEVEL"),
        profile_topn=int(_required(merged, "FILEANALYZE_PROFILE_TOPN")),
        max_preview_rows=int(_required(merged, "FILEANALYZE_MAX_PREVIEW_ROWS")),
        enable_env_override=enable_override,
        session_ttl_hours=int(_required(merged, "FILEANALYZE_SESSION_TTL_HOURS")),
        run_ttl_hours=int(_required(merged, "FILEANALYZE_RUN_TTL_HOURS")),
        retention_sweep_minutes=int(_required(merged, "FILEANALYZE_RETENTION_SWEEP_MINUTES")),
    )

    # Runtime folders are proactively created so users can run in fresh environments.
    for folder in [config.output_root, config.state_root, config.cache_root, config.temp_root]:
        folder.mkdir(parents=True, exist_ok=True)
    return config

