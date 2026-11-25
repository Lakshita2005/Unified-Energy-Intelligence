"""Centralised, environment-driven application settings.

All runtime configuration is read from environment variables, optionally via a
``.env`` file at the project root. Nothing sensitive has a default value:
database credentials must be supplied explicitly through the environment.

Environment variable names are the upper-cased field names, e.g. the field
``db_host`` is populated from ``DB_HOST``.

Usage::

    from config.settings import get_settings

    settings = get_settings()
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, SecretStr, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent

Environment = Literal["development", "test", "staging", "production"]
LogFormat = Literal["text", "json"]
RawStorageBackendName = Literal["local"]

_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
_SCHEMA_NAME_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")


class Settings(BaseSettings):
    """Typed application settings populated from environment variables."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        validate_default=True,
    )

    # --- Application -------------------------------------------------------
    app_name: str = "price-intelligence-ingestion"
    app_env: Environment = "development"

    # --- PostgreSQL (no credential defaults by design) ---------------------
    db_host: str | None = None
    db_port: int = Field(default=5432, ge=1, le=65535)
    db_name: str | None = None
    db_user: str | None = None
    db_password: SecretStr | None = None
    db_driver: str = "postgresql+psycopg"
    db_schema: str = "pi"
    db_sslmode: str | None = None
    db_connect_timeout_seconds: int = Field(default=10, ge=1)
    db_pool_size: int = Field(default=5, ge=1)
    db_max_overflow: int = Field(default=10, ge=0)
    db_pool_timeout_seconds: int = Field(default=30, ge=1)
    db_pool_recycle_seconds: int = Field(default=1800, ge=-1)
    db_echo: bool = False

    # --- Logging -----------------------------------------------------------
    log_level: str = "INFO"
    log_format: LogFormat = "text"
    log_dir: Path = Path("logs")
    log_to_file: bool = True
    # Console (stdout) logging. Services set LOG_TO_CONSOLE=false so the service wrapper's stdout capture
    # does not duplicate the rotating log file.
    log_to_console: bool = True
    log_file_name: str = "ingestion.log"
    log_max_bytes: int = Field(default=10 * 1024 * 1024, ge=1024)
    log_backup_count: int = Field(default=5, ge=0)

    # --- HTTP client -------------------------------------------------------
    http_timeout_seconds: float = Field(default=30.0, gt=0)
    http_connect_timeout_seconds: float = Field(default=10.0, gt=0)
    http_max_retries: int = Field(default=3, ge=0)
    http_backoff_factor_seconds: float = Field(default=1.0, ge=0)
    http_backoff_max_seconds: float = Field(default=60.0, ge=0)
    http_user_agent: str = "price-intelligence-ingestion/0.1"
    http_verify_ssl: bool = True

    # --- Open-Meteo --------------------------------------------------------
    # "free": https://api.open-meteo.com / archive-api.open-meteo.com, no API key, NON-COMMERCIAL use only,
    #         fewer than 10,000 calls a day, data under CC BY 4.0 (attribution required).
    # "commercial": paid plan; requires OPEN_METEO_API_KEY (the pipeline then uses the customer- hosts).
    # The scheduler refuses Open-Meteo schedules when plan and key disagree, so the endpoint never changes
    # silently.
    open_meteo_plan: Literal["free", "commercial"] = "free"
    open_meteo_daily_call_limit: int = Field(default=10000, ge=1)
    # Only for the commercial plan. Never logged.
    open_meteo_api_key: SecretStr | None = None
    weather_locations_file: Path = Path("config/weather_locations.toml")

    # --- NPP (National Power Portal) ---------------------------------------
    # Base of the undocumented dashboard JSON endpoints (demandmet1/2chartdata).
    npp_base_url: str = "https://npp.gov.in/dashBoard"

    # --- IEX (Indian Energy Exchange) --------------------------------------
    # Base of the undocumented JSON API used by the iexindia.com market data pages.
    iex_api_base_url: str = "https://www.iexindia.com/api/v1"

    # --- CEA (Central Electricity Authority) --------------------------------
    # Daily reports are mirrored on NPP; monthly reports are listed on cea.nic.in.
    cea_npp_reports_base_url: str = "https://npp.gov.in/public-reports/cea"
    cea_site_base_url: str = "https://cea.nic.in"

    # --- Grid-India (NLDC) ----------------------------------------------------
    # File listing API used by grid-india.in, and the CDN serving the files.
    grid_india_api_base_url: str = "https://webapi.grid-india.in/api/v1"
    grid_india_cdn_base_url: str = "https://webcdn.grid-india.in"

    # --- WBES (Grid-India Web Based Energy Scheduling) -------------------------
    # Login-gated, undocumented JSON API behind newwbes.grid-india.in. The login is
    # interactive (username + OTP + CAPTCHA by a person); tokens are kept in a session
    # file OUTSIDE the project, OneDrive and raw-data directories (never in .env/DB/logs).
    wbes_api_base_url: str = "https://newwbes-api.grid-india.in"
    wbes_portal_url: str = "https://newwbes.grid-india.in/site/open-access/px"
    wbes_utility_id: str | None = None          # your WBES utility id (32 hex characters); not a secret
    wbes_session_file: Path | None = None       # default: <local app data>/pi/wbes/session.json
    wbes_refresh_margin_seconds: int = Field(default=150, ge=30)
    wbes_request_pause_seconds: float = Field(default=0.5, ge=0)
    wbes_login_wait_minutes: int = Field(default=15, ge=1)
    wbes_login_browser: str = "chrome"          # chrome, msedge or chromium (Playwright)
    # Time zone of WBES CreatedOn values that carry no offset. Unverified until live verification:
    # 'unverified' uses the later plausible reading (UTC unless after retrieval, then IST) so that
    # availability is never earlier than the truth; set 'Asia/Kolkata' or 'UTC' once confirmed.
    wbes_created_on_naive_timezone: Literal["unverified", "Asia/Kolkata", "UTC"] = "unverified"

    # --- IMD (India Meteorological Department) -----------------------------------
    # API platform documented at https://api.imd.gov.in/public/api_reference.html. Access requires IMD
    # approval (nodal officer) and IP whitelisting; whether a key/token is also issued is unverified.
    # If IMD issues one, set both IMD_API_KEY and IMD_API_KEY_HEADER (the header name IMD specifies).
    imd_api_base_url: str = "https://api.imd.gov.in/api/v1"
    imd_api_key: SecretStr | None = None
    imd_api_key_header: str | None = None
    imd_request_pause_seconds: float = Field(default=1.0, ge=0)
    # Time zone of the AWS DATE/TIME fields: unverified. While 'unverified', observed_at stays NULL and
    # only the published date and time are stored.
    imd_aws_time_zone: Literal["unverified", "Asia/Kolkata", "UTC"] = "unverified"

    # --- MOSDAC (ISRO Space Applications Centre) energy forecasts -------------------
    # GET <base>/solar?lat=&lon= and <base>/wind?lat=&lon= (no key or login per the supplied endpoint
    # information). Plant coordinates come from a TOML file in the weather-location format.
    mosdac_api_base_url: str = "https://mosdac.gov.in/apienergy"
    mosdac_locations_file: Path = Path("config/mosdac_locations.toml")
    mosdac_request_pause_seconds: float = Field(default=1.0, ge=0)

    # --- Scheduler / worker (src/scheduler, pi_jobs schema, migration 0011_pi_jobs) ---------------
    # Master switch: while false, no schedule is checked even if enabled in SCHEDULER_CONFIG_FILE.
    scheduler_global_enable: bool = False
    scheduler_config_file: Path = Path("config/scheduler.toml")
    scheduler_timezone: str = "Asia/Kolkata"
    scheduler_tick_seconds: float = Field(default=30.0, ge=1)
    scheduler_recovery_interval_seconds: float = Field(default=300.0, ge=10)
    scheduler_jobs_schema: str = "pi_jobs"
    scheduler_log_file_name: str = "pi-scheduler.log"
    worker_log_file_name: str = "pi-worker.log"
    # Manual commands get their own files so they never share (and break rotation of) a service's log.
    scheduler_check_log_file_name: str = "pi-scheduler-check.log"
    worker_once_log_file_name: str = "pi-worker-once.log"
    worker_concurrency: int = Field(default=2, ge=1, le=16)
    worker_poll_seconds: float = Field(default=10.0, ge=0.5)
    worker_lease_seconds: int = Field(default=600, ge=60)
    worker_heartbeat_seconds: int = Field(default=30, ge=5)
    worker_shutdown_grace_seconds: int = Field(default=300, ge=0)
    # Optional graceful-stop file (for hosts that cannot send Ctrl+C, e.g. Task Scheduler): when it
    # exists, the process finishes its current work and exits.
    scheduler_stop_file: Path | None = None
    worker_stop_file: Path | None = None

    # --- Raw storage -------------------------------------------------------
    raw_storage_backend: RawStorageBackendName = "local"
    raw_data_dir: Path = Path("raw_data")
    raw_storage_write_metadata: bool = True

    @field_validator("db_host", "db_name", "db_user", "db_password", "db_sslmode", "open_meteo_api_key",
                     "wbes_utility_id", "wbes_session_file", "imd_api_key", "imd_api_key_header",
                     "scheduler_stop_file", "worker_stop_file", mode="before")
    @classmethod
    def _blank_to_none(cls, value: Any, info: ValidationInfo) -> Any:
        """Treat empty strings (e.g. ``DB_HOST=`` in .env) as unset.

        Surrounding whitespace is stripped from everything except the password,
        where it could be significant.
        """
        if isinstance(value, str):
            if not value.strip():
                return None
            if info.field_name != "db_password":
                return value.strip()
        return value

    @field_validator("db_schema")
    @classmethod
    def _validate_schema_name(cls, value: str) -> str:
        schema = value.strip()
        if not _SCHEMA_NAME_PATTERN.fullmatch(schema):
            raise ValueError(f"DB_SCHEMA must be a lower-case PostgreSQL identifier, got {value!r}")
        return schema

    @field_validator("open_meteo_plan", mode="before")
    @classmethod
    def _normalise_open_meteo_plan(cls, value: Any) -> Any:
        return value.strip().lower() if isinstance(value, str) else value

    @field_validator("log_level")
    @classmethod
    def _normalise_log_level(cls, value: str) -> str:
        level = value.strip().upper()
        if level not in _VALID_LOG_LEVELS:
            raise ValueError(f"LOG_LEVEL must be one of {sorted(_VALID_LOG_LEVELS)}, got {value!r}")
        return level

    @field_validator("log_dir", "raw_data_dir", "weather_locations_file", "mosdac_locations_file",
                     "scheduler_config_file", "scheduler_stop_file", "worker_stop_file")
    @classmethod
    def _resolve_relative_paths(cls, value: Path | None) -> Path | None:
        """Interpret relative paths as relative to the project root."""
        if value is None:
            return None
        return value if value.is_absolute() else (PROJECT_ROOT / value).resolve()

    @field_validator("scheduler_jobs_schema")
    @classmethod
    def _validate_jobs_schema(cls, value: str) -> str:
        schema = value.strip()
        if not _SCHEMA_NAME_PATTERN.fullmatch(schema):
            raise ValueError(f"SCHEDULER_JOBS_SCHEMA must be a lower-case PostgreSQL identifier, got {value!r}")
        return schema


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings instance (cached after first call)."""
    return Settings()
