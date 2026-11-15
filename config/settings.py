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
    # Only needed for commercial use (customer- API hosts). Never logged.
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

    # --- Raw storage -------------------------------------------------------
    raw_storage_backend: RawStorageBackendName = "local"
    raw_data_dir: Path = Path("raw_data")
    raw_storage_write_metadata: bool = True

    @field_validator("db_host", "db_name", "db_user", "db_password", "db_sslmode", "open_meteo_api_key",
                     "wbes_utility_id", "wbes_session_file", mode="before")
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

    @field_validator("log_level")
    @classmethod
    def _normalise_log_level(cls, value: str) -> str:
        level = value.strip().upper()
        if level not in _VALID_LOG_LEVELS:
            raise ValueError(f"LOG_LEVEL must be one of {sorted(_VALID_LOG_LEVELS)}, got {value!r}")
        return level

    @field_validator("log_dir", "raw_data_dir", "weather_locations_file")
    @classmethod
    def _resolve_relative_paths(cls, value: Path) -> Path:
        """Interpret relative paths as relative to the project root."""
        return value if value.is_absolute() else (PROJECT_ROOT / value).resolve()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings instance (cached after first call)."""
    return Settings()
