"""Run MOSDAC ingestion commands (no scheduling, no backfill: MOSDAC serves only its current series).

* ``solar`` / ``wind`` / ``all``: for each selected location (default: every location in
  ``MOSDAC_LOCATIONS_FILE``), one ingestion run per endpoint. Runs are independent: a failing
  endpoint or location is recorded as FAILED and the others still load (partial success).

Locations use the weather-location TOML format and validation, but are NOT registered in
``pi.weather_location`` (Open-Meteo's all-locations runs include every registered location).
A location whose code is already stored with different coordinates is refused.

The tables come from Alembic ``0009_mosdac``; if they are missing the command stops with a
configuration error before any request is made. Dry run: all database changes are rolled back and
raw files go to a temporary directory.
"""

from __future__ import annotations

import tempfile
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import inspect, text

from config.settings import Settings, get_settings
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.common.http_client import HttpClient
from src.common.logging_config import get_logger
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager, create_raw_storage_manager
from src.ingestion.base import TransactionalDatabase
from src.ingestion.models import IngestionResult, RunStatus
from src.pipelines.mosdac.errors import MosdacLocationError
from src.pipelines.mosdac.pipeline import MosdacSolarPipeline, MosdacWindPipeline, _MosdacPipeline
from src.pipelines.open_meteo.locations import LocationConfig, LocationError, load_location_config

logger = get_logger(__name__)

COMMANDS: dict[str, tuple[type[_MosdacPipeline], ...]] = {
    "solar": (MosdacSolarPipeline,),
    "wind": (MosdacWindPipeline,),
    "all": (MosdacSolarPipeline, MosdacWindPipeline),
}
REQUIRED_TABLES = ("mosdac_solar_forecast", "mosdac_wind_forecast")


@dataclass
class MosdacRunSummary:
    command: str
    dry_run: bool
    runs: dict[tuple[str, str], IngestionResult] = field(default_factory=dict)   # (location, kind) -> result
    location_errors: dict[str, str] = field(default_factory=dict)
    raw_data_dir: Path | None = None

    @property
    def ok(self) -> bool:
        return (bool(self.runs) and not self.location_errors
                and all(r.status is RunStatus.SUCCESS for r in self.runs.values()))


def load_locations(settings: Settings) -> dict[str, LocationConfig]:
    """Configured MOSDAC locations by code (validated like weather locations)."""
    try:
        configs = load_location_config(settings.mosdac_locations_file)
    except LocationError as exc:
        raise MosdacLocationError(str(exc)) from exc
    return {config.code: config for config in configs}


@contextmanager
def _execution_context(settings: Settings, dry_run: bool, database: TransactionalDatabase | None,
                       raw_storage: RawStorageManager | None,
                       ) -> Iterator[tuple[TransactionalDatabase, RawStorageManager, Path | None]]:
    if database is not None:
        yield database, raw_storage or create_raw_storage_manager(settings), None
        return
    manager = DatabaseManager.from_settings(settings)
    try:
        if not dry_run:
            yield manager, create_raw_storage_manager(settings), None
            return
        raw_dir = Path(tempfile.mkdtemp(prefix="mosdac_dry_run_"))
        with manager.engine.connect() as connection:
            outer = connection.begin()
            try:
                yield SavepointDatabase(connection), RawStorageManager(LocalFileSystemBackend(raw_dir)), raw_dir
            finally:
                outer.rollback()
                logger.info("Dry run: all database changes rolled back; raw files kept in %s", raw_dir)
    finally:
        manager.dispose()


def _stored_coordinates(connection: Any, schema: str, code: str) -> set[tuple[Any, Any]]:
    coordinates: set[tuple[Any, Any]] = set()
    for table in REQUIRED_TABLES:
        coordinates |= {tuple(r) for r in connection.execute(text(
            f"SELECT DISTINCT latitude, longitude FROM {schema}.{table} WHERE location_code = :c"), {"c": code})}
    return coordinates


def run_mosdac(command: str, *, locations: Sequence[str] | None = None, dry_run: bool = False,
               reprocess: bool = False, settings: Settings | None = None, http_client: HttpClient | None = None,
               database: TransactionalDatabase | None = None, raw_storage: RawStorageManager | None = None,
               sleep: Callable[[float], None] = time.sleep) -> MosdacRunSummary:
    """Run one MOSDAC command.

    Raises:
        ValueError: Unknown command.
        MosdacLocationError: No locations configured, or an unknown location requested.
        ConfigurationError: The MOSDAC tables do not exist (migration 0009_mosdac not applied).
    """
    settings = settings or get_settings()
    if command not in COMMANDS:
        raise ValueError(f"unknown command {command!r}; available: {sorted(COMMANDS)}")
    configs = load_locations(settings)
    if not configs:
        raise MosdacLocationError(f"No MOSDAC locations configured in {settings.mosdac_locations_file}")
    codes = list(dict.fromkeys(locations)) if locations else sorted(configs)
    unknown = [c for c in codes if c not in configs]
    if unknown:
        raise MosdacLocationError(f"Unknown MOSDAC location(s) {unknown}; configured: {sorted(configs)}")

    summary = MosdacRunSummary(command, dry_run)
    with _execution_context(settings, dry_run, database, raw_storage) as (db, storage, raw_dir):
        summary.raw_data_dir = raw_dir
        with db.connect() as connection:
            existing = set(inspect(connection).get_table_names(schema=settings.db_schema))
            missing = [t for t in REQUIRED_TABLES if t not in existing]
            if missing:
                raise ConfigurationError(f"MOSDAC tables missing {missing}: Alembic migration 0009_mosdac is not applied")
            stored = {code: _stored_coordinates(connection, settings.db_schema, code) for code in codes}
        first = True
        for code in codes:
            config = configs[code]
            moved = {c for c in stored[code] if (c[0], c[1]) != (config.latitude, config.longitude)}
            if moved:
                summary.location_errors[code] = (f"{code} is stored at {sorted(map(str, moved))} but configured at "
                                                 f"({config.latitude}, {config.longitude}); locations are never moved")
                logger.error("%s", summary.location_errors[code])
                continue
            for pipeline_class in COMMANDS[command]:
                if not first:
                    sleep(settings.mosdac_request_pause_seconds)
                first = False
                pipeline = pipeline_class(database=db, raw_storage=storage, settings=settings,
                                          http_client=http_client, sleep=sleep)
                pipeline.reload_duplicate_documents = reprocess
                parameters = {"command": command, "dry_run": dry_run, "reprocess": reprocess,
                              **pipeline_class.location_parameters(code, config.latitude, config.longitude)}
                result = pipeline.run(parameters)       # failures are recorded in the result, not raised
                summary.runs[(code, pipeline_class.kind)] = result
                if result.status is not RunStatus.SUCCESS:
                    logger.error("MOSDAC %s %s: %s%s", pipeline_class.kind, code, result.status.value,
                                 f" ({result.error})" if result.error else "")
    return summary
