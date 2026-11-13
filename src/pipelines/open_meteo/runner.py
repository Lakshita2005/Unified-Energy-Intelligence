"""Multi-location execution of the Open-Meteo pipelines, including dry runs.

Each location is a separate ingestion run (own ``pi.ingestion_run`` row and
transactions), so one failing location never affects the others.

Dry run: the same code path executes, including API calls, parsing, validation
and loading, but every database change happens inside one outer transaction
that is rolled back. Raw files go to a temporary directory instead of
``RAW_DATA_DIR``.
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from config.settings import Settings, get_settings
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.http_client import HttpClient
from src.common.logging_config import get_logger
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager, create_raw_storage_manager
from src.ingestion.base import TransactionalDatabase
from src.ingestion.models import IngestionResult, RunStatus
from src.pipelines.open_meteo import api
from src.pipelines.open_meteo.locations import (
    LocationError,
    WeatherLocation,
    WeatherLocationRepository,
    is_test_location,
    load_location_config,
)
from src.pipelines.open_meteo.pipeline import OpenMeteoForecastPipeline, OpenMeteoHistoricalPipeline

logger = get_logger(__name__)

Kind = Literal["forecast", "historical"]


def today_ist() -> date:
    """Today's date in India. Uses a fixed offset (India has no DST), so no tz database is needed."""
    return datetime.now(timezone(timedelta(hours=5, minutes=30))).date()


@dataclass
class LocationOutcome:
    """Result for one location."""

    location_code: str
    status: RunStatus
    result: IngestionResult | None = None
    error: str | None = None


@dataclass
class RunSummary:
    """Result of a multi-location execution."""

    kind: Kind
    dry_run: bool
    outcomes: list[LocationOutcome] = field(default_factory=list)
    raw_data_dir: Path | None = None
    registered_locations: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return bool(self.outcomes) and all(o.status is RunStatus.SUCCESS for o in self.outcomes)


def resolve_locations(
    database: TransactionalDatabase, settings: Settings, requested: Sequence[str] | None
) -> tuple[dict[str, WeatherLocation | LocationError], list[str]]:
    """Register configured locations and return the requested ones.

    Without ``requested``, every configured or registered location except
    ``TEST_ONLY_*`` test locations is used.

    Each requested code maps to a :class:`WeatherLocation`, or to the
    :class:`LocationError` explaining why it cannot be used, so one bad
    location does not block the others.
    """
    configs = {config.code: config for config in load_location_config(settings.weather_locations_file)}
    repository = WeatherLocationRepository(settings.db_schema)
    with database.connect() as connection:
        registered = repository.list_locations(connection)
    if requested:
        codes = list(requested)
    else:
        # Test locations are never part of an all-locations run.
        codes = sorted(code for code in set(configs) | set(registered) if not is_test_location(code))
    if not codes:
        raise LocationError(
            f"No weather locations available. Add verified coordinates to {settings.weather_locations_file} "
            "(see config/weather_locations.example.toml)."
        )

    resolved: dict[str, WeatherLocation | LocationError] = {}
    created: list[str] = []
    for code in codes:
        try:
            if code in configs:
                with database.connect() as connection:
                    location, was_created = repository.ensure(connection, configs[code])
                if was_created:
                    created.append(code)
                resolved[code] = location
            elif code in registered:
                resolved[code] = registered[code]
            else:
                raise LocationError(f"Location {code} is neither configured nor registered in weather_location")
        except LocationError as exc:
            logger.error("%s", exc)
            resolved[code] = exc
    return resolved, created


@contextmanager
def _execution_context(settings: Settings, dry_run: bool) -> Iterator[tuple[TransactionalDatabase, RawStorageManager, Path]]:
    if not dry_run:
        manager = DatabaseManager.from_settings(settings)
        try:
            yield manager, create_raw_storage_manager(settings), settings.raw_data_dir
        finally:
            manager.dispose()
        return
    manager = DatabaseManager.from_settings(settings)
    raw_dir = Path(tempfile.mkdtemp(prefix="open_meteo_dry_run_"))
    try:
        with manager.engine.connect() as connection:
            outer = connection.begin()
            try:
                yield SavepointDatabase(connection), RawStorageManager(LocalFileSystemBackend(raw_dir)), raw_dir
            finally:
                outer.rollback()
                logger.info("Dry run: all database changes rolled back; raw files kept in %s", raw_dir)
    finally:
        manager.dispose()


def run_open_meteo(
    kind: Kind,
    *,
    locations: Sequence[str] | None = None,
    dry_run: bool = False,
    forecast_days: int = 7,
    start_date: date | None = None,
    end_date: date | None = None,
    max_days: int = api.HISTORICAL_CHUNK_DAYS,
    settings: Settings | None = None,
    http_client: HttpClient | None = None,
    database: TransactionalDatabase | None = None,
    raw_storage: RawStorageManager | None = None,
) -> RunSummary:
    """Run forecast or historical ingestion for one or more locations.

    Args:
        kind: ``"forecast"`` or ``"historical"``.
        locations: Location codes; default is every configured or registered location
            except ``TEST_ONLY_*`` test locations.
        dry_run: Execute fully but roll back all database changes.
        forecast_days: Forecast horizon (1-16 days, from today IST).
        start_date, end_date: Inclusive IST date range (historical; both required).
        max_days: Upper bound on the historical range, preventing accidental backfills.
        settings, http_client, database, raw_storage: Injection points (tests).

    Raises:
        ValueError: Invalid horizon or date range.
        LocationError: No locations available at all.
    """
    settings = settings or get_settings()
    if kind == "forecast":
        api.validate_forecast_days(forecast_days)
        run_parameters: dict[str, Any] = {"forecast_days": forecast_days}
        pipeline_class: type[OpenMeteoForecastPipeline | OpenMeteoHistoricalPipeline] = OpenMeteoForecastPipeline
    else:
        if start_date is None or end_date is None:
            raise ValueError("Historical ingestion requires both start_date and end_date")
        api.validate_historical_range(start_date, end_date, today=today_ist(), max_days=max_days)
        run_parameters = {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()}
        pipeline_class = OpenMeteoHistoricalPipeline

    summary = RunSummary(kind=kind, dry_run=dry_run)

    @contextmanager
    def injected() -> Iterator[tuple[TransactionalDatabase, RawStorageManager, Path | None]]:
        yield database, raw_storage or create_raw_storage_manager(settings), None  # type: ignore[misc]

    context = injected() if database is not None else _execution_context(settings, dry_run)
    with context as (db, storage, raw_dir):
        summary.raw_data_dir = raw_dir
        resolved, summary.registered_locations = resolve_locations(db, settings, locations)
        pipeline = pipeline_class(database=db, raw_storage=storage, settings=settings, http_client=http_client)
        for code, location in resolved.items():
            if isinstance(location, LocationError):
                summary.outcomes.append(LocationOutcome(code, RunStatus.FAILED, error=str(location)))
                continue
            parameters = {**pipeline.location_parameters(location), **run_parameters,
                          "variables": [v.api_name for v in api.HOURLY_VARIABLES],
                          "timezone": api.REQUEST_TIMEZONE, "dry_run": dry_run}
            try:
                result = pipeline.run(parameters)
                summary.outcomes.append(LocationOutcome(code, result.status, result, result.error))
            except Exception as exc:  # registration failures before a run row exists
                logger.exception("Location %s could not be ingested", code)
                summary.outcomes.append(LocationOutcome(code, RunStatus.FAILED, error=f"{type(exc).__name__}: {exc}"))
    return summary


