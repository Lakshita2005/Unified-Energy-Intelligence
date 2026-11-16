"""Run IMD ingestion commands (no scheduling, no backfill).

* ``aws``: the station mapping (``imd_station``) first, then the observations
  (``imd_station_observation``), so observations can be linked to stations. Each is its own run.
* ``city-forecast``: the 7-day city forecast (``imd_city_forecast``).

The IMD tables come from Alembic ``0008_imd``; if they are missing (the migration is not yet
applied) the command stops with a configuration error before any request is made.
Dry run: all database changes are rolled back and raw files go to a temporary directory.
"""

from __future__ import annotations

import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import inspect

from config.settings import Settings, get_settings
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.common.http_client import HttpClient
from src.common.logging_config import get_logger
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager, create_raw_storage_manager
from src.ingestion.base import TransactionalDatabase
from src.ingestion.models import IngestionResult, RunStatus
from src.pipelines.imd.pipeline import ImdCityForecastPipeline, ImdObservationPipeline, ImdStationPipeline
from src.pipelines.imd.reference import AWS_STATE_IDS

logger = get_logger(__name__)

COMMANDS: dict[str, tuple[type, ...]] = {
    "aws": (ImdStationPipeline, ImdObservationPipeline),
    "city-forecast": (ImdCityForecastPipeline,),
}
REQUIRED_TABLES = ("imd_station", "imd_station_observation", "imd_city_forecast")


@dataclass
class ImdRunSummary:
    command: str
    dry_run: bool
    runs: dict[str, IngestionResult] = field(default_factory=dict)
    raw_data_dir: Path | None = None

    @property
    def ok(self) -> bool:
        return bool(self.runs) and all(r.status is RunStatus.SUCCESS for r in self.runs.values())


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
        raw_dir = Path(tempfile.mkdtemp(prefix="imd_dry_run_"))
        with manager.engine.connect() as connection:
            outer = connection.begin()
            try:
                yield SavepointDatabase(connection), RawStorageManager(LocalFileSystemBackend(raw_dir)), raw_dir
            finally:
                outer.rollback()
                logger.info("Dry run: all database changes rolled back; raw files kept in %s", raw_dir)
    finally:
        manager.dispose()


def run_imd(command: str, *, station: str | None = None, state_id: int | None = None, dry_run: bool = False,
            reprocess: bool = False, settings: Settings | None = None, http_client: HttpClient | None = None,
            database: TransactionalDatabase | None = None, raw_storage: RawStorageManager | None = None,
            sleep: Callable[[float], None] = time.sleep) -> ImdRunSummary:
    """Run one IMD command.

    Raises:
        ValueError: Unknown command, or an invalid state id / option combination.
        ConfigurationError: The IMD tables do not exist (migration 0008_imd not applied).
    """
    settings = settings or get_settings()
    if command not in COMMANDS:
        raise ValueError(f"unknown command {command!r}; available: {sorted(COMMANDS)}")
    if state_id is not None and state_id not in AWS_STATE_IDS:
        raise ValueError(f"state id {state_id} is not one of the documented IMD state ids 1-36")
    if command == "city-forecast" and state_id is not None:
        raise ValueError("--state-id applies to the aws command only")
    summary = ImdRunSummary(command, dry_run)
    parameters: dict[str, Any] = {"command": command, "station": station, "state_id": state_id, "dry_run": dry_run,
                                  "reprocess": reprocess}
    with _execution_context(settings, dry_run, database, raw_storage) as (db, storage, raw_dir):
        summary.raw_data_dir = raw_dir
        with db.connect() as connection:
            existing = set(inspect(connection).get_table_names(schema=settings.db_schema))
        missing = [t for t in REQUIRED_TABLES if t not in existing]
        if missing:
            raise ConfigurationError(f"IMD tables missing {missing}: Alembic migration 0008_imd is not applied")
        for position, pipeline_class in enumerate(COMMANDS[command]):
            if position:
                sleep(settings.imd_request_pause_seconds)
            pipeline = pipeline_class(database=db, raw_storage=storage, settings=settings, http_client=http_client,
                                      sleep=sleep)
            pipeline.reload_duplicate_documents = reprocess
            run_parameters = dict(parameters)
            if pipeline_class is ImdStationPipeline:     # the mapping endpoint takes no filter
                run_parameters.update(station=None, state_id=None)
            summary.runs[pipeline_class.target_table] = pipeline.run(run_parameters)
    return summary
