"""Date-range execution of the NPP pipelines, including dry runs.

Each IST day is a separate ingestion run (own ``pi.ingestion_run`` row and
transaction), so a failing day never affects the others. At most
:data:`~src.pipelines.npp.api.MAX_DAYS_PER_COMMAND` days per call.

Dry run: the same code path executes, including API calls, parsing,
validation and loading, but every database change happens inside one outer
transaction that is rolled back. Raw files go to a temporary directory
instead of ``RAW_DATA_DIR``.
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Literal

from config.settings import Settings, get_settings
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.http_client import HttpClient
from src.common.logging_config import get_logger
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager, create_raw_storage_manager
from src.ingestion.base import TransactionalDatabase
from src.ingestion.models import IngestionResult, RunStatus
from src.pipelines.npp import api
from src.pipelines.npp.pipeline import NppDemandPipeline, NppGenerationPipeline

logger = get_logger(__name__)

Kind = Literal["demand", "generation"]
PIPELINES: dict[str, type[NppDemandPipeline | NppGenerationPipeline]] = {
    "demand": NppDemandPipeline,
    "generation": NppGenerationPipeline,
}


@dataclass
class DayOutcome:
    """Result for one day."""

    day: date
    status: RunStatus
    result: IngestionResult | None = None
    error: str | None = None


@dataclass
class RunSummary:
    """Result of a date-range execution."""

    kind: Kind
    dry_run: bool
    outcomes: list[DayOutcome] = field(default_factory=list)
    raw_data_dir: Path | None = None

    @property
    def ok(self) -> bool:
        return bool(self.outcomes) and all(o.status is RunStatus.SUCCESS for o in self.outcomes)


@contextmanager
def _execution_context(settings: Settings, dry_run: bool) -> Iterator[tuple[TransactionalDatabase, RawStorageManager, Path]]:
    manager = DatabaseManager.from_settings(settings)
    try:
        if not dry_run:
            yield manager, create_raw_storage_manager(settings), settings.raw_data_dir
            return
        raw_dir = Path(tempfile.mkdtemp(prefix="npp_dry_run_"))
        with manager.engine.connect() as connection:
            outer = connection.begin()
            try:
                yield SavepointDatabase(connection), RawStorageManager(LocalFileSystemBackend(raw_dir)), raw_dir
            finally:
                outer.rollback()
                logger.info("Dry run: all database changes rolled back; raw files kept in %s", raw_dir)
    finally:
        manager.dispose()


def run_npp(
    kind: Kind,
    *,
    start_date: date,
    end_date: date | None = None,
    dry_run: bool = False,
    settings: Settings | None = None,
    http_client: HttpClient | None = None,
    database: TransactionalDatabase | None = None,
    raw_storage: RawStorageManager | None = None,
    today: date | None = None,
) -> RunSummary:
    """Ingest NPP demand or generation for ``start_date``..``end_date`` (inclusive IST days).

    Args:
        kind: ``"demand"`` or ``"generation"``.
        start_date, end_date: IST days; ``end_date`` defaults to ``start_date``.
        dry_run: Execute fully but roll back all database changes.
        settings, http_client, database, raw_storage, today: Injection points (tests).

    Raises:
        ValueError: Invalid, future, pre-history or longer than 31-day range.
    """
    settings = settings or get_settings()
    end_date = end_date or start_date
    api.validate_date_range(start_date, end_date, today=today or api.today_ist())
    summary = RunSummary(kind=kind, dry_run=dry_run)

    @contextmanager
    def injected() -> Iterator[tuple[TransactionalDatabase, RawStorageManager, Path | None]]:
        yield database, raw_storage or create_raw_storage_manager(settings), None  # type: ignore[misc]

    context = injected() if database is not None else _execution_context(settings, dry_run)
    with context as (db, storage, raw_dir):
        summary.raw_data_dir = raw_dir
        pipeline = PIPELINES[kind](database=db, raw_storage=storage, settings=settings, http_client=http_client)
        for day in api.date_range(start_date, end_date):
            parameters = {"date": day.isoformat(), "endpoint": pipeline.endpoint.path, "dry_run": dry_run}
            try:
                result = pipeline.run(parameters)
                summary.outcomes.append(DayOutcome(day, result.status, result, result.error))
            except Exception as exc:  # registration failures before a run row exists
                logger.exception("NPP %s for %s could not be ingested", kind, day)
                summary.outcomes.append(DayOutcome(day, RunStatus.FAILED, error=f"{type(exc).__name__}: {exc}"))
    return summary
