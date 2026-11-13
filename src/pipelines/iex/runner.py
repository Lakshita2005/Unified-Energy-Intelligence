"""Execution of the IEX pipelines for a date range, including dry runs.

One call is one ingestion run for one market over at most 31 delivery days
(the API returns a range in one request, so a range costs the same number of
requests as one day).

The reference rows in ``pi.market_master`` / ``pi.market_area`` must exist
(``python -m scripts.ingest_iex reference``). A dry run that finds them
missing seeds them inside its rolled-back transaction so it can still execute
fully.

Dry run: the same code path executes, including API calls, parsing,
validation and loading, but every database change happens inside one outer
transaction that is rolled back. Raw files go to a temporary directory
instead of ``RAW_DATA_DIR``.
"""

from __future__ import annotations

import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Literal

from config.settings import Settings, get_settings
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.common.http_client import HttpClient
from src.common.logging_config import get_logger
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager, create_raw_storage_manager
from src.ingestion.base import TransactionalDatabase
from src.ingestion.models import IngestionResult, RunStatus
from src.pipelines.iex import api
from src.pipelines.iex.pipeline import IexDamPipeline, IexGdamPipeline, IexRtmPipeline
from src.pipelines.iex.reference import missing_reference, seed_reference

logger = get_logger(__name__)

Kind = Literal["dam", "gdam", "rtm"]
PIPELINES: dict[str, type[IexDamPipeline | IexGdamPipeline | IexRtmPipeline]] = {
    "dam": IexDamPipeline, "gdam": IexGdamPipeline, "rtm": IexRtmPipeline,
}


@dataclass
class RunSummary:
    """Result of one IEX run."""

    kind: Kind
    start_date: date
    end_date: date
    dry_run: bool
    status: RunStatus = RunStatus.FAILED
    result: IngestionResult | None = None
    error: str | None = None
    raw_data_dir: Path | None = None
    seeded_reference: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status is RunStatus.SUCCESS


@contextmanager
def _execution_context(settings: Settings, dry_run: bool) -> Iterator[tuple[TransactionalDatabase, RawStorageManager, Path]]:
    manager = DatabaseManager.from_settings(settings)
    try:
        if not dry_run:
            yield manager, create_raw_storage_manager(settings), settings.raw_data_dir
            return
        raw_dir = Path(tempfile.mkdtemp(prefix="iex_dry_run_"))
        with manager.engine.connect() as connection:
            outer = connection.begin()
            try:
                yield SavepointDatabase(connection), RawStorageManager(LocalFileSystemBackend(raw_dir)), raw_dir
            finally:
                outer.rollback()
                logger.info("Dry run: all database changes rolled back; raw files kept in %s", raw_dir)
    finally:
        manager.dispose()


def run_iex(
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
    sleep: Callable[[float], None] = time.sleep,
) -> RunSummary:
    """Ingest IEX prices for one market over ``start_date``..``end_date`` (inclusive delivery days).

    Raises:
        ValueError: Invalid, unpublished, pre-history or longer than 31-day range.
        ConfigurationError: Reference rows missing (outside dry runs).
    """
    settings = settings or get_settings()
    end_date = end_date or start_date
    today = today or api.today_ist()
    pipeline_class = PIPELINES[kind]
    api.validate_date_range(pipeline_class.market, start_date, end_date, today=today)
    summary = RunSummary(kind, start_date, end_date, dry_run)

    @contextmanager
    def injected() -> Iterator[tuple[TransactionalDatabase, RawStorageManager, Path | None]]:
        yield database, raw_storage or create_raw_storage_manager(settings), None  # type: ignore[misc]

    context = injected() if database is not None else _execution_context(settings, dry_run)
    with context as (db, storage, raw_dir):
        summary.raw_data_dir = raw_dir
        pipeline = pipeline_class(database=db, raw_storage=storage, settings=settings, http_client=http_client,
                                  sleep=sleep)
        with db.connect() as connection:
            missing = missing_reference(connection, settings.db_schema, pipeline.market.market_code, pipeline.areas())
            if missing and dry_run:
                summary.seeded_reference = seed_reference(connection, settings.db_schema).inserted
                logger.info("Dry run: seeded missing reference rows (rolled back): %s", summary.seeded_reference)
            elif missing:
                raise ConfigurationError(f"Reference rows missing: {missing}. Run: python -m scripts.ingest_iex reference")
        parameters = {"start_date": start_date.isoformat(), "end_date": end_date.isoformat(),
                      "market": pipeline.market.market_code, "areas": pipeline.areas(), "interval": api.INTERVAL,
                      "today": today.isoformat(), "dry_run": dry_run}
        summary.result = pipeline.run(parameters)
        summary.status, summary.error = summary.result.status, summary.result.error
    return summary
