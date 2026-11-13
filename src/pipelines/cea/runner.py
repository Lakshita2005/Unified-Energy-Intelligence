"""Execution of the CEA pipelines and the reference seeding, including dry runs.

One call is one ingestion run: one daily report type over at most 31 report
dates, or one monthly report series over at most 12 months.

Reference rows (regions/states; plants, units and name mappings) must exist
(``python -m scripts.ingest_cea reference``). A dry run that finds them
missing seeds them inside its rolled-back transaction so it can execute fully.

Reprocess (``reprocess=True``): files whose identical content was already
ingested are parsed and loaded again instead of being skipped, so a parser fix
reaches them; records already stored unchanged are skipped by the natural-key
comparison, so only missing or changed records are inserted.

Dry run: the same code path executes (downloads, parsing, validation and
loading), but every database change happens inside one outer transaction
that is rolled back. Raw files go to a temporary directory.
"""

from __future__ import annotations

import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Literal

from config.settings import Settings, get_settings
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.common.http_client import HttpClient
from src.common.logging_config import get_logger
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager, create_raw_storage_manager
from src.ingestion.base import TransactionalDatabase
from src.ingestion.models import DataQualityIssue, IngestionResult, RunStatus, Severity
from src.ingestion.repositories import IngestionRepository
from src.pipelines.cea import api
from src.pipelines.cea.cells import read_grid
from src.pipelines.cea.parse_dgr import Dgr2Report, read_dgr2
from src.pipelines.cea.pipeline import (
    CoalStockPipeline,
    PlantDailyPipeline,
    PspEnergyPipeline,
    PspPeakPipeline,
    UnitOutagePipeline,
)
from src.pipelines.cea.reference import SeedResult, load_entities, missing_geography, seed_reference

logger = get_logger(__name__)

DailyKind = Literal["coal", "outages", "plant-daily"]
PspKind = Literal["psp-energy", "psp-peak"]
DAILY_PIPELINES = {"coal": CoalStockPipeline, "outages": UnitOutagePipeline, "plant-daily": PlantDailyPipeline}
PSP_PIPELINES = {"psp-energy": PspEnergyPipeline, "psp-peak": PspPeakPipeline}
RECENT_DAYS = 2      # a report missing for one of the last 2 days may simply be late (INFO, not WARNING)
RECENT_MONTHS = 4    # monthly reports appear about 2.5 months after the month


@dataclass
class RunSummary:
    """Result of one CEA run."""

    kind: str
    period: str
    dry_run: bool
    status: RunStatus = RunStatus.FAILED
    result: IngestionResult | None = None
    error: str | None = None
    raw_data_dir: Path | None = None
    seeded_reference: SeedResult | None = None
    missing: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status is RunStatus.SUCCESS


@contextmanager
def _execution_context(settings: Settings, dry_run: bool) -> Iterator[tuple[TransactionalDatabase, RawStorageManager, Path | None]]:
    manager = DatabaseManager.from_settings(settings)
    try:
        if not dry_run:
            yield manager, create_raw_storage_manager(settings), None
            return
        raw_dir = Path(tempfile.mkdtemp(prefix="cea_dry_run_"))
        with manager.engine.connect() as connection:
            outer = connection.begin()
            try:
                yield SavepointDatabase(connection), RawStorageManager(LocalFileSystemBackend(raw_dir)), raw_dir
            finally:
                outer.rollback()
                logger.info("Dry run: all database changes rolled back; raw files kept in %s", raw_dir)
    finally:
        manager.dispose()


@contextmanager
def _context(settings: Settings, dry_run: bool, database: TransactionalDatabase | None,
             raw_storage: RawStorageManager | None) -> Iterator[tuple[TransactionalDatabase, RawStorageManager, Path | None]]:
    if database is not None:
        yield database, raw_storage or create_raw_storage_manager(settings), None
    else:
        with _execution_context(settings, dry_run) as ctx:
            yield ctx


# ----------------------------------------------------------------- reference

def fetch_dgr2(client: HttpClient, settings: Settings, day: date,
               raw_storage: RawStorageManager | None = None) -> Dgr2Report:
    """Download (and store raw) DGR sub-report 2 for ``day`` and parse it.

    Raises:
        ConfigurationError: The report is not published for that date.
    """
    url = api.DGR2.url(settings.cea_npp_reports_base_url, day)
    fetched = api.fetch_file(client, url)
    if fetched is None:
        raise ConfigurationError(f"DGR sub-report 2 for {day} is not published (HTTP 404); choose another --date")
    api.require_binary(fetched, f"dgr2 {day}")
    if raw_storage is not None:
        stored = raw_storage.save_bytes(
            fetched.content, source="cea", dataset="cea_reference", extension="xls",
            ingested_at=fetched.retrieved_at, partition="dgr2",
            metadata={"purpose": "reference seeding (plant / unit master)", "source_url": url,
                      "report_date": day.isoformat(), "retrieved_at": fetched.retrieved_at.isoformat(),
                      "published_at": fetched.last_modified.isoformat() if fetched.last_modified else None})
        logger.info("Stored reference source %s (sha256=%s)", stored.key, stored.sha256)
    return read_dgr2(read_grid(fetched.content), expected_date=day)


def run_reference(*, report_date: date | None = None, dry_run: bool = False, settings: Settings | None = None,
                  http_client: HttpClient | None = None, database: TransactionalDatabase | None = None,
                  raw_storage: RawStorageManager | None = None, today: date | None = None) -> tuple[SeedResult, date]:
    """Seed regions/states and the plants, units and mappings of DGR sub-report 2 for ``report_date``.

    ``report_date`` defaults to two days ago (the latest report that is reliably published).
    """
    settings = settings or get_settings()
    report_date = report_date or (today or api.today_ist()) - timedelta(days=2)
    client = http_client or HttpClient(settings=settings)
    try:
        with _context(settings, dry_run, database, raw_storage) as (db, storage, _):
            report = fetch_dgr2(client, settings, report_date, storage)
            with db.connect() as connection:
                result = seed_reference(connection, settings.db_schema, report)
    finally:
        if http_client is None:
            client.close()
    return result, report_date


# ------------------------------------------------------------------- runs

def _record_missing(db: TransactionalDatabase, settings: Settings, result: IngestionResult, table: str,
                    issues: list[DataQualityIssue]) -> None:
    if not issues:
        return
    with db.connect() as connection:
        IngestionRepository(settings.db_schema).record_issues(connection, result.ingestion_run_id, issues)
    result.data_quality_issues += len(issues)


def run_cea_daily(
    kind: DailyKind,
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
    reprocess: bool = False,
) -> RunSummary:
    """Ingest one daily CEA report type for ``start_date``..``end_date`` (inclusive report dates).

    Raises:
        ValueError: Invalid, unpublishable, pre-history or longer than 31-day range.
        ConfigurationError: Reference rows missing (outside dry runs).
    """
    settings = settings or get_settings()
    end_date = end_date or start_date
    today = today or api.today_ist()
    pipeline_class = DAILY_PIPELINES[kind]
    api.validate_date_range(pipeline_class.report, start_date, end_date, today=today)
    summary = RunSummary(kind, f"{start_date}..{end_date}", dry_run)
    with _context(settings, dry_run, database, raw_storage) as (db, storage, raw_dir):
        summary.raw_data_dir = raw_dir
        with db.connect() as connection:
            entities = load_entities(connection, settings.db_schema, pipeline_class.dataset.code)
        if not entities.plants:
            if not dry_run:
                raise ConfigurationError("No CEA plant mappings found. Run: python -m scripts.ingest_cea reference")
            client = http_client or HttpClient(settings=settings)
            try:
                report = fetch_dgr2(client, settings, start_date)
            finally:
                if http_client is None:
                    client.close()
            with db.connect() as connection:
                summary.seeded_reference = seed_reference(connection, settings.db_schema, report)
                entities = load_entities(connection, settings.db_schema, pipeline_class.dataset.code)
            logger.info("Dry run: seeded reference rows from DGR sub-report 2 of %s (rolled back)", start_date)
        pipeline = pipeline_class(database=db, raw_storage=storage, settings=settings, http_client=http_client,
                                  sleep=sleep, entities=entities)
        pipeline.reload_duplicate_documents = reprocess
        parameters = {"start_date": start_date.isoformat(), "end_date": end_date.isoformat(),
                      "report": pipeline_class.report.kind, "today": today.isoformat(), "dry_run": dry_run,
                      "mapped_plants": len(entities.plants), "reprocess": reprocess}
        summary.result = pipeline.run(parameters)
        summary.missing = [d.isoformat() for d in pipeline.missing]
        issues = [DataQualityIssue(
            pipeline_class.target_table, "MISSING_DAY",
            f"CEA {pipeline_class.report.kind} report for {day} is not published (HTTP 404)"
            + (" - may simply be late" if (today - day).days <= RECENT_DAYS else ""),
            Severity.INFO if (today - day).days <= RECENT_DAYS else Severity.WARNING, day.isoformat())
            for day in pipeline.missing]
        _record_missing(db, settings, summary.result, pipeline_class.target_table, issues)
        summary.status, summary.error = summary.result.status, summary.result.error
    return summary


def run_cea_psp(
    kind: PspKind,
    *,
    start_month: date,
    end_month: date | None = None,
    dry_run: bool = False,
    settings: Settings | None = None,
    http_client: HttpClient | None = None,
    database: TransactionalDatabase | None = None,
    raw_storage: RawStorageManager | None = None,
    today: date | None = None,
    sleep: Callable[[float], None] = time.sleep,
    reprocess: bool = False,
) -> RunSummary:
    """Ingest one monthly Power Supply Position series for ``start_month``..``end_month`` (inclusive).

    Raises:
        ValueError: Invalid, unpublishable, unsupported or longer than 12-month range.
        ConfigurationError: Region/state rows missing (outside dry runs).
    """
    settings = settings or get_settings()
    start_month = api.month_start(start_month)
    end_month = api.month_start(end_month or start_month)
    today = today or api.today_ist()
    api.validate_month_range(start_month, end_month, today=today)
    pipeline_class = PSP_PIPELINES[kind]
    summary = RunSummary(kind, f"{start_month:%Y-%m}..{end_month:%Y-%m}", dry_run)
    with _context(settings, dry_run, database, raw_storage) as (db, storage, raw_dir):
        summary.raw_data_dir = raw_dir
        with db.connect() as connection:
            missing = missing_geography(connection, settings.db_schema)
            if missing and dry_run:
                summary.seeded_reference = seed_reference(connection, settings.db_schema, None)
            elif missing:
                raise ConfigurationError(f"Region/state rows missing ({len(missing)}). "
                                         "Run: python -m scripts.ingest_cea reference")
        pipeline = pipeline_class(database=db, raw_storage=storage, settings=settings, http_client=http_client,
                                  sleep=sleep)
        pipeline.reload_duplicate_documents = reprocess
        parameters = {"start_month": start_month.isoformat(), "end_month": end_month.isoformat(),
                      "measure": pipeline_class.psp.measure, "today": today.isoformat(), "dry_run": dry_run,
                      "reprocess": reprocess}
        summary.result = pipeline.run(parameters)
        summary.missing = [f"{m:%Y-%m}" for m in pipeline.missing]
        current = api.month_start(today)
        issues = []
        for month in pipeline.missing:
            age = (current.year - month.year) * 12 + current.month - month.month
            recent = age <= RECENT_MONTHS
            issues.append(DataQualityIssue(
                pipeline_class.target_table, "MISSING_MONTH",
                f"No CEA {pipeline_class.psp.listing_title!r} report listed for {month:%Y-%m}"
                + (" - may not be published yet" if recent else ""),
                Severity.INFO if recent else Severity.WARNING, f"{month:%Y-%m}"))
        _record_missing(db, settings, summary.result, pipeline_class.target_table, issues)
        summary.status, summary.error = summary.result.status, summary.result.error
    return summary

