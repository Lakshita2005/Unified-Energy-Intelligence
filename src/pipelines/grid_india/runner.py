"""Execution of the Grid-India Daily PSP sections, including dry runs.

One command covers a range of report dates (at most 31) and one or more
sections. The runner:

1. queries the file listing for the neighbouring months and stores each raw
   listing response (write-once JSON);
2. selects every workbook version (.xls) for the requested dates and downloads
   each **once**, storing it **once** (write-once raw file + SHA-256, with the
   listing entry and the availability decision in the metadata);
3. runs one pipeline per section, each with its **own ingestion run** and
   status, on the same downloaded workbooks;
4. records report dates without a workbook as ``MISSING_DAY`` in each run.

If the listing or a download fails, every requested section's run is recorded
as FAILED with that error (files downloaded before the failure stay in raw
storage). The command succeeds only if every section's run is SUCCESS.

Dry run: everything executes, but all database changes are in one outer
transaction that is rolled back; raw files go to a temporary directory.
"""

from __future__ import annotations

import json
import tempfile
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from config.settings import Settings, get_settings
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.common.http_client import HttpClient
from src.common.logging_config import get_logger
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager, create_raw_storage_manager
from src.ingestion.base import TransactionalDatabase
from src.ingestion.models import DataQualityIssue, IngestionResult, RawDocument, RunStatus, Severity
from src.ingestion.repositories import IngestionRepository
from src.pipelines.cea.reference import missing_geography, seed_reference
from src.pipelines.grid_india import api
from src.pipelines.grid_india.pipeline import SECTION_PIPELINES, PreparedWorkbook

logger = get_logger(__name__)

RAW_SOURCE = "grid_india"
RAW_DATASET = "grid_india_psp_workbook"   # one folder for the shared workbooks and listings


@dataclass
class SectionSummary:
    section: str
    status: RunStatus = RunStatus.FAILED
    result: IngestionResult | None = None
    error: str | None = None


@dataclass
class PspRunSummary:
    start_date: date
    end_date: date
    dry_run: bool
    sections: dict[str, SectionSummary] = field(default_factory=dict)
    workbooks: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    fetch_error: str | None = None
    raw_data_dir: Path | None = None
    seeded_reference: bool = False

    @property
    def ok(self) -> bool:
        return bool(self.sections) and all(s.status is RunStatus.SUCCESS for s in self.sections.values())


@contextmanager
def _execution_context(settings: Settings, dry_run: bool, database: TransactionalDatabase | None,
                       raw_storage: RawStorageManager | None) -> Iterator[tuple[TransactionalDatabase, RawStorageManager, Path | None]]:
    if database is not None:
        yield database, raw_storage or create_raw_storage_manager(settings), None
        return
    manager = DatabaseManager.from_settings(settings)
    try:
        if not dry_run:
            yield manager, create_raw_storage_manager(settings), None
            return
        raw_dir = Path(tempfile.mkdtemp(prefix="grid_india_dry_run_"))
        with manager.engine.connect() as connection:
            outer = connection.begin()
            try:
                yield SavepointDatabase(connection), RawStorageManager(LocalFileSystemBackend(raw_dir)), raw_dir
            finally:
                outer.rollback()
                logger.info("Dry run: all database changes rolled back; raw files kept in %s", raw_dir)
    finally:
        manager.dispose()


def fetch_workbooks(client: HttpClient, settings: Settings, storage: RawStorageManager, start: date, end: date,
                    sleep: Callable[[float], None]) -> tuple[list[PreparedWorkbook], list[api.ListingEntry]]:
    """Listing + one download and one raw copy per workbook version. Raises on any failure."""
    entries: list[api.ListingEntry] = []
    for month in api.listing_months(start, end):
        month_entries, body = api.list_files(client, settings.grid_india_api_base_url, month)
        stored = storage.save_bytes(body, source=RAW_SOURCE, dataset=RAW_DATASET, extension="json",
                                    partition="listing",
                                    metadata={"file_type": api.PSP_FILE_TYPE, "fy": api.fy_label(month),
                                              "month": f"{month.month:02d}", "entries": len(month_entries)})
        logger.info("Listing %s %s: %d entries (raw %s)", api.fy_label(month), f"{month.month:02d}",
                    len(month_entries), stored.key)
        entries.extend(month_entries)
        sleep(api.REQUEST_PAUSE_SECONDS)

    selected = api.select_workbooks(entries, start, end)
    prepared = []
    for index, entry in enumerate(selected):
        if index:
            sleep(api.REQUEST_PAUSE_SECONDS)
        content, url, content_type, retrieved_at = api.download(client, settings.grid_india_cdn_base_url, entry)
        avail = api.availability(entry.report_date, entry.created_on)
        stored = storage.save_bytes(
            content, source=RAW_SOURCE, dataset=RAW_DATASET, extension=entry.file_name.rsplit(".", 1)[-1].lower(),
            ingested_at=retrieved_at, partition="xls",
            metadata={"source_url": url, "document_name": entry.file_name, "report_date": entry.report_date.isoformat(),
                      "listing_entry": entry.raw, "retrieved_at": retrieved_at.isoformat(),
                      "availability_basis": avail.basis, "available_at": avail.available_at.isoformat(),
                      "published_at": avail.published_at.isoformat() if avail.published_at else None})
        logger.info("Stored workbook %s for %s (%d bytes, sha256=%s, %s)", entry.file_name, entry.report_date,
                    stored.size_bytes, stored.sha256, avail.basis)
        document = RawDocument(content=content, retrieved_at=retrieved_at, extension="xls", content_type=content_type,
                               published_at=avail.published_at, source_url=url, document_name=entry.file_name,
                               source_report_date=entry.report_date)
        prepared.append(PreparedWorkbook(entry, document, stored, avail))
    return prepared, entries


def run_grid_india_psp(
    *,
    start_date: date,
    end_date: date | None = None,
    sections: Sequence[str] | None = None,
    dry_run: bool = False,
    reprocess: bool = False,
    settings: Settings | None = None,
    http_client: HttpClient | None = None,
    database: TransactionalDatabase | None = None,
    raw_storage: RawStorageManager | None = None,
    today: date | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> PspRunSummary:
    """Download the Daily PSP workbooks for ``start_date``..``end_date`` once and load the requested sections.

    Raises:
        ValueError: Invalid, unpublishable or longer than 31-day range, or an unknown section.
        ConfigurationError: Region/state reference rows missing (outside dry runs).
    """
    settings = settings or get_settings()
    end_date = end_date or start_date
    today = today or api.today_ist()
    api.validate_date_range(start_date, end_date, today=today)
    sections = list(sections) if sections is not None else list(SECTION_PIPELINES)  # default: every section
    unknown = [s for s in sections if s not in SECTION_PIPELINES]
    if unknown or not sections:
        raise ValueError(f"unknown section(s) {unknown}; available: {sorted(SECTION_PIPELINES)}")
    summary = PspRunSummary(start_date, end_date, dry_run)

    with _execution_context(settings, dry_run, database, raw_storage) as (db, storage, raw_dir):
        summary.raw_data_dir = raw_dir
        with db.connect() as connection:
            missing_ref = missing_geography(connection, settings.db_schema)
            if missing_ref and dry_run:
                seed_reference(connection, settings.db_schema, None)
                summary.seeded_reference = True
            elif missing_ref:
                raise ConfigurationError(f"Region/state reference rows missing ({len(missing_ref)}). "
                                         "Run: python -m scripts.ingest_cea reference")

        client = http_client or HttpClient(settings=settings)
        workbooks: list[PreparedWorkbook] = []
        fetch_error: BaseException | None = None
        try:
            workbooks, _ = fetch_workbooks(client, settings, storage, start_date, end_date, sleep)
        except Exception as exc:  # recorded on every section's run below
            fetch_error = exc
            summary.fetch_error = f"{type(exc).__name__}: {exc}"
            logger.error("Fetching Daily PSP workbooks failed: %s", summary.fetch_error)
        finally:
            if http_client is None:
                client.close()
        summary.workbooks = [w.entry.file_name for w in workbooks]
        covered = {w.entry.report_date for w in workbooks}
        missing_days = [] if fetch_error else [
            start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)
            if start_date + timedelta(days=i) not in covered]
        summary.missing = [d.isoformat() for d in missing_days]

        parameters = {
            "start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "sections": list(sections),
            "today": today.isoformat(), "dry_run": dry_run, "reprocess": reprocess,
            "workbooks": [{"file": w.entry.file_name, "report_date": w.entry.report_date.isoformat(),
                           "created_on": w.entry.created_on.isoformat() if w.entry.created_on else None,
                           "availability_basis": w.availability.basis, "raw_key": w.stored.key} for w in workbooks],
        }
        for name in sections:
            pipeline_class = SECTION_PIPELINES[name]
            pipeline = pipeline_class(database=db, raw_storage=storage, settings=settings, http_client=http_client,
                                      sleep=sleep, workbooks=workbooks, fetch_error=fetch_error)
            pipeline.reload_duplicate_documents = reprocess
            result = pipeline.run({**parameters, "section": pipeline_class.section_label})
            issues = [DataQualityIssue(
                pipeline_class.target_table, "MISSING_DAY",
                f"No Daily PSP workbook listed for {day}" + (" (may not be published yet)" if day >= today - timedelta(days=1) else ""),
                Severity.INFO if day >= today - timedelta(days=1) else Severity.WARNING, day.isoformat())
                for day in missing_days]
            if issues:
                with db.connect() as connection:
                    IngestionRepository(settings.db_schema).record_issues(connection, result.ingestion_run_id, issues)
                result.data_quality_issues += len(issues)
            summary.sections[name] = SectionSummary(name, result.status, result, result.error)
    return summary
