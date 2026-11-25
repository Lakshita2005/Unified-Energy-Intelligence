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

After the price run, a second run (dataset ``IEX_*_SNAPSHOT``) loads the
snapshot document's volumes and congestion into ``iex_market_snapshot``,
without another request. It is skipped, and reported, while that table does not
exist (migration ``0013_iex_market_snapshot`` not applied).

:func:`backfill_snapshots` loads ``iex_market_snapshot`` from snapshot files
already in raw storage (no IEX call). Each file is checked against the SHA-256
in its metadata, files are replayed in retrieval order, and identical content
is loaded once. Files whose content the snapshot dataset already registered are
skipped, so a backfill can be repeated safely.
"""

from __future__ import annotations

import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, fields
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import inspect

from config.settings import Settings, get_settings
from src.common.checksums import sha256_bytes
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError, StorageError
from src.common.http_client import HttpClient
from src.common.logging_config import get_logger
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager, StoredObject, create_raw_storage_manager
from src.ingestion.base import TransactionalDatabase
from src.ingestion.models import IngestionResult, RawDocument, RunStatus
from src.pipelines.iex import api
from src.pipelines.iex.pipeline import (
    SNAPSHOT_TABLE,
    IexDamPipeline,
    IexDamSnapshotPipeline,
    IexGdamPipeline,
    IexGdamSnapshotPipeline,
    IexRtmPipeline,
    IexRtmSnapshotPipeline,
    IexSnapshotPipeline,
    name_range,
)
from src.pipelines.iex.reference import missing_reference, seed_reference

logger = get_logger(__name__)

Kind = Literal["dam", "gdam", "rtm"]
PIPELINES: dict[str, type[IexDamPipeline | IexGdamPipeline | IexRtmPipeline]] = {
    "dam": IexDamPipeline, "gdam": IexGdamPipeline, "rtm": IexRtmPipeline,
}
SNAPSHOT_PIPELINES: dict[str, type[IexSnapshotPipeline]] = {
    "dam": IexDamSnapshotPipeline, "gdam": IexGdamSnapshotPipeline, "rtm": IexRtmSnapshotPipeline,
}
SNAPSHOT_MIGRATION = "0013_iex_market_snapshot"
STORAGE_SOURCE = "iex"


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
    snapshot: IngestionResult | None = None
    snapshot_skipped: str | None = None

    @property
    def ok(self) -> bool:
        return self.status is RunStatus.SUCCESS and (self.snapshot is None
                                                      or self.snapshot.status is RunStatus.SUCCESS)


def snapshot_table_exists(db: TransactionalDatabase, schema: str) -> bool:
    with db.connect() as connection:
        return inspect(connection).has_table(SNAPSHOT_TABLE, schema=schema)


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
        if not pipeline.snapshot_documents:
            summary.snapshot_skipped = "no snapshot document was stored by the price run"
        elif not snapshot_table_exists(db, settings.db_schema):
            summary.snapshot_skipped = f"{settings.db_schema}.{SNAPSHOT_TABLE} does not exist (apply {SNAPSHOT_MIGRATION})"
        else:
            snapshot = SNAPSHOT_PIPELINES[kind](database=db, raw_storage=storage, settings=settings, sleep=sleep,
                                                documents=pipeline.snapshot_documents)
            summary.snapshot = snapshot.run({**parameters, "price_ingestion_run_id": summary.result.ingestion_run_id})
        if summary.snapshot_skipped:
            logger.info("IEX %s snapshot volumes not loaded: %s", kind, summary.snapshot_skipped)
    return summary


# ------------------------------------------------------------------------ backfill from raw storage

@dataclass
class BackfillSummary:
    """Result of one market's snapshot backfill."""

    kind: Kind
    dry_run: bool
    files: list[str] = field(default_factory=list)
    identical_files: int = 0
    result: IngestionResult | None = None
    raw_data_dir: Path | None = None

    @property
    def ok(self) -> bool:
        return self.result is not None and self.result.status is RunStatus.SUCCESS


def _stored_object(meta: dict[str, Any]) -> StoredObject:
    values = {f.name: meta[f.name] for f in fields(StoredObject) if f.name in meta}
    values["ingested_at"] = datetime.fromisoformat(str(meta["ingested_at"]))
    return StoredObject(**values)


def stored_snapshot_documents(source: RawStorageManager, market: api.Market, *, start: date | None = None,
                              end: date | None = None) -> tuple[list[tuple[RawDocument, StoredObject]], int]:
    """Stored ``market-snapshot`` files of ``market`` in retrieval order, verified, one per distinct content.

    Only files whose requested range overlaps ``start``..``end`` (if given) are returned. Returns the documents
    and the number of byte-identical re-fetches left out.

    Raises:
        StorageError: A file does not match the size or SHA-256 recorded in its metadata.
    """
    prefix = f"{market.dataset_code.lower()}__{api.MKT_AREA.lower()}__"
    candidates = []
    for key in source.list_keys(STORAGE_SOURCE, market.dataset_code.lower()):
        meta = source.read_metadata(key)
        name = (meta.get("metadata") or {}).get("document_name") or ""
        if not name.startswith(prefix):
            continue
        first, last = name_range(name)
        if (start and last < start) or (end and first > end):
            continue
        candidates.append((datetime.fromisoformat(str(meta["ingested_at"])), key, meta, name, first, last))
    documents, seen, identical = [], set(), 0
    for retrieved_at, key, meta, name, first, last in sorted(candidates):
        content = source.read_bytes(key)
        if len(content) != meta.get("size_bytes") or sha256_bytes(content) != meta.get("sha256"):
            raise StorageError(f"raw document {key}: size or SHA-256 does not match its metadata; not loaded")
        if meta["sha256"] in seen:
            identical += 1
            continue
        seen.add(meta["sha256"])
        info = meta["metadata"]
        documents.append((RawDocument(content=content, retrieved_at=retrieved_at, extension="json",
                                      content_type=info.get("content_type"), published_at=None,
                                      source_url=info.get("source_url"), document_name=name,
                                      source_report_date=first if first == last else None),
                          _stored_object(meta)))
    return documents, identical


def backfill_snapshots(
    kind: Kind,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    dry_run: bool = False,
    settings: Settings | None = None,
    database: TransactionalDatabase | None = None,
    raw_source: RawStorageManager | None = None,
) -> BackfillSummary:
    """Load ``iex_market_snapshot`` for one market from snapshot files already in raw storage (no IEX call).

    Raises:
        ValueError: No stored snapshot file for the market (and range).
        ConfigurationError: ``iex_market_snapshot`` does not exist.
        StorageError: A stored file fails checksum verification.
    """
    settings = settings or get_settings()
    source = raw_source or create_raw_storage_manager(settings)
    market = api.MARKETS[kind]
    documents, identical = stored_snapshot_documents(source, market, start=start_date, end=end_date)
    if not documents:
        raise ValueError(f"no stored {market.market_code} snapshot files to backfill"
                         + (f" for {start_date}..{end_date}" if start_date or end_date else ""))
    summary = BackfillSummary(kind, dry_run, [stored.key for _, stored in documents], identical)

    @contextmanager
    def injected() -> Iterator[tuple[TransactionalDatabase, RawStorageManager, Path | None]]:
        yield database, source, None  # type: ignore[misc]

    context = injected() if database is not None else _execution_context(settings, dry_run)
    with context as (db, storage, raw_dir):
        summary.raw_data_dir = raw_dir
        if not snapshot_table_exists(db, settings.db_schema):
            raise ConfigurationError(f"{settings.db_schema}.{SNAPSHOT_TABLE} does not exist; apply {SNAPSHOT_MIGRATION}")
        pipeline = SNAPSHOT_PIPELINES[kind](database=db, raw_storage=storage, settings=settings, documents=documents)
        summary.result = pipeline.run({
            "backfill": True, "market": market.market_code, "dry_run": dry_run,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
            "documents": [{"raw_key": stored.key, "sha256": stored.sha256, "name": document.document_name}
                          for document, stored in documents],
        })
    return summary
