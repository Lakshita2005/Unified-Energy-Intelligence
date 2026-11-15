"""Base class for all source-specific ingestion pipelines.

A source pipeline declares *what* it ingests and implements only the
source-specific steps:

* :meth:`BaseIngestionPipeline.fetch` - retrieve raw documents (no parsing);
* :meth:`BaseIngestionPipeline.parse` - turn one document into records (no DB access);
* :meth:`BaseIngestionPipeline.validator` - optional validation rules.

:meth:`BaseIngestionPipeline.run` provides everything else:

1. registers the source and dataset (``pi.data_source``, ``pi.source_dataset``)
   and creates an ``pi.ingestion_run`` row with status RUNNING (committed at once);
2. calls ``fetch`` with retries on transient network errors;
3. for each document, *before any database work*, stores the raw bytes under
   ``<source>/<dataset>/YYYY/MM/DD/`` with a SHA-256 checksum (never deleted);
4. parses and validates the records;
5. in **one transaction per document**: registers ``pi.source_document``
   (skipping content already ingested), loads records idempotently via
   :class:`~src.loaders.table_loader.TableLoader`, writes every rejected or
   flagged record to ``pi.data_quality_issue`` and saves the full rejected
   records to raw storage. Any error rolls the whole document back;
6. finishes the run as SUCCESS, PARTIAL (records rejected) or FAILED
   (with ``error_message``).

Example::

    class DamPricePipeline(BaseIngestionPipeline):
        source = SourceDefinition("IEX", "Indian Energy Exchange", "https://www.iexindia.com/")
        dataset = DatasetDefinition("IEX_DAM_PRICES", "IEX day-ahead prices")
        target_table = "iex_market_price"
        natural_key = ("market_code", "area_code", "delivery_date", "time_block")

        def fetch(self, context): ...
        def parse(self, document, context): ...
"""

from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping, MutableMapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Protocol

from sqlalchemy.engine import Connection

from config.settings import Settings, get_settings
from src.common.database import DatabaseManager
from src.common.http_client import HttpClient
from src.common.logging_config import get_logger
from src.common.raw_storage import RawStorageManager, StoredObject, create_raw_storage_manager
from src.common.retry import TRANSIENT_NETWORK_ERRORS, retry_call
from src.ingestion.models import (
    DataQualityIssue,
    DatasetDefinition,
    IngestionResult,
    ParsedRecord,
    ParseResult,
    RawDocument,
    RunStatus,
    Severity,
    SourceDefinition,
    ValidationIssue,
)
from src.ingestion.repositories import IngestionRepository
from src.ingestion.validation import NoOpValidator, Validator, validate_records
from src.loaders.table_loader import (
    DEFAULT_LINEAGE_COLUMNS,
    ConflictPolicy,
    LoadIssue,
    LoadResult,
    RowToLoad,
    TableLoader,
)

logger = get_logger(__name__)

_MAX_RAW_IN_DESCRIPTION = 2000
_MAX_ERROR_MESSAGE = 4000


class TransactionalDatabase(Protocol):
    """Anything providing transactional connections (e.g. :class:`DatabaseManager`)."""

    def connect(self) -> AbstractContextManager[Connection]: ...


class RunLogger(logging.LoggerAdapter):  # type: ignore[type-arg]
    """Prefixes messages with the run id and dataset, and adds them as log ``extra`` fields."""

    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> tuple[Any, MutableMapping[str, Any]]:
        extra = dict(self.extra or {})
        kwargs["extra"] = {**extra, **kwargs.get("extra", {})}
        return f"[run={extra.get('ingestion_run_id')} dataset={extra.get('dataset_code')}] {msg}", kwargs


@dataclass(frozen=True)
class RunContext:
    """Information about the current run, passed to source-specific hooks."""

    ingestion_run_id: int
    source_id: int
    dataset_id: int
    source: SourceDefinition
    dataset: DatasetDefinition
    parameters: Mapping[str, Any]
    log: RunLogger


def _describe(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_MESSAGE]


def _with_raw(description: str, raw: Any) -> str:
    """Append the original record to an issue description so it is never lost."""
    if raw is None:
        return description
    raw_text = json.dumps(raw, default=str, ensure_ascii=False)
    if len(raw_text) > _MAX_RAW_IN_DESCRIPTION:
        raw_text = raw_text[:_MAX_RAW_IN_DESCRIPTION] + "...(truncated; full record in raw storage)"
    return f"{description} | raw={raw_text}"


class BaseIngestionPipeline(ABC):
    """Template for ingestion pipelines. Subclass, set the class attributes and
    implement :meth:`fetch` and :meth:`parse`.

    Class attributes:
        source: Source registered in ``pi.data_source``.
        dataset: Dataset registered in ``pi.source_dataset``.
        target_table: Existing table in ``DB_SCHEMA`` that receives the records.
        natural_key: Columns identifying a record (used for duplicate detection).
        nullable_key_columns: Natural key columns allowed to be NULL (matched null-safely).
        conflict_policy: What to do when a known record arrives with new values.
        pipeline_version: Stored in ``ingestion_run.pipeline_version``.
        store_raw_payload_in_db: Also store JSON documents in
            ``source_document.raw_payload``. Off by default: the raw file is the
            authoritative copy, and JSONB does not preserve formatting or key order.
        fetch_attempts: Attempts for the whole fetch step on transient network
            errors (individual HTTP requests are already retried by HttpClient).
        extra_lineage_columns: Columns that describe where a value came from (e.g. a
            source revision number) rather than what it says. Like the default
            lineage columns, they are stored but never make a row "changed", so a
            new version is created only when a measured value changes. Must not
            overlap the natural key. Empty by default (no behaviour change).
    """

    source: ClassVar[SourceDefinition]
    dataset: ClassVar[DatasetDefinition]
    target_table: ClassVar[str]
    natural_key: ClassVar[tuple[str, ...]]
    nullable_key_columns: ClassVar[tuple[str, ...]] = ()
    conflict_policy: ClassVar[ConflictPolicy] = ConflictPolicy.VERSION
    pipeline_version: ClassVar[str] = "0.1.0"
    store_raw_payload_in_db: ClassVar[bool] = False
    fetch_attempts: ClassVar[int] = 2
    fetch_retry_on: ClassVar[tuple[type[BaseException], ...]] = TRANSIENT_NETWORK_ERRORS
    # Opt-in per instance: when True, byte-identical documents are re-parsed and their
    # records loaded again (unchanged records are skipped by the loader). Off by default.
    reload_duplicate_documents: bool = False
    extra_lineage_columns: ClassVar[frozenset[str]] = frozenset()

    def __init__(
        self,
        *,
        database: TransactionalDatabase | None = None,
        raw_storage: RawStorageManager | None = None,
        settings: Settings | None = None,
        http_client: HttpClient | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        for attribute in ("source", "dataset", "target_table", "natural_key"):
            if not hasattr(self, attribute):
                raise TypeError(f"{type(self).__name__} must define class attribute '{attribute}'")
        self.settings = settings or get_settings()
        self.database: TransactionalDatabase = database or DatabaseManager.from_settings(self.settings)
        self.raw_storage = raw_storage or create_raw_storage_manager(self.settings)
        self.repository = IngestionRepository(self.settings.db_schema)
        overlap = sorted(set(self.extra_lineage_columns) & set(self.natural_key))
        if overlap:
            raise TypeError(f"{type(self).__name__}: extra_lineage_columns overlap the natural key: {overlap}")
        self.loader = TableLoader(
            self.target_table,
            schema=self.settings.db_schema,
            natural_key=self.natural_key,
            nullable_key_columns=self.nullable_key_columns,
            conflict_policy=self.conflict_policy,
            ignore_columns=DEFAULT_LINEAGE_COLUMNS | self.extra_lineage_columns,
        )
        self._http_client = http_client
        self._owns_http_client = False
        self._sleep = sleep

    # ------------------------------------------------- source-specific hooks

    @abstractmethod
    def fetch(self, context: RunContext) -> Iterable[RawDocument]:
        """Retrieve raw documents from the source, unmodified.

        Set ``retrieved_at`` to the actual retrieval time and ``published_at``
        only from what the source states. Use :attr:`http` for HTTP requests.
        """

    @abstractmethod
    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        """Parse one document into records. Must not access the database.

        Return unparseable records as :class:`ParseFailure` rather than
        dropping them. Preserve source values and timestamps as published.
        """

    def validator(self) -> Validator:
        """Validation rules for parsed records. Override to add rules."""
        return NoOpValidator()

    # ----------------------------------------------------------- utilities

    @property
    def http(self) -> HttpClient:
        """Shared HTTP client (timeouts and retries from settings), closed after the run."""
        if self._http_client is None:
            self._http_client = HttpClient(settings=self.settings)
            self._owns_http_client = True
        return self._http_client

    @property
    def storage_source(self) -> str:
        """Raw storage folder for this source."""
        return self.source.code.lower()

    @property
    def storage_dataset(self) -> str:
        """Raw storage folder for this dataset."""
        return self.dataset.code.lower()

    def raw_storage_partition(self, document: RawDocument, context: RunContext) -> str | None:
        """Optional sub-folder under the dataset folder (e.g. a location). None by default."""
        return None

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        """Optional document-level quality issues (e.g. an incomplete day). None by default.

        They are recorded with the record-level issues in the document's
        transaction and do not reject any record.
        """
        return []

    # ---------------------------------------------------------- execution

    def run(self, parameters: Mapping[str, Any] | None = None) -> IngestionResult:
        """Execute one ingestion run. Returns a summary; failures are recorded, not raised.

        Raises:
            ConfigurationError, IngestionError: If registration fails before a
                run row could be created (e.g. inactive dataset).
        """
        params = dict(parameters or {})
        started = time.perf_counter()

        with self.database.connect() as connection:
            source_id = self.repository.ensure_source(connection, self.source)
            dataset_id = self.repository.ensure_dataset(connection, source_id, self.dataset)
            run_id = self.repository.start_run(
                connection, dataset_id, pipeline_version=self.pipeline_version, parameters=params
            )

        log = RunLogger(logger, {"ingestion_run_id": run_id, "dataset_code": self.dataset.code})
        context = RunContext(run_id, source_id, dataset_id, self.source, self.dataset, params, log)
        result = IngestionResult(ingestion_run_id=run_id, dataset_code=self.dataset.code)
        published: list[datetime] = []
        log.info("Started ingestion run: source=%s target=%s.%s pipeline=%s %s parameters=%s",
                 self.source.code, self.settings.db_schema, self.target_table,
                 type(self).__name__, self.pipeline_version, params)

        try:
            documents = retry_call(
                lambda: list(self.fetch(context)),
                attempts=self.fetch_attempts,
                backoff_factor_seconds=self.settings.http_backoff_factor_seconds,
                backoff_max_seconds=self.settings.http_backoff_max_seconds,
                retry_on=self.fetch_retry_on,
                sleep=self._sleep,
                description=f"[run={run_id}] fetch {self.dataset.code}",
            )
            log.info("Fetched %d document(s)", len(documents))
            for document in documents:
                self._process_document(document, context, result)
                if document.published_at is not None:
                    published.append(document.published_at)
            result.status = RunStatus.PARTIAL if result.records_rejected else RunStatus.SUCCESS
        except Exception as exc:
            result.status = RunStatus.FAILED
            result.error = _describe(exc)
            log.exception("Ingestion run failed: %s", result.error)
        except BaseException as exc:  # KeyboardInterrupt, SystemExit: record, then propagate
            result.status = RunStatus.FAILED
            result.error = f"Interrupted: {_describe(exc)}"
            self._finish(context, result, published)
            raise
        finally:
            self._close_http_client()

        self._finish(context, result, published)
        log.info(
            "Finished with status %s in %.2fs: documents=%d duplicate_documents=%d read=%d inserted=%d "
            "updated=%d unchanged=%d rejected=%d quality_issues=%d",
            result.status.value, time.perf_counter() - started, result.documents_processed,
            result.duplicate_documents, result.records_read, result.records_inserted, result.records_updated,
            result.records_unchanged, result.records_rejected, result.data_quality_issues,
        )
        return result

    # ------------------------------------------------------------ internals

    def _finish(self, context: RunContext, result: IngestionResult, published: list[datetime]) -> None:
        with self.database.connect() as connection:
            self.repository.finish_run(
                connection,
                context.ingestion_run_id,
                status=result.status,
                records_read=result.records_read,
                records_inserted=result.records_inserted,
                records_updated=result.records_updated,
                records_rejected=result.records_rejected,
                source_published_at=max(published, default=None),
                error_message=result.error,
            )

    def _close_http_client(self) -> None:
        if self._owns_http_client and self._http_client is not None:
            self._http_client.close()
            self._http_client = None
            self._owns_http_client = False

    def _store_raw(self, document: RawDocument, context: RunContext) -> StoredObject:
        stored = self.raw_storage.save_bytes(
            document.content,
            source=self.storage_source,
            dataset=self.storage_dataset,
            extension=document.extension,
            ingested_at=document.retrieved_at,
            partition=self.raw_storage_partition(document, context),
            metadata={
                "ingestion_run_id": context.ingestion_run_id,
                "source_code": self.source.code,
                "dataset_code": self.dataset.code,
                "source_url": document.source_url,
                "document_name": document.document_name,
                "content_type": document.content_type,
                "retrieved_at": document.retrieved_at.isoformat(),
                "published_at": document.published_at.isoformat() if document.published_at else None,
            },
        )
        context.log.info("Stored raw document %s (%d bytes, sha256=%s)", stored.key, stored.size_bytes, stored.sha256)
        return stored

    def _issue(self, key: str | None, issue: ValidationIssue | LoadIssue, raw: Any) -> DataQualityIssue:
        code = issue.code if isinstance(issue, ValidationIssue) else issue.issue_code
        return DataQualityIssue(
            table_name=self.target_table,
            source_record_key=key,
            issue_code=code,
            issue_description=_with_raw(issue.description, raw),
            severity=issue.severity,
        )

    @staticmethod
    def _lineage(columns: frozenset[str], run_id: int, document_id: int) -> dict[str, Any]:
        """Run and document references for the lineage columns the target table has."""
        lineage: dict[str, Any] = {}
        if "ingestion_run_id" in columns:
            lineage["ingestion_run_id"] = run_id
        if "document_id" in columns:
            lineage["document_id"] = document_id
        return lineage

    def _process_document(self, document: RawDocument, context: RunContext, result: IngestionResult) -> None:
        log = context.log

        # 1. Raw first: the payload is preserved even if everything after fails.
        stored = self._store_raw(document, context)
        result.raw_storage_keys.append(stored.key)

        # 2. Parse and validate (pure; no database access).
        parsed = self.parse(document, context)
        validation = validate_records(parsed.records, self.validator())
        log.info("Parsed %d record(s): %d parse failure(s), %d failed validation, %d accepted",
                 parsed.records_read, len(parsed.failures), len(validation.rejected), len(validation.accepted))

        issues: list[DataQualityIssue] = []
        rejected_records: list[dict[str, Any]] = []
        for failure in parsed.failures:
            issues.append(DataQualityIssue(self.target_table, "PARSE_ERROR",
                                           _with_raw(failure.description, failure.raw), Severity.ERROR, failure.key))
            rejected_records.append({"key": failure.key, "stage": "parse", "raw": failure.raw,
                                     "issues": [{"code": "PARSE_ERROR", "description": failure.description}]})
        for record, record_issues in validation.rejected:
            issues.extend(self._issue(record.key, issue, record.raw) for issue in record_issues)
            rejected_records.append({"key": record.key, "stage": "validation", "raw": record.raw,
                                     "issues": [{"code": i.code, "description": i.description,
                                                 "severity": i.severity.value} for i in record_issues]})
        for record, record_issues in validation.accepted:
            issues.extend(self._issue(record.key, issue, record.raw) for issue in record_issues)
        issues.extend(self.document_issues(document, parsed, context))

        raw_payload = None
        if self.store_raw_payload_in_db and document.extension.lower() == "json":
            raw_payload = json.loads(document.content)

        # 3. Register document, load, and record issues in ONE transaction:
        #    any failure rolls back the document row, loaded records and issues together.
        result.records_read += parsed.records_read
        load: LoadResult | None = None
        with self.database.connect() as connection:
            document_id, is_new = self.repository.register_document(
                connection, dataset_id=context.dataset_id, run_id=context.ingestion_run_id,
                document=document, stored=stored, raw_payload=raw_payload,
            )
            if not is_new and self.reload_duplicate_documents:
                # Opt-in reprocessing (e.g. after a parser fix): load the records again;
                # the natural-key comparison skips everything already stored unchanged.
                issues.append(DataQualityIssue(
                    "source_document", "REPROCESSED_DOCUMENT",
                    f"Content already ingested as document_id={document_id}; records re-parsed and reloaded",
                    Severity.INFO, stored.sha256,
                ))
                load = self._load_document(connection, document, context, document_id, validation.accepted,
                                           issues, rejected_records, stored, result)
            elif not is_new:
                # Identical content was already ingested; its records and quality
                # issues were recorded by the run that first loaded it.
                issues = [DataQualityIssue(
                    "source_document", "DUPLICATE_DOCUMENT",
                    f"Content already ingested as document_id={document_id}; records not reloaded",
                    Severity.INFO, stored.sha256,
                )]
                self.repository.record_issues(connection, context.ingestion_run_id, issues)
            else:
                load = self._load_document(connection, document, context, document_id, validation.accepted,
                                           issues, rejected_records, stored, result)

        # Counters are only updated once the transaction has committed.
        result.document_ids.append(document_id)
        result.data_quality_issues += len(issues)
        if load is None:
            result.duplicate_documents += 1
            log.info("Document sha256=%s already ingested as document_id=%d; skipped loading",
                     stored.sha256, document_id)
            return
        rejected = len(parsed.failures) + len(validation.rejected) + load.rejected
        result.documents_processed += 1
        result.records_inserted += load.inserted
        result.records_updated += load.updated
        result.records_unchanged += load.unchanged
        result.records_rejected += rejected
        log.info("Document_id=%d committed: inserted=%d updated=%d unchanged=%d rejected=%d issues=%d",
                 document_id, load.inserted, load.updated, load.unchanged, rejected, len(issues))

    def _load_document(
        self,
        connection: Connection,
        document: RawDocument,
        context: RunContext,
        document_id: int,
        accepted: list[tuple[ParsedRecord, list[ValidationIssue]]],
        issues: list[DataQualityIssue],
        rejected_records: list[dict[str, Any]],
        stored: StoredObject,
        result: IngestionResult,
    ) -> LoadResult:
        """Load accepted records and record issues, inside the caller's transaction."""
        columns = self.loader.column_names(connection)
        rows = []
        for record, _ in accepted:
            row = {**record.values, **self._lineage(columns, context.ingestion_run_id, document_id)}
            # Retrieval and publication times are recorded separately; a
            # timestamp provided by the source record itself always wins.
            if "retrieved_at" in columns:
                row.setdefault("retrieved_at", document.retrieved_at)
            if "source_published_at" in columns and document.published_at is not None:
                row.setdefault("source_published_at", document.published_at)
            rows.append(RowToLoad(record.key, row, record.raw))

        load = self.loader.load(connection, rows)
        for load_issue in load.issues:
            issues.append(self._issue(load_issue.key, load_issue, load_issue.raw))
            if load_issue.severity is Severity.ERROR:
                rejected_records.append({"key": load_issue.key, "stage": "load", "raw": load_issue.raw,
                                         "issues": [{"code": load_issue.issue_code,
                                                     "description": load_issue.description}]})

        self.repository.record_issues(connection, context.ingestion_run_id, issues)

        if rejected_records:
            # Written inside the transaction: if it fails, the load rolls back too.
            # Decimals are written as strings to keep their exact digits.
            rejects_document = {"ingestion_run_id": context.ingestion_run_id, "document_id": document_id,
                                "document_sha256": stored.sha256, "rejected": rejected_records}
            rejects = self.raw_storage.save_bytes(
                json.dumps(rejects_document, default=str, ensure_ascii=False, indent=2).encode("utf-8"),
                source=self.storage_source, dataset=f"{self.storage_dataset}-rejected", extension="json",
                ingested_at=document.retrieved_at, partition=self.raw_storage_partition(document, context),
                metadata={"ingestion_run_id": context.ingestion_run_id, "document_id": document_id},
            )
            result.raw_storage_keys.append(rejects.key)
            context.log.warning("%d rejected record(s) saved to %s", len(rejected_records), rejects.key)
        return load
