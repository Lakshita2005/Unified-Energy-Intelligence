"""Data structures shared by every ingestion pipeline."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any


class RunStatus(str, enum.Enum):
    """Values allowed by the ``pi.ingestion_run.run_status`` CHECK constraint."""

    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class Severity(str, enum.Enum):
    """Values allowed by the ``pi.data_quality_issue.severity`` CHECK constraint.

    ``ERROR`` issues reject the record; ``WARNING`` and ``INFO`` issues are
    recorded but the record is still loaded.
    """

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass(frozen=True)
class SourceDefinition:
    """A row in ``pi.data_source``, identified by ``code``."""

    code: str
    name: str
    url: str | None = None


@dataclass(frozen=True)
class DatasetDefinition:
    """A row in ``pi.source_dataset``, identified by ``code`` (globally unique)."""

    code: str
    name: str
    endpoint_or_report: str | None = None
    native_granularity: str | None = None
    expected_frequency: str | None = None
    default_timezone: str = "Asia/Kolkata"


@dataclass(frozen=True)
class RawDocument:
    """A payload exactly as retrieved from a source.

    Attributes:
        content: Unmodified bytes as received.
        retrieved_at: When this system retrieved the payload (timezone-aware).
        extension: File extension for raw storage, e.g. ``"json"`` or ``"csv"``.
        content_type: MIME type reported by the source, if any.
        published_at: When the source published the payload, if known. Kept
            separate from ``retrieved_at`` and never derived from it.
        source_url: Where the payload was retrieved from (avoid embedding secrets).
        document_name: Human-readable name, e.g. the source's file name.
        source_report_date: The business date the document reports on, if any.
    """

    content: bytes
    retrieved_at: datetime
    extension: str
    content_type: str | None = None
    published_at: datetime | None = None
    source_url: str | None = None
    document_name: str | None = None
    source_report_date: date | None = None

    def __post_init__(self) -> None:
        if self.retrieved_at.tzinfo is None:
            raise ValueError("RawDocument.retrieved_at must be timezone-aware")
        if self.published_at is not None and self.published_at.tzinfo is None:
            raise ValueError("RawDocument.published_at must be timezone-aware")


@dataclass(frozen=True)
class ParsedRecord:
    """One record parsed from a document.

    Attributes:
        key: Stable identifier of the record within the source, used to trace
            data-quality issues back to it (e.g. ``"2026-11-10|block=5"``).
        values: Target-table column -> value, using proper Python types
            (``date``, ``int``, ``Decimal``, aware ``datetime``) and preserving
            the source's values. Lineage columns (``ingestion_run_id``,
            ``document_id``, ``retrieved_at``, ``source_published_at``) are filled
            in by the framework.
        raw: The original source record, unmodified.
    """

    key: str
    values: dict[str, Any]
    raw: Any = None


@dataclass(frozen=True)
class ParseFailure:
    """A source record that could not be parsed. Recorded, never discarded."""

    key: str | None
    raw: Any
    description: str


@dataclass
class ParseResult:
    """Output of a pipeline's ``parse`` step."""

    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)

    @property
    def records_read(self) -> int:
        return len(self.records) + len(self.failures)


@dataclass(frozen=True)
class ValidationIssue:
    """A problem found by a validator."""

    code: str
    description: str
    severity: Severity = Severity.ERROR
    field: str | None = None


@dataclass(frozen=True)
class DataQualityIssue:
    """A row to be written to ``pi.data_quality_issue``."""

    table_name: str
    issue_code: str
    issue_description: str
    severity: Severity
    source_record_key: str | None = None


@dataclass
class IngestionResult:
    """Summary of one ingestion run, returned by ``BaseIngestionPipeline.run``."""

    ingestion_run_id: int
    dataset_code: str
    status: RunStatus = RunStatus.RUNNING
    records_read: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_unchanged: int = 0
    records_rejected: int = 0
    data_quality_issues: int = 0
    documents_processed: int = 0
    duplicate_documents: int = 0
    document_ids: list[int] = field(default_factory=list)
    raw_storage_keys: list[str] = field(default_factory=list)
    error: str | None = None
