"""Value types shared by the scheduler, checkers, repository and worker."""

from __future__ import annotations

import enum
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


class CheckOutcome(str, enum.Enum):
    """Result of one availability check (``pi_jobs.availability_check.outcome``)."""

    NEW_DATA = "NEW_DATA"                    # at least one new or revised item; jobs were considered
    NO_NEW_DATA = "NO_NEW_DATA"              # the source has nothing that is not stored already
    NOT_YET_PUBLISHED = "NOT_YET_PUBLISHED"  # expected items are not published yet (404, empty data)
    LATE = "LATE"                            # an expected item is still missing after its deadline
    SOURCE_ERROR = "SOURCE_ERROR"            # the check itself failed (network, 5xx, unexpected format)


class JobStatus(str, enum.Enum):
    """Life cycle of ``pi_jobs.job``."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    PARTIAL = "PARTIAL"            # loaded, but some records were rejected (final; not retried)
    NOT_PUBLISHED = "NOT_PUBLISHED"  # the runner found the data not (or no longer) available; re-detected later
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


ACTIVE_STATUSES: frozenset[JobStatus] = frozenset({JobStatus.PENDING, JobStatus.RUNNING, JobStatus.RETRY_WAIT})


class ErrorClass(str, enum.Enum):
    """How a failure is handled."""

    TRANSIENT = "TRANSIENT"            # network, timeout, HTTP 5xx/429: retried with exponential backoff
    NOT_PUBLISHED = "NOT_PUBLISHED"    # 404 / empty data: not an error; the item is checked again later
    PERMANENT = "PERMANENT"            # format change, invalid request, configuration: not retried (quarantine)


class WatermarkStatus(str, enum.Enum):
    """State of one detected item in ``pi_jobs.source_watermark``."""

    QUEUED = "QUEUED"
    INGESTED = "INGESTED"
    INGESTED_WITH_REJECTS = "INGESTED_WITH_REJECTS"
    NOT_PUBLISHED = "NOT_PUBLISHED"    # the runner found it not (yet) published; detected again later
    FAILED = "FAILED"                  # retries exhausted; detected again after the failure cool-down
    QUARANTINED = "QUARANTINED"        # permanent error; not re-queued for the same version until requeued


def version_token(*parts: Any) -> str:
    """A short stable token for a source version (e.g. a SHA-256 of content, or a JSON of values)."""
    text = json.dumps(parts, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class Detection:
    """One new or revised item found by a checker.

    Attributes:
        item_key: What the item is, stable across checks (e.g. ``"2026-11-24"`` or ``"2026-11-24|dgr10"``).
        version_token: The source version of the item (checksum, block count, listing file, ...).
            A different token for the same item is a revision.
        target: Runner arguments for the job (``{"runner": ..., ...}``), JSON-serialisable.
        detail: Free-form facts for the check history (dates, file names, counts).
    """

    item_key: str
    version_token: str
    target: dict[str, Any]
    detail: dict[str, Any] = field(default_factory=dict)

    @property
    def is_revision(self) -> bool:
        return bool(self.detail.get("revision"))


@dataclass
class CheckResult:
    """What one availability check found."""

    outcome: CheckOutcome
    detections: list[Detection] = field(default_factory=list)
    detail: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    error_class: ErrorClass | None = None

    @classmethod
    def from_detections(cls, detections: list[Detection], *, not_published: list[str] | None = None,
                        late: list[str] | None = None, **detail: Any) -> CheckResult:
        """NEW_DATA if anything was detected, else LATE / NOT_YET_PUBLISHED / NO_NEW_DATA."""
        info = dict(detail)
        if not_published:
            info["not_published"] = not_published
        if late:
            info["late"] = late
        if detections:
            outcome = CheckOutcome.NEW_DATA
        elif late:
            outcome = CheckOutcome.LATE
        elif not_published:
            outcome = CheckOutcome.NOT_YET_PUBLISHED
        else:
            outcome = CheckOutcome.NO_NEW_DATA
        return cls(outcome, detections, info)


@dataclass(frozen=True)
class Watermark:
    """Last known state of one item of a schedule."""

    item_key: str
    version_token: str
    status: WatermarkStatus
    updated_at: datetime
    last_job_id: int | None = None


@dataclass
class Job:
    """A row of ``pi_jobs.job`` as seen by the worker."""

    job_id: int
    schedule_key: str
    dedupe_key: str
    item_key: str
    version_token: str
    target: dict[str, Any]
    lock_group: str
    status: JobStatus
    attempts: int
    max_attempts: int
    backoff_base_seconds: float
    backoff_max_seconds: float
    created_at: datetime
    next_attempt_at: datetime
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    last_error: str | None = None


@dataclass
class ExecutionOutcome:
    """Result of executing one job (one attempt)."""

    status: JobStatus                      # SUCCEEDED, PARTIAL, FAILED or NOT_PUBLISHED (before retry policy)
    ingestion_run_ids: list[int] = field(default_factory=list)
    records_read: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_unchanged: int = 0
    records_rejected: int = 0
    data_quality_issues: int = 0
    error: str | None = None
    error_class: ErrorClass | None = None
    detail: dict[str, Any] = field(default_factory=dict)
