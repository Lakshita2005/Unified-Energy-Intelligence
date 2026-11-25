"""Test doubles for the scheduler: an in-memory job repository (same semantics as PostgresJobRepository),
a fake stored-state reader, a scripted checker, a mocked HTTP client and schedule factories."""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx

from config.settings import Settings
from src.common.http_client import HttpClient
from src.scheduler.checkers.base import AvailabilityChecker, CheckContext
from src.scheduler.config import ScheduleConfig, Window, parse_scheduler_config
from src.scheduler.models import (
    ACTIVE_STATUSES,
    CheckOutcome,
    CheckResult,
    Detection,
    ErrorClass,
    ExecutionOutcome,
    Job,
    JobStatus,
    Watermark,
    WatermarkStatus,
)
from src.scheduler.repository import dedupe_key


def settings(**overrides: Any) -> Settings:
    base = {"log_to_file": False, "scheduler_global_enable": True, "http_max_retries": 0}
    return Settings(_env_file=None, **{**base, **overrides})


def http(handler: Callable[[httpx.Request], httpx.Response], cfg: Settings | None = None) -> HttpClient:
    return HttpClient(settings=cfg or settings(), transport=httpx.MockTransport(handler), sleep=lambda _s: None)


def schedule(key: str = "TEST", *, checker: str = "fake", enabled: bool = True, every: str = "30m",
             windows: list[dict[str, str]] | None = None, params: dict[str, Any] | None = None,
             lock_group: str = "G", **extra: Any) -> ScheduleConfig:
    raw = {"key": key, "checker": checker, "enabled": enabled, "source": "TEST", "datasets": ["X"],
           "lock_group": lock_group, "every": every, "windows": windows or [], "params": params or {}, **extra}
    return parse_scheduler_config({"schedule": [raw]}).schedules[key]


# ---------------------------------------------------------------------------------- repository
@dataclass
class _JobRow:
    job: Job
    priority: int
    check_id: int | None
    detail: dict[str, Any]
    started_at: datetime | None = None
    finished_at: datetime | None = None
    runs: list[int] = field(default_factory=list)
    inserted: int = 0
    rejected: int = 0
    error_class: str | None = None


class InMemoryJobRepository:
    """Mirrors PostgresJobRepository's behaviour (claiming, leases, de-duplication, ownership)."""

    def __init__(self) -> None:
        self.rows: dict[int, _JobRow] = {}
        self.checks: list[dict[str, Any]] = []
        self.attempts: list[dict[str, Any]] = []
        self.watermarks: dict[tuple[str, str], Watermark] = {}
        self.heartbeats: dict[str, datetime] = {}
        self.db: dict[str, tuple[bool, str]] | None = None   # None = no database gate
        self._next = 1
        self._mutex = threading.Lock()

    # scheduler side
    def db_schedules(self) -> dict[str, tuple[bool, str]] | None:
        return None if self.db is None else dict(self.db)

    def sync_schedules(self, schedules: list[ScheduleConfig], now: datetime) -> dict[str, str]:
        """Like Postgres: stores config hashes, never changes enablement (new rows are disabled)."""
        self.db = {} if self.db is None else self.db
        result = {}
        for s in schedules:
            enabled, stored = self.db.get(s.key, (False, None))
            result[s.key] = ("inserted (disabled in the database)" if stored is None else
                             "unchanged" if stored == s.hash else "updated (database enablement unchanged)")
            self.db[s.key] = (enabled, s.hash)
        return result

    def set_enabled(self, schedule_key: str, enabled: bool, now: datetime) -> bool:
        if self.db is None or schedule_key not in self.db:
            return False
        self.db[schedule_key] = (enabled, self.db[schedule_key][1])
        return True

    def last_check_times(self) -> dict[str, datetime]:
        result: dict[str, datetime] = {}
        for c in self.checks:
            result[c["schedule_key"]] = max(result.get(c["schedule_key"], c["checked_at"]), c["checked_at"])
        return result

    def record_check(self, schedule_key: str, checked_at: datetime, outcome: CheckOutcome, *, duration_ms: int,
                     detections: int, jobs_created: int, detail: dict[str, Any], error: str | None,
                     error_class: ErrorClass | None) -> int:
        self.checks.append({"check_id": len(self.checks) + 1, "schedule_key": schedule_key, "checked_at": checked_at,
                            "outcome": outcome, "detections": detections, "detail": detail, "error": error,
                            "error_class": error_class, "jobs_created": jobs_created})
        return len(self.checks)

    def update_check_jobs(self, check_id: int, jobs_created: int) -> None:
        self.checks[check_id - 1]["jobs_created"] = jobs_created

    def get_watermark(self, schedule_key: str, item_key: str) -> Watermark | None:
        return self.watermarks.get((schedule_key, item_key))

    def set_watermark(self, schedule_key: str, item_key: str, version_token: str, status: WatermarkStatus,
                      job_id: int | None, now: datetime) -> None:
        with self._mutex:
            self._mark(schedule_key, item_key, version_token, status, job_id, now, same_version_only=False)

    def _mark(self, schedule_key: str, item_key: str, version_token: str, status: WatermarkStatus,
              job_id: int | None, now: datetime, *, same_version_only: bool) -> None:
        current = self.watermarks.get((schedule_key, item_key))
        if same_version_only and current is not None and current.version_token != version_token:
            return
        self.watermarks[(schedule_key, item_key)] = Watermark(
            item_key, version_token, status, now, job_id if job_id is not None else
            (current.last_job_id if current else None))

    def enqueue(self, schedule: ScheduleConfig, detection: Detection, now: datetime,
                check_id: int | None = None) -> int | None:
        """Job insert and QUEUED watermark under one lock (= one transaction in PostgreSQL)."""
        key = dedupe_key(schedule.key, detection)
        with self._mutex:
            if any(r.job.dedupe_key == key and r.job.status in ACTIVE_STATUSES for r in self.rows.values()):
                return None
            job_id = self._next
            self._next += 1
            self.rows[job_id] = _JobRow(Job(job_id, schedule.key, key, detection.item_key, detection.version_token,
                                            dict(detection.target), schedule.lock_group, JobStatus.PENDING, 0,
                                            schedule.max_attempts, schedule.backoff_base_seconds,
                                            schedule.backoff_max_seconds, now, now), schedule.priority, check_id,
                                        dict(detection.detail))
            self._mark(schedule.key, detection.item_key, detection.version_token, WatermarkStatus.QUEUED, job_id,
                       now, same_version_only=False)
        return job_id

    def recover_expired_leases(self, now: datetime) -> list[Job]:
        recovered = []
        with self._mutex:
            for row in self.rows.values():
                job = row.job
                if job.status is JobStatus.RUNNING and job.lease_expires_at is not None and job.lease_expires_at < now:
                    job.status = JobStatus.FAILED if job.attempts >= job.max_attempts else JobStatus.RETRY_WAIT
                    job.lease_owner = job.lease_expires_at = None
                    job.next_attempt_at = now
                    job.last_error = "lease expired: the worker stopped or crashed during the attempt"
                    row.error_class = "TRANSIENT"
                    if job.status is JobStatus.FAILED:
                        row.finished_at = now
                        self._mark(job.schedule_key, job.item_key, job.version_token, WatermarkStatus.FAILED,
                                   job.job_id, now, same_version_only=True)
                    recovered.append(replace(job))
        return recovered

    def beat(self, process_id: str, kind: str, now: datetime, detail: dict[str, Any]) -> None:
        self.heartbeats[process_id] = now

    # worker side
    def claim(self, worker_id: str, now: datetime, lease_seconds: int) -> Job | None:
        with self._mutex:
            running = {r.job.lock_group for r in self.rows.values() if r.job.status is JobStatus.RUNNING}
            due = [r for r in self.rows.values() if r.job.status in (JobStatus.PENDING, JobStatus.RETRY_WAIT)
                   and r.job.next_attempt_at <= now and r.job.lock_group not in running]
            if not due:
                return None
            row = min(due, key=lambda r: (r.priority, r.job.next_attempt_at, r.job.job_id))
            job = row.job
            job.status, job.attempts = JobStatus.RUNNING, job.attempts + 1
            job.lease_owner, job.lease_expires_at = worker_id, now + timedelta(seconds=lease_seconds)
            row.started_at = now
            return replace(job)

    def release(self, job: Job, worker_id: str, now: datetime, delay_seconds: float) -> None:
        with self._mutex:
            stored = self.rows[job.job_id].job
            if stored.lease_owner == worker_id and stored.status is JobStatus.RUNNING:
                stored.status, stored.attempts = JobStatus.PENDING, max(stored.attempts - 1, 0)
                stored.lease_owner = stored.lease_expires_at = None
                stored.next_attempt_at = now + timedelta(seconds=delay_seconds)

    def renew_lease(self, job_id: int, worker_id: str, now: datetime, lease_seconds: int) -> bool:
        with self._mutex:
            stored = self.rows[job_id].job
            if stored.lease_owner != worker_id or stored.status is not JobStatus.RUNNING:
                return False
            stored.lease_expires_at = now + timedelta(seconds=lease_seconds)
            return True

    def finish(self, job: Job, worker_id: str, now: datetime, status: JobStatus, outcome: ExecutionOutcome,
               next_attempt_at: datetime | None, watermark: WatermarkStatus | None = None) -> bool:
        with self._mutex:
            row = self.rows[job.job_id]
            stored = row.job
            owned = stored.lease_owner == worker_id and stored.status is JobStatus.RUNNING
            self.attempts.append({"job_id": job.job_id, "attempt_no": job.attempts, "status": status if owned else
                                  f"{status.value} (lease lost)", "error": outcome.error,
                                  "error_class": outcome.error_class, "runs": list(outcome.ingestion_run_ids)})
            if not owned:
                return False
            stored.status = status
            stored.lease_owner = stored.lease_expires_at = None
            stored.next_attempt_at = next_attempt_at or now
            stored.last_error = outcome.error
            row.error_class = outcome.error_class.value if outcome.error_class else None
            row.runs += outcome.ingestion_run_ids
            row.inserted += outcome.records_inserted
            row.rejected += outcome.records_rejected
            if status not in (JobStatus.RETRY_WAIT, JobStatus.PENDING):
                row.finished_at = now
            if watermark is not None:
                self._mark(job.schedule_key, job.item_key, job.version_token, watermark, job.job_id, now,
                           same_version_only=True)
            return True

    # admin
    def requeue(self, job_id: int, now: datetime, *, force: bool = False) -> tuple[bool, str]:
        with self._mutex:
            if job_id not in self.rows:
                return False, "no such job"
            job = self.rows[job_id].job
            if job.status not in (JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.NOT_PUBLISHED):
                return False, f"status {job.status.value} cannot be requeued"
            active = [r.job.job_id for r in self.rows.values()
                      if r.job.dedupe_key == job.dedupe_key and r.job.status in ACTIVE_STATUSES]
            if active:
                return False, f"job {active[0]} for the same version is already active"
            mark = self.watermarks.get((job.schedule_key, job.item_key))
            if mark is not None and mark.version_token != job.version_token and not force:
                return False, (f"the item now has version {mark.version_token} ({mark.status.value}), not this "
                               f"job's version {job.version_token}; use --force to re-run it anyway")
            job.status, job.attempts, job.next_attempt_at = JobStatus.PENDING, 0, now
            self.rows[job_id].finished_at = None
            if mark is None or mark.version_token == job.version_token:
                self._mark(job.schedule_key, job.item_key, job.version_token, WatermarkStatus.QUEUED, job_id, now,
                           same_version_only=False)
            return True, "requeued"

    def cancel(self, job_id: int, now: datetime) -> bool:
        with self._mutex:
            job = self.rows[job_id].job
            if job.status not in (JobStatus.PENDING, JobStatus.RETRY_WAIT):
                return False
            job.status = JobStatus.CANCELLED
            return True

    def job(self, job_id: int) -> Job:
        return self.rows[job_id].job

    def jobs(self, status: JobStatus | None = None) -> list[Job]:
        return [r.job for r in self.rows.values() if status is None or r.job.status is status]


# ---------------------------------------------------------------------------------- state / checkers
@dataclass
class FakeState:
    """In-memory SourceState."""

    checksums: dict[tuple[str, str | None], dict[date, set[str]]] = field(default_factory=dict)
    names: dict[str, set[tuple[date, str]]] = field(default_factory=dict)
    psp: dict[str, dict[date, set[str]]] = field(default_factory=dict)
    iex: dict[str, dict[date, dict[int, Decimal]]] = field(default_factory=dict)
    npp: dict[date, int] = field(default_factory=dict)
    mosdac: dict[str, datetime] = field(default_factory=dict)            # solar, per location
    mosdac_wind: dict[str, datetime] = field(default_factory=dict)       # wind, per location

    def document_checksums(self, dataset_code: str, since: date, url_marker: str | None = None
                           ) -> dict[date, set[str]]:
        data = self.checksums.get((dataset_code, url_marker), {})
        return {d: set(v) for d, v in data.items() if d >= since}

    def document_names(self, dataset_codes: list[str], since: date) -> dict[str, set[tuple[date, str]]]:
        return {c: {(d, n) for d, n in self.names.get(c, set()) if d >= since} for c in dataset_codes}

    def cea_psp_months(self, measure: str, since: date) -> dict[date, set[str]]:
        return {m: set(s) for m, s in self.psp.get(measure, {}).items() if m >= since}

    def iex_mkt_prices(self, market_code: str, start: date, end: date) -> dict[date, dict[int, Decimal]]:
        return {d: dict(b) for d, b in self.iex.get(market_code, {}).items() if start <= d <= end}

    def npp_max_updated_ms(self, series: tuple[str, ...], days: list[date]) -> dict[date, int]:
        return {d: v for d, v in self.npp.items() if d in days}

    def mosdac_latest_series(self, location_code: str, kind: str = "solar") -> datetime | None:
        return (self.mosdac if kind == "solar" else self.mosdac_wind).get(location_code)


class ScriptedChecker(AvailabilityChecker):
    """Returns queued results (or raises queued exceptions); records every context."""

    name = "fake"
    defaults: dict[str, Any] = {}

    def __init__(self, schedule: ScheduleConfig, results: list[Any] | None = None) -> None:
        super().__init__(schedule)
        self.results = list(results or [])
        self.contexts: list[CheckContext] = []

    def check(self, ctx: CheckContext) -> CheckResult:
        self.contexts.append(ctx)
        result = self.results.pop(0) if self.results else CheckResult(CheckOutcome.NO_NEW_DATA)
        if isinstance(result, BaseException):
            raise result
        return result


def detection(item: str = "2026-11-24", version: str = "v1", **target: Any) -> Detection:
    return Detection(item, version, {"runner": "fake", **target}, {"revision": False})


def new_data(*detections: Detection) -> CheckResult:
    return CheckResult.from_detections(list(detections))


__all__ = ["FakeState", "InMemoryJobRepository", "ScriptedChecker", "Window", "detection", "http", "new_data",
           "schedule", "settings"]
