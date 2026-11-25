"""Job queue and scheduler state in PostgreSQL (schema ``pi_jobs``, migration ``0011_pi_jobs``).

:class:`JobRepository` is the interface the scheduler, worker and admin commands use;
:class:`PostgresJobRepository` implements it with plain SQL:

* **Claiming** - ``UPDATE ... WHERE job_id = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)``: two workers
  never claim the same job, and a job whose ``lock_group`` already has a RUNNING job is not claimed
  (the worker additionally holds an advisory lock per group, see :mod:`src.scheduler.locks`).
* **Leases** - a claimed job carries ``lease_owner`` / ``lease_expires_at``; the worker renews it while
  running. Expired leases (a crashed or killed worker) are recovered by the scheduler: back to
  ``RETRY_WAIT``, or ``FAILED`` when no attempts remain.
* **De-duplication** - ``dedupe_key = schedule|item|version``; a partial unique index allows only one
  active (PENDING/RUNNING/RETRY_WAIT) job per key, so repeated detections never create duplicate jobs.
* **Ownership** - a worker can only finish a job it still owns; a job recovered meanwhile is left alone.

All timestamps are timezone-aware and passed in (the clock is injectable).
"""

from __future__ import annotations

import json
import socket
from collections.abc import Iterator
from contextlib import AbstractContextManager
from datetime import datetime, timedelta
from typing import Any, Protocol

from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.scheduler.config import ScheduleConfig, config_hash
from src.scheduler.models import (
    CheckOutcome,
    Detection,
    ErrorClass,
    ExecutionOutcome,
    Job,
    JobStatus,
    Watermark,
    WatermarkStatus,
)


class ConnectionProvider(Protocol):
    def connect(self) -> AbstractContextManager[Connection]:
        """A connection inside a transaction that commits on success."""


def dedupe_key(schedule_key: str, detection: Detection) -> str:
    return f"{schedule_key}|{detection.item_key}|{detection.version_token}"


class JobRepository(Protocol):
    """Persistence used by the scheduler, worker and admin commands."""

    # configuration / scheduler state
    def sync_schedules(self, schedules: list[ScheduleConfig], now: datetime) -> dict[str, str]: ...
    def db_schedules(self) -> dict[str, tuple[bool, str]] | None:
        """``{schedule_key: (enabled, config hash)}`` as stored; None = no database gate (tests)."""
    def last_check_times(self) -> dict[str, datetime]: ...
    def record_check(self, schedule_key: str, checked_at: datetime, outcome: CheckOutcome, *, duration_ms: int,
                     detections: int, jobs_created: int, detail: dict[str, Any], error: str | None,
                     error_class: ErrorClass | None) -> int: ...
    def get_watermark(self, schedule_key: str, item_key: str) -> Watermark | None: ...
    def set_watermark(self, schedule_key: str, item_key: str, version_token: str, status: WatermarkStatus,
                      job_id: int | None, now: datetime) -> None: ...
    def enqueue(self, schedule: ScheduleConfig, detection: Detection, now: datetime,
                check_id: int | None = None) -> int | None:
        """Insert a PENDING job AND mark the item QUEUED, atomically; None if an active duplicate exists."""
    def recover_expired_leases(self, now: datetime) -> list[Job]: ...
    def beat(self, process_id: str, kind: str, now: datetime, detail: dict[str, Any]) -> None: ...

    # worker
    def claim(self, worker_id: str, now: datetime, lease_seconds: int) -> Job | None: ...
    def release(self, job: Job, worker_id: str, now: datetime, delay_seconds: float) -> None: ...
    def renew_lease(self, job_id: int, worker_id: str, now: datetime, lease_seconds: int) -> bool: ...
    def finish(self, job: Job, worker_id: str, now: datetime, status: JobStatus, outcome: ExecutionOutcome,
               next_attempt_at: datetime | None, watermark: WatermarkStatus | None = None) -> bool:
        """Record the attempt, the job state and (same version only) the watermark, atomically."""

    # admin
    def list_jobs(self, *, status: str | None = None, schedule_key: str | None = None,
                  limit: int = 50) -> list[dict[str, Any]]: ...
    def requeue(self, job_id: int, now: datetime, *, force: bool = False) -> tuple[bool, str]: ...
    def cancel(self, job_id: int, now: datetime) -> bool: ...
    def set_enabled(self, schedule_key: str, enabled: bool, now: datetime) -> bool: ...
    def status_summary(self) -> dict[str, Any]: ...


def process_identity(kind: str) -> str:
    import os

    return f"{kind}@{socket.gethostname()}:{os.getpid()}"


_JOB_COLUMNS = ("job_id, schedule_key, dedupe_key, item_key, version_token, target, lock_group, status, attempts, "
                "max_attempts, backoff_base_seconds, backoff_max_seconds, created_at, next_attempt_at, lease_owner, "
                "lease_expires_at, last_error")


def _job(row: Any) -> Job:
    m = row._mapping
    target = m["target"] if isinstance(m["target"], dict) else json.loads(m["target"])
    return Job(m["job_id"], m["schedule_key"], m["dedupe_key"], m["item_key"], m["version_token"], target,
               m["lock_group"], JobStatus(m["status"]), m["attempts"], m["max_attempts"],
               float(m["backoff_base_seconds"]), float(m["backoff_max_seconds"]), m["created_at"],
               m["next_attempt_at"], m["lease_owner"], m["lease_expires_at"], m["last_error"])


class PostgresJobRepository:
    """:class:`JobRepository` on PostgreSQL. ``database`` is a DatabaseManager (or SavepointDatabase in tests)."""

    def __init__(self, database: ConnectionProvider, schema: str = "pi_jobs") -> None:
        self.database = database
        self.s = schema

    def _conn(self) -> AbstractContextManager[Connection]:
        return self.database.connect()

    # ------------------------------------------------------------------ setup / checks
    def schema_exists(self) -> bool:
        with self._conn() as c:
            return c.execute(text("SELECT to_regclass(:t) IS NOT NULL"), {"t": f"{self.s}.job"}).scalar_one()

    def sync_schedules(self, schedules: list[ScheduleConfig], now: datetime) -> dict[str, str]:
        """Copy the TOML configuration into ``pi_jobs.schedule``. NEVER writes the ``enabled`` column.

        New rows get ``enabled = false`` (column default). Turning a schedule on in the database is a separate,
        admin-only operation (:meth:`set_enabled`; the application role has no UPDATE privilege on that
        column). Schedules no longer in the file are left as they are: the scheduler only runs schedules that
        are in the file, so they cannot run.
        """
        result: dict[str, str] = {}
        with self._conn() as c:
            existing = {r[0]: r[1] for r in c.execute(text(f"SELECT schedule_key, config FROM {self.s}.schedule"))}
            for schedule in schedules:
                row = schedule.as_row()
                if schedule.key not in existing:
                    result[schedule.key] = "inserted (disabled in the database)"
                elif config_hash(existing[schedule.key]) != config_hash(row):
                    result[schedule.key] = "updated (database enablement unchanged)"
                else:
                    result[schedule.key] = "unchanged"
                    continue
                c.execute(text(
                    f"INSERT INTO {self.s}.schedule (schedule_key, checker, source_code, dataset_codes, lock_group, "
                    "config, synced_at) VALUES (:k, :checker, :source, :datasets, :lock, CAST(:config AS jsonb), :now) "
                    "ON CONFLICT (schedule_key) DO UPDATE SET checker = EXCLUDED.checker, "
                    "source_code = EXCLUDED.source_code, dataset_codes = EXCLUDED.dataset_codes, "
                    "lock_group = EXCLUDED.lock_group, config = EXCLUDED.config, synced_at = EXCLUDED.synced_at"),
                    {"k": schedule.key, "checker": schedule.checker, "source": schedule.source,
                     "datasets": list(schedule.datasets), "lock": schedule.lock_group,
                     "config": json.dumps(row, sort_keys=True, default=str), "now": now})
            for key in sorted(set(existing) - {s.key for s in schedules}):
                result[key] = "not in the configuration file (ignored by the scheduler; left unchanged)"
        return result

    def db_schedules(self) -> dict[str, tuple[bool, str]]:
        with self._conn() as c:
            return {r[0]: (bool(r[1]), config_hash(r[2])) for r in c.execute(text(
                f"SELECT schedule_key, enabled, config FROM {self.s}.schedule"))}

    def set_enabled(self, schedule_key: str, enabled: bool, now: datetime) -> bool:
        """Admin only: the application role is not granted UPDATE on ``enabled`` (permission error)."""
        with self._conn() as c:
            return c.execute(text(f"UPDATE {self.s}.schedule SET enabled = :e WHERE schedule_key = :k "
                                  "RETURNING schedule_key"), {"e": enabled, "k": schedule_key}).first() is not None

    def last_check_times(self) -> dict[str, datetime]:
        with self._conn() as c:
            return {r[0]: r[1] for r in c.execute(text(
                f"SELECT schedule_key, max(checked_at) FROM {self.s}.availability_check GROUP BY 1"))}

    def record_check(self, schedule_key: str, checked_at: datetime, outcome: CheckOutcome, *, duration_ms: int,
                     detections: int, jobs_created: int, detail: dict[str, Any], error: str | None,
                     error_class: ErrorClass | None) -> int:
        with self._conn() as c:
            return c.execute(text(
                f"INSERT INTO {self.s}.availability_check (schedule_key, checked_at, outcome, duration_ms, detections, "
                "jobs_created, detail, error, error_class) VALUES (:k, :t, :o, :d, :n, :j, CAST(:detail AS jsonb), "
                ":e, :ec) RETURNING check_id"),
                {"k": schedule_key, "t": checked_at, "o": outcome.value, "d": duration_ms, "n": detections,
                 "j": jobs_created, "detail": json.dumps(detail, default=str), "e": error,
                 "ec": error_class.value if error_class else None}).scalar_one()

    def update_check_jobs(self, check_id: int, jobs_created: int) -> None:
        with self._conn() as c:
            c.execute(text(f"UPDATE {self.s}.availability_check SET jobs_created = :j WHERE check_id = :c"),
                      {"j": jobs_created, "c": check_id})

    # ------------------------------------------------------------------ watermarks
    def get_watermark(self, schedule_key: str, item_key: str) -> Watermark | None:
        with self._conn() as c:
            row = c.execute(text(
                f"SELECT item_key, version_token, status, updated_at, last_job_id FROM {self.s}.source_watermark "
                "WHERE schedule_key = :k AND item_key = :i"), {"k": schedule_key, "i": item_key}).first()
        return None if row is None else Watermark(row[0], row[1], WatermarkStatus(row[2]), row[3], row[4])

    def set_watermark(self, schedule_key: str, item_key: str, version_token: str, status: WatermarkStatus,
                      job_id: int | None, now: datetime) -> None:
        with self._conn() as c:
            self._upsert_watermark(c, schedule_key, item_key, version_token, status, job_id, now,
                                   same_version_only=False)

    # ------------------------------------------------------------------ jobs
    def enqueue(self, schedule: ScheduleConfig, detection: Detection, now: datetime,
                check_id: int | None = None) -> int | None:
        """Insert a PENDING job and mark the item QUEUED in ONE transaction.

        None (and no watermark change) if an active job with the same dedupe key exists. Because both writes
        commit together, a worker can never finish the job before its QUEUED mark exists.
        """
        with self._conn() as c:
            job_id = c.execute(text(
                f"INSERT INTO {self.s}.job (schedule_key, dedupe_key, item_key, version_token, target, lock_group, "
                "priority, status, attempts, max_attempts, backoff_base_seconds, backoff_max_seconds, "
                "next_attempt_at, created_at, updated_at, check_id, detail) VALUES (:k, :dk, :i, :v, "
                "CAST(:target AS jsonb), :lock, :prio, 'PENDING', 0, :max, :bb, :bm, :now, :now, :now, :check, "
                "CAST(:detail AS jsonb)) ON CONFLICT (dedupe_key) WHERE status IN ('PENDING', 'RUNNING', "
                "'RETRY_WAIT') DO NOTHING RETURNING job_id"),
                {"k": schedule.key, "dk": dedupe_key(schedule.key, detection), "i": detection.item_key,
                 "v": detection.version_token, "target": json.dumps(detection.target, default=str),
                 "lock": schedule.lock_group, "prio": schedule.priority, "max": schedule.max_attempts,
                 "bb": schedule.backoff_base_seconds, "bm": schedule.backoff_max_seconds, "now": now,
                 "check": check_id, "detail": json.dumps(detection.detail, default=str)}).scalar_one_or_none()
            if job_id is not None:
                self._upsert_watermark(c, schedule.key, detection.item_key, detection.version_token,
                                       WatermarkStatus.QUEUED, job_id, now, same_version_only=False)
            return job_id

    def _upsert_watermark(self, c: Connection, schedule_key: str, item_key: str, version_token: str,
                          status: WatermarkStatus, job_id: int | None, now: datetime, *,
                          same_version_only: bool) -> None:
        """Insert/update a watermark. ``same_version_only``: never overwrite a NEWER version's state
        (a late finish of an older version must not mark the item as ingested)."""
        guard = (f" WHERE {self.s}.source_watermark.version_token = EXCLUDED.version_token"
                 if same_version_only else "")
        c.execute(text(
            f"INSERT INTO {self.s}.source_watermark (schedule_key, item_key, version_token, status, last_job_id, "
            "first_seen_at, updated_at) VALUES (:k, :i, :v, :s, :j, :now, :now) "
            "ON CONFLICT (schedule_key, item_key) DO UPDATE SET version_token = EXCLUDED.version_token, "
            "status = EXCLUDED.status, last_job_id = COALESCE(EXCLUDED.last_job_id, "
            f"{self.s}.source_watermark.last_job_id), updated_at = EXCLUDED.updated_at{guard}"),
            {"k": schedule_key, "i": item_key, "v": version_token, "s": status.value, "j": job_id, "now": now})

    def claim(self, worker_id: str, now: datetime, lease_seconds: int) -> Job | None:
        with self._conn() as c:
            row = c.execute(text(
                f"WITH candidate AS (SELECT j.job_id FROM {self.s}.job j "
                "WHERE j.status IN ('PENDING', 'RETRY_WAIT') AND j.next_attempt_at <= :now "
                f"AND NOT EXISTS (SELECT 1 FROM {self.s}.job r WHERE r.status = 'RUNNING' "
                "AND r.lock_group = j.lock_group) "
                "ORDER BY j.priority, j.next_attempt_at, j.job_id FOR UPDATE SKIP LOCKED LIMIT 1) "
                f"UPDATE {self.s}.job j SET status = 'RUNNING', attempts = j.attempts + 1, lease_owner = :w, "
                "lease_expires_at = :exp, started_at = :now, updated_at = :now FROM candidate "
                f"WHERE j.job_id = candidate.job_id RETURNING {', '.join('j.' + c for c in _JOB_COLUMNS.split(', '))}"),
                {"now": now, "w": worker_id, "exp": now + timedelta(seconds=lease_seconds)}).first()
        return None if row is None else _job(row)

    def release(self, job: Job, worker_id: str, now: datetime, delay_seconds: float) -> None:
        """Give a claimed job back without counting the attempt (e.g. its source lock was busy)."""
        with self._conn() as c:
            c.execute(text(
                f"UPDATE {self.s}.job SET status = 'PENDING', attempts = greatest(attempts - 1, 0), lease_owner = NULL, "
                "lease_expires_at = NULL, next_attempt_at = :next, updated_at = :now "
                "WHERE job_id = :id AND lease_owner = :w AND status = 'RUNNING'"),
                {"id": job.job_id, "w": worker_id, "now": now, "next": now + timedelta(seconds=delay_seconds)})

    def renew_lease(self, job_id: int, worker_id: str, now: datetime, lease_seconds: int) -> bool:
        with self._conn() as c:
            return c.execute(text(
                f"UPDATE {self.s}.job SET lease_expires_at = :exp, updated_at = :now "
                "WHERE job_id = :id AND lease_owner = :w AND status = 'RUNNING' RETURNING job_id"),
                {"id": job_id, "w": worker_id, "now": now,
                 "exp": now + timedelta(seconds=lease_seconds)}).first() is not None

    def finish(self, job: Job, worker_id: str, now: datetime, status: JobStatus, outcome: ExecutionOutcome,
               next_attempt_at: datetime | None, watermark: WatermarkStatus | None = None) -> bool:
        """Record the attempt, the job's new state and (if owned, same version only) the item's watermark,
        in one transaction. False if the worker no longer owns the job (its state is then left alone)."""
        final = status not in (JobStatus.RETRY_WAIT, JobStatus.PENDING)
        params = {"id": job.job_id, "w": worker_id, "now": now, "status": status.value,
                  "next": next_attempt_at or now, "runs": outcome.ingestion_run_ids,
                  "read": outcome.records_read, "ins": outcome.records_inserted, "upd": outcome.records_updated,
                  "unch": outcome.records_unchanged, "rej": outcome.records_rejected,
                  "dq": outcome.data_quality_issues, "err": outcome.error,
                  "ec": outcome.error_class.value if outcome.error_class else None,
                  "result": json.dumps(outcome.detail, default=str), "final": final}
        with self._conn() as c:
            owned = c.execute(text(
                f"UPDATE {self.s}.job SET status = :status, lease_owner = NULL, lease_expires_at = NULL, "
                "next_attempt_at = :next, updated_at = :now, "
                "finished_at = CASE WHEN :final THEN CAST(:now AS timestamptz) ELSE NULL END, "
                "ingestion_run_ids = ingestion_run_ids || CAST(:runs AS bigint[]), records_read = records_read + :read, "
                "records_inserted = records_inserted + :ins, records_updated = records_updated + :upd, "
                "records_unchanged = records_unchanged + :unch, records_rejected = records_rejected + :rej, "
                "data_quality_issues = data_quality_issues + :dq, last_error = :err, error_class = :ec, "
                "result = CAST(:result AS jsonb) WHERE job_id = :id AND lease_owner = :w AND status = 'RUNNING' "
                "RETURNING job_id"), params).first() is not None
            c.execute(text(
                f"INSERT INTO {self.s}.job_attempt (job_id, attempt_no, worker_id, started_at, finished_at, status, "
                "error_class, error, ingestion_run_ids, records_read, records_inserted, records_updated, "
                "records_unchanged, records_rejected, data_quality_issues, detail) VALUES (:id, :attempt, :w, "
                f"(SELECT started_at FROM {self.s}.job WHERE job_id = :id), :now, :astatus, :ec, :err, "
                "CAST(:runs AS bigint[]), :read, :ins, :upd, :unch, :rej, :dq, CAST(:result AS jsonb))"),
                {**params, "attempt": job.attempts,
                 "astatus": status.value if owned else f"{status.value} (lease lost)"})
            if owned and watermark is not None:
                self._upsert_watermark(c, job.schedule_key, job.item_key, job.version_token, watermark, job.job_id,
                                       now, same_version_only=True)
        return owned

    def recover_expired_leases(self, now: datetime) -> list[Job]:
        """RUNNING jobs whose lease expired -> RETRY_WAIT (or FAILED, with a FAILED watermark for the same
        version, when no attempts remain). One transaction."""
        with self._conn() as c:
            rows = c.execute(text(
                f"UPDATE {self.s}.job SET status = CASE WHEN attempts >= max_attempts THEN 'FAILED' "
                "ELSE 'RETRY_WAIT' END, finished_at = CASE WHEN attempts >= max_attempts THEN "
                "CAST(:now AS timestamptz) END, lease_owner = NULL, lease_expires_at = NULL, next_attempt_at = :now, "
                "updated_at = :now, last_error = 'lease expired: the worker stopped or crashed during the attempt', "
                "error_class = 'TRANSIENT' WHERE status = 'RUNNING' AND lease_expires_at < :now "
                f"RETURNING {_JOB_COLUMNS}"), {"now": now}).all()
            jobs = [_job(r) for r in rows]
            for job in jobs:
                if job.status is JobStatus.FAILED:
                    self._upsert_watermark(c, job.schedule_key, job.item_key, job.version_token,
                                           WatermarkStatus.FAILED, job.job_id, now, same_version_only=True)
        return jobs

    def beat(self, process_id: str, kind: str, now: datetime, detail: dict[str, Any]) -> None:
        import os

        with self._conn() as c:
            c.execute(text(
                f"INSERT INTO {self.s}.process_heartbeat (process_id, process_kind, host, pid, started_at, last_seen_at, "
                "detail) VALUES (:p, :k, :h, :pid, :now, :now, CAST(:d AS jsonb)) ON CONFLICT (process_id) DO UPDATE "
                "SET last_seen_at = EXCLUDED.last_seen_at, detail = EXCLUDED.detail"),
                {"p": process_id, "k": kind, "h": socket.gethostname(), "pid": os.getpid(), "now": now,
                 "d": json.dumps(detail, default=str)})

    # ------------------------------------------------------------------ admin
    def list_jobs(self, *, status: str | None = None, schedule_key: str | None = None,
                  limit: int = 50) -> list[dict[str, Any]]:
        with self._conn() as c:
            rows = c.execute(text(
                f"SELECT job_id, schedule_key, item_key, status, attempts, max_attempts, created_at, started_at, "
                "finished_at, next_attempt_at, records_inserted, records_rejected, ingestion_run_ids, last_error "
                f"FROM {self.s}.job WHERE (CAST(:st AS text) IS NULL OR status = :st) "
                "AND (CAST(:k AS text) IS NULL OR schedule_key = :k) ORDER BY job_id DESC LIMIT :n"),
                {"st": status, "k": schedule_key, "n": limit}).mappings().all()
        return [dict(r) for r in rows]

    def requeue(self, job_id: int, now: datetime, *, force: bool = False) -> tuple[bool, str]:
        """Put a FAILED / CANCELLED / NOT_PUBLISHED job back to PENDING with fresh attempts (version-aware).

        Refused when the item's watermark now refers to a DIFFERENT version (a newer detection was queued or
        ingested meanwhile) unless ``force``; the watermark is set back to QUEUED only for the job's own
        version, so a requeue can never relabel a newer version. Runners always fetch the source's current
        data, so a forced requeue re-ingests the latest data, never the old version.
        """
        with self._conn() as c:
            job = c.execute(text(
                f"SELECT job_id, schedule_key, item_key, version_token, status, dedupe_key FROM {self.s}.job "
                "WHERE job_id = :id FOR UPDATE"), {"id": job_id}).mappings().first()
            if job is None:
                return False, "no such job"
            if job["status"] not in ("FAILED", "CANCELLED", "NOT_PUBLISHED"):
                return False, f"status {job['status']} cannot be requeued"
            active = c.execute(text(
                f"SELECT job_id FROM {self.s}.job WHERE dedupe_key = :d AND status IN ('PENDING', 'RUNNING', "
                "'RETRY_WAIT')"), {"d": job["dedupe_key"]}).scalar()
            if active is not None:
                return False, f"job {active} for the same version is already active"
            mark = c.execute(text(
                f"SELECT version_token, status FROM {self.s}.source_watermark WHERE schedule_key = :k "
                "AND item_key = :i FOR UPDATE"), {"k": job["schedule_key"], "i": job["item_key"]}).first()
            if mark is not None and mark[0] != job["version_token"] and not force:
                return False, (f"the item now has version {mark[0]} ({mark[1]}), not this job's version "
                               f"{job['version_token']}; use --force to re-run it anyway")
            c.execute(text(f"UPDATE {self.s}.job SET status = 'PENDING', attempts = 0, next_attempt_at = :now, "
                           "finished_at = NULL, updated_at = :now WHERE job_id = :id"), {"id": job_id, "now": now})
            if mark is None or mark[0] == job["version_token"]:
                self._upsert_watermark(c, job["schedule_key"], job["item_key"], job["version_token"],
                                       WatermarkStatus.QUEUED, job_id, now, same_version_only=False)
        return True, "requeued"

    def cancel(self, job_id: int, now: datetime) -> bool:
        with self._conn() as c:
            return c.execute(text(
                f"UPDATE {self.s}.job SET status = 'CANCELLED', finished_at = :now, updated_at = :now "
                "WHERE job_id = :id AND status IN ('PENDING', 'RETRY_WAIT') RETURNING job_id"),
                {"id": job_id, "now": now}).first() is not None

    def status_summary(self) -> dict[str, Any]:
        with self._conn() as c:
            jobs = {r[0]: r[1] for r in c.execute(text(f"SELECT status, count(*) FROM {self.s}.job GROUP BY 1"))}
            schedules = [dict(r) for r in c.execute(text(
                f"SELECT s.schedule_key, s.enabled, s.lock_group, c.checked_at AS last_check, c.outcome AS last_outcome, "
                f"(SELECT count(*) FROM {self.s}.job j WHERE j.schedule_key = s.schedule_key "
                "AND j.status IN ('PENDING', 'RUNNING', 'RETRY_WAIT')) AS active_jobs, "
                f"(SELECT max(finished_at) FROM {self.s}.job j WHERE j.schedule_key = s.schedule_key "
                f"AND j.status IN ('SUCCEEDED', 'PARTIAL')) AS last_success FROM {self.s}.schedule s "
                f"LEFT JOIN LATERAL (SELECT checked_at, outcome FROM {self.s}.availability_check a "
                "WHERE a.schedule_key = s.schedule_key ORDER BY checked_at DESC LIMIT 1) c ON true "
                "ORDER BY s.schedule_key")).mappings()]
            processes = [dict(r) for r in c.execute(text(
                f"SELECT process_id, process_kind, host, pid, started_at, last_seen_at FROM {self.s}.process_heartbeat "
                "ORDER BY last_seen_at DESC")).mappings()]
        return {"jobs": jobs, "schedules": schedules, "processes": processes}


def iter_chunks(items: list[Any], size: int) -> Iterator[list[Any]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]
