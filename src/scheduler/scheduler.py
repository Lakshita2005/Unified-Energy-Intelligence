"""pi-scheduler: availability checks -> jobs.

One scheduler runs at a time (advisory lock). Each tick (``SCHEDULER_TICK_SECONDS``):

1. Every enabled schedule whose interval has elapsed (per its local windows) is checked by its checker.
2. The check is recorded in ``pi_jobs.availability_check`` (outcome, detected items, duration, error).
3. Each detection passes the watermark guards and becomes a job (``pi_jobs.job``, de-duplicated):

   * same version already ingested -> skipped;
   * same version quarantined (permanent failure) -> skipped until someone requeues the job;
   * same version failed after all retries -> skipped until ``failure_cooldown`` has passed;
   * an active job with the same ``schedule|item|version`` -> no second job (unique index).

On start, and every ``SCHEDULER_RECOVERY_INTERVAL_SECONDS``, jobs whose worker lease expired are put
back to RETRY_WAIT (or FAILED when no attempts remain). On start, every enabled schedule is checked once
immediately (catch-up after downtime): checkers compare the source with the database over the catch-up
window, so anything published while the system was down is found then.

A schedule runs only if ALL of these hold (checked on every tick):

* ``SCHEDULER_GLOBAL_ENABLE`` is true;
* it is ``enabled = true`` in the configuration file;
* it is enabled in ``pi_jobs.schedule`` (an admin-only switch: ``pi_jobs enable``; ``pi_app`` cannot set it);
* the configuration hash stored by ``sync-config`` equals the hash of the file's current configuration;
* Open-Meteo only: plan and key agree (free = no key, non-commercial use; commercial = key) and, on the free
  plan, the worst-case daily call count of the enabled Open-Meteo schedules is within the daily limit.
  An unsynced edit of ``config/scheduler.toml`` therefore stops the schedule (with a warning) instead of
  silently running a configuration the database does not know about.

The scheduler re-verifies on every tick that its PostgreSQL session still holds the scheduler advisory
lock; if the session was lost (server restart, network drop, terminated backend) another scheduler may
already have taken over, so it stops with :class:`SchedulerLockLost` (exit code 1, the service restarts it).
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from config.settings import Settings
from src.common.http_client import HttpClient
from src.common.logging_config import get_logger
from src.scheduler.checkers.base import AvailabilityChecker, CheckContext
from src.scheduler.checkers.open_meteo import SOURCE as OPEN_METEO_SOURCE
from src.scheduler.checkers.open_meteo import open_meteo_refusal
from src.scheduler.clock import Clock
from src.scheduler.config import ScheduleConfig, SchedulerConfig
from src.scheduler.locks import SCHEDULER_LOCK, LockFactory
from src.scheduler.models import CheckOutcome, CheckResult, Detection, ErrorClass, WatermarkStatus
from src.scheduler.process import StopSignal
from src.scheduler.repository import JobRepository
from src.scheduler.retry import classify_exception

logger = get_logger(__name__)


def gate_refusal(schedule: ScheduleConfig, db: dict[str, tuple[bool, str]] | None, settings: Settings,
                 enabled: Iterable[ScheduleConfig] = ()) -> str | None:
    """Why a schedule that is enabled in the file must not run, or None.

    ``db`` is ``repository.db_schedules()`` (``{key: (enabled, config hash)}``); None means no database gate
    (in-memory repositories in unit tests). ``enabled`` = every schedule enabled in the file (Open-Meteo's
    daily call budget is shared). ``SCHEDULER_GLOBAL_ENABLE`` is checked by the caller.
    """
    if schedule.source == OPEN_METEO_SOURCE:
        reason = open_meteo_refusal(schedule, enabled, settings)
        if reason is not None:
            return reason
    if db is None:
        return None
    stored = db.get(schedule.key)
    if stored is None:
        return "not in pi_jobs.schedule (run: pi_jobs sync-config)"
    enabled, stored_hash = stored
    if stored_hash != schedule.hash:
        return ("configuration differs from the synced copy in pi_jobs.schedule (hash "
                f"{stored_hash[:12]} in the database vs {schedule.hash[:12]} in the file); refusing to run "
                "it until 'pi_jobs sync-config' is run")
    if not enabled:
        return "not enabled in pi_jobs.schedule (admin-only: pi_jobs enable)"
    return None


class SchedulerAlreadyRunning(RuntimeError):
    """Another pi-scheduler holds the scheduler lock."""


class SchedulerLockLost(RuntimeError):
    """This scheduler's advisory-lock session is gone; another scheduler may now hold the lock."""


@dataclass
class CheckReport:
    schedule_key: str
    outcome: CheckOutcome
    check_id: int | None
    jobs_created: list[int] = field(default_factory=list)
    skipped: dict[str, str] = field(default_factory=dict)      # item_key -> reason
    error: str | None = None


class Scheduler:
    def __init__(self, *, config: SchedulerConfig, checkers: dict[str, AvailabilityChecker], repository: JobRepository,
                 clock: Clock, settings: Settings, http_factory: Callable[[], HttpClient], state: Any,
                 locks: LockFactory | None = None, process_id: str = "pi-scheduler") -> None:
        self.config = config
        self.checkers = checkers
        self.repository = repository
        self.clock = clock
        self.settings = settings
        self.http_factory = http_factory
        self.state = state
        self.locks = locks
        self.process_id = process_id
        self._last_check: dict[str, datetime] = {}
        self._last_recovery: datetime | None = None
        self._gate_messages: dict[str, str] = {}    # schedule -> last refusal logged (log changes only)

    # ------------------------------------------------------------------ which schedules run
    def refusal(self, schedule: ScheduleConfig, db: dict[str, tuple[bool, str]] | None) -> str | None:
        return gate_refusal(schedule, db, self.settings, self.config.enabled())

    def active_schedules(self) -> list[ScheduleConfig]:
        if not self.settings.scheduler_global_enable:
            return []
        db = self.repository.db_schedules()
        active = []
        for schedule in self.config.enabled():
            reason = self.refusal(schedule, db)
            if reason is not None:
                if self._gate_messages.get(schedule.key) != reason:
                    logger.warning("%s is enabled in %s but will not run: %s", schedule.key, self.config.path,
                                   reason)
                    self._gate_messages[schedule.key] = reason
                continue
            if self._gate_messages.pop(schedule.key, None) is not None:
                logger.info("%s: database gate now open; schedule active", schedule.key)
            active.append(schedule)
        return active

    # ------------------------------------------------------------------ one tick
    def tick(self, *, force: bool = False) -> list[CheckReport]:
        now = self.clock.now()
        if self._last_recovery is None or (now - self._last_recovery) >= timedelta(
                seconds=self.settings.scheduler_recovery_interval_seconds):
            self.recover(now)
        if not self._last_check:
            self._last_check = dict(self.repository.last_check_times())
        reports = []
        for schedule in sorted(self.active_schedules(), key=lambda s: (s.priority, s.key)):
            now = self.clock.now()
            if not force and not schedule.is_due(self._last_check.get(schedule.key), now):
                continue
            reports.append(self.run_check(schedule, now))
        return reports

    def run_check(self, schedule: ScheduleConfig, now: datetime) -> CheckReport:
        checker = self.checkers[schedule.key]
        previous = self._last_check.get(schedule.key)
        started = time.perf_counter()
        http = self.http_factory()
        try:
            ctx = CheckContext(schedule, now, previous, http, self.settings, self.state,
                               lambda item: self.repository.get_watermark(schedule.key, item))
            result = checker.check(ctx)
        except Exception as exc:   # recorded, never fatal for the scheduler
            error_class = classify_exception(exc)
            result = CheckResult(CheckOutcome.SOURCE_ERROR, error=f"{type(exc).__name__}: {exc}"[:4000],
                                 error_class=error_class)
            logger.warning("%s: check failed (%s): %s", schedule.key, error_class.value, result.error)
        finally:
            http.close()
        duration_ms = int((time.perf_counter() - started) * 1000)
        self._last_check[schedule.key] = now
        detail = dict(result.detail)
        detail["detections"] = [{"item": d.item_key, "version": d.version_token, **d.detail}
                                for d in result.detections]
        check_id = self.repository.record_check(
            schedule.key, now, result.outcome, duration_ms=duration_ms, detections=len(result.detections),
            jobs_created=0, detail=detail, error=result.error, error_class=result.error_class)
        report = CheckReport(schedule.key, result.outcome, check_id, error=result.error)
        for detection in result.detections:
            reason = self._skip_reason(schedule, detection, now)
            if reason:
                report.skipped[detection.item_key] = reason
                continue
            job_id = self.repository.enqueue(schedule, detection, now, check_id)   # job + QUEUED mark, atomic
            if job_id is None:
                report.skipped[detection.item_key] = "an active job for this version exists"
                continue
            report.jobs_created.append(job_id)
        if report.jobs_created and hasattr(self.repository, "update_check_jobs"):
            self.repository.update_check_jobs(check_id, len(report.jobs_created))
        logger.info("%s: %s, %d detection(s), %d job(s) created%s (%d ms)", schedule.key, result.outcome.value,
                    len(result.detections), len(report.jobs_created),
                    f", skipped {report.skipped}" if report.skipped else "", duration_ms)
        return report

    def _skip_reason(self, schedule: ScheduleConfig, detection: Detection, now: datetime) -> str | None:
        mark = self.repository.get_watermark(schedule.key, detection.item_key)
        if mark is None or mark.version_token != detection.version_token:
            return None
        if mark.status in (WatermarkStatus.INGESTED, WatermarkStatus.INGESTED_WITH_REJECTS):
            return "this version was already ingested"
        if mark.status is WatermarkStatus.QUARANTINED:
            return "quarantined after a permanent failure (requeue the job after fixing the cause)"
        if mark.status is WatermarkStatus.FAILED and now - mark.updated_at < timedelta(
                seconds=schedule.failure_cooldown_seconds):
            return "failed after all retries; waiting for the failure cool-down"
        return None

    # ------------------------------------------------------------------ recovery
    def recover(self, now: datetime) -> list[int]:
        """Expired leases -> RETRY_WAIT / FAILED (the repository marks FAILED watermarks in the same transaction)."""
        recovered = self.repository.recover_expired_leases(now)
        for job in recovered:
            logger.warning("Recovered job %s (%s %s): worker lease expired -> %s", job.job_id, job.schedule_key,
                           job.item_key, job.status.value)
        self._last_recovery = now
        return [job.job_id for job in recovered]

    # ------------------------------------------------------------------ main loop
    def run(self, stop: StopSignal) -> None:
        lock = self.locks.try_acquire(SCHEDULER_LOCK) if self.locks is not None else None
        if self.locks is not None and lock is None:
            raise SchedulerAlreadyRunning("another pi-scheduler is running (scheduler advisory lock is held)")
        try:
            active = self.active_schedules()
            logger.info("pi-scheduler started: %d of %d schedule(s) active%s", len(active),
                        len(self.config.schedules),
                        "" if self.settings.scheduler_global_enable else " (SCHEDULER_GLOBAL_ENABLE=false)")
            self.recover(self.clock.now())
            self.tick(force=True)              # catch-up check after (re)start
            while not stop.is_set():
                try:
                    self.repository.beat(self.process_id, "scheduler", self.clock.now(),
                                         {"active_schedules": [s.key for s in self.active_schedules()]})
                except Exception:
                    logger.exception("Scheduler heartbeat failed; retrying on the next tick")
                if stop.wait(self.settings.scheduler_tick_seconds):
                    break
                self.ensure_lock(lock)         # outside the try below: a lost lock must end the process
                try:
                    self.tick()
                except Exception:   # a database hiccup must not end the service; next tick retries
                    logger.exception("Scheduler tick failed; retrying on the next tick")
        finally:
            if lock is not None:
                lock.release()
            logger.info("pi-scheduler stopped (%s)", stop.reason or "error")

    @staticmethod
    def ensure_lock(lock: Any) -> None:
        """Raise :class:`SchedulerLockLost` unless this process still holds the scheduler lock."""
        if lock is not None and not lock.is_held():
            logger.error("pi-scheduler lost its advisory-lock session; stopping so that only one scheduler runs")
            raise SchedulerLockLost("the scheduler advisory lock is no longer held by this process")


def describe_due(config: SchedulerConfig, now: datetime) -> list[dict[str, Any]]:
    """For status output: each schedule's interval right now."""
    return [{"schedule": s.key, "enabled": s.enabled, "interval_now_s": s.interval_at(now),
             "in_window": s.in_window(now), "windows": [w.describe() for w in s.windows]}
            for s in config.schedules.values()]


def sync_report(config: SchedulerConfig, db: dict[str, tuple[bool, str]] | None,
                settings: Settings) -> list[dict[str, Any]]:
    """Per schedule: file vs database configuration and whether the scheduler would run it now."""
    rows = []
    for s in config.schedules.values():
        stored = (db or {}).get(s.key)
        reason = (gate_refusal(s, db, settings, config.enabled()) if s.enabled
                  else "disabled in the configuration file")
        if reason is None and not settings.scheduler_global_enable:
            reason = "SCHEDULER_GLOBAL_ENABLE=false"
        rows.append({"schedule": s.key, "file_enabled": s.enabled,
                     "db_enabled": None if stored is None else stored[0],
                     "in_sync": None if stored is None else stored[1] == s.hash,
                     "file_hash": s.hash[:12], "db_hash": None if stored is None else stored[1][:12],
                     "would_run": reason is None, "reason": reason})
    return rows


__all__ = ["CheckReport", "ErrorClass", "Scheduler", "SchedulerAlreadyRunning", "SchedulerLockLost",
           "describe_due", "gate_refusal", "sync_report"]
