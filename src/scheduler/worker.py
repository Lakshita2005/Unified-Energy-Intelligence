"""pi-worker: claim jobs, run the existing ingestion runners, record the outcome.

``WORKER_CONCURRENCY`` slots (threads) each loop: claim a due job (``FOR UPDATE SKIP LOCKED``), take the
advisory lock of its ``lock_group`` (one running job per source group: protects the source's rate limits
and avoids overlapping loads), renew the lease every ``WORKER_HEARTBEAT_SECONDS`` while the runner
executes, then apply the retry policy:

=======================  ==========================================  =========================
outcome                  job status                                  watermark
=======================  ==========================================  =========================
all runs SUCCESS         SUCCEEDED                                   INGESTED
some records rejected    PARTIAL (final)                             INGESTED_WITH_REJECTS
not published (404 ...)  NOT_PUBLISHED (final; re-detected later)    NOT_PUBLISHED
transient failure        RETRY_WAIT, next = now + backoff            QUEUED
                         FAILED once ``max_attempts`` are used       FAILED (cool-down)
permanent failure        FAILED (not retried)                        QUARANTINED
=======================  ==========================================  =========================

Backoff: ``backoff_base * 2 ** (attempt - 1)``, capped at ``backoff_max`` (per schedule). If the worker
dies, its lease expires and the scheduler puts the job back to RETRY_WAIT; re-running is safe because the
pipelines skip identical documents and unchanged records.

The job state, the attempt row and the watermark are written in ONE transaction
(:meth:`JobRepository.finish`), and the watermark is only changed while it still refers to the job's own
version: a late finish of an older version never relabels a newer detection.
"""

from __future__ import annotations

import threading
import time
from datetime import timedelta

from config.settings import Settings
from src.common.logging_config import get_logger
from src.scheduler.clock import Clock
from src.scheduler.executors import Executor
from src.scheduler.locks import LockFactory
from src.scheduler.models import ErrorClass, ExecutionOutcome, Job, JobStatus, WatermarkStatus
from src.scheduler.process import StopSignal
from src.scheduler.repository import JobRepository
from src.scheduler.retry import backoff_seconds, classify_exception

logger = get_logger(__name__)

_WATERMARK = {JobStatus.SUCCEEDED: WatermarkStatus.INGESTED, JobStatus.PARTIAL: WatermarkStatus.INGESTED_WITH_REJECTS,
              JobStatus.NOT_PUBLISHED: WatermarkStatus.NOT_PUBLISHED, JobStatus.RETRY_WAIT: WatermarkStatus.QUEUED}


def watermark_for(status: JobStatus, outcome: ExecutionOutcome) -> WatermarkStatus:
    # FAILED, or PARTIAL because independent units failed (decide): the item is retried after the failure
    # cool-down (TRANSIENT) or quarantined (PERMANENT); for a MOSDAC series only the missing plants are fetched.
    if status is JobStatus.FAILED or (status is JobStatus.PARTIAL and outcome.status is JobStatus.FAILED):
        return (WatermarkStatus.QUARANTINED if outcome.error_class is not ErrorClass.TRANSIENT
                else WatermarkStatus.FAILED)
    return _WATERMARK[status]


def decide(job: Job, outcome: ExecutionOutcome) -> tuple[JobStatus, float | None]:
    """(final status, retry delay in seconds or None) for an attempt outcome.

    A failed attempt whose runner stored some independent units (``detail["partial_success"]``, e.g. some
    MOSDAC plants) is retried like any failure; once retries are used up it ends PARTIAL, not FAILED, because
    data was stored. Its ``last_error`` names the failed units, and the health check reports it.
    """
    if outcome.status is not JobStatus.FAILED:
        return outcome.status, None
    if outcome.error_class is ErrorClass.TRANSIENT and job.attempts < job.max_attempts:
        return JobStatus.RETRY_WAIT, backoff_seconds(job.attempts, base=job.backoff_base_seconds,
                                                     maximum=job.backoff_max_seconds)
    if outcome.detail.get("partial_success"):
        return JobStatus.PARTIAL, None
    return JobStatus.FAILED, None


class Worker:
    def __init__(self, *, repository: JobRepository, executor: Executor, locks: LockFactory, clock: Clock,
                 settings: Settings, worker_id: str) -> None:
        self.repository = repository
        self.executor = executor
        self.locks = locks
        self.clock = clock
        self.settings = settings
        self.worker_id = worker_id

    # ------------------------------------------------------------------ one job
    def run_once(self, slot: int = 0) -> Job | None:
        """Claim and execute at most one job. Returns the job (None if nothing was due)."""
        owner = f"{self.worker_id}/slot{slot}"
        job = self.repository.claim(owner, self.clock.now(), self.settings.worker_lease_seconds)
        if job is None:
            return None
        lock = self.locks.try_acquire(f"group:{job.lock_group}")
        if lock is None:
            # another process is running a job of this source group: give the job back untouched
            self.repository.release(job, owner, self.clock.now(), self.settings.worker_poll_seconds)
            logger.info("Job %s: source group %s is busy; released", job.job_id, job.lock_group)
            return None
        try:
            self._execute(job, owner)
        finally:
            lock.release()
        return job

    def _execute(self, job: Job, owner: str) -> None:
        logger.info("Job %s %s %s attempt %d/%d: %s", job.job_id, job.schedule_key, job.item_key, job.attempts,
                    job.max_attempts, job.target)
        stop_heartbeat = threading.Event()
        heartbeat = threading.Thread(target=self._heartbeat, args=(job, owner, stop_heartbeat),
                                     name=f"lease-{job.job_id}", daemon=True)
        heartbeat.start()
        try:
            try:
                outcome = self.executor.execute(job.target)
            except Exception as exc:
                error_class = classify_exception(exc)
                outcome = ExecutionOutcome(JobStatus.NOT_PUBLISHED if error_class is ErrorClass.NOT_PUBLISHED
                                           else JobStatus.FAILED, error=f"{type(exc).__name__}: {exc}"[:4000],
                                           error_class=error_class)
                logger.warning("Job %s raised %s (%s)", job.job_id, outcome.error, error_class.value)
        finally:
            stop_heartbeat.set()
            heartbeat.join(timeout=5)
        now = self.clock.now()
        status, delay = decide(job, outcome)
        next_attempt = now + timedelta(seconds=delay) if delay is not None else None
        owned = self.repository.finish(job, owner, now, status, outcome, next_attempt,
                                       watermark=watermark_for(status, outcome))
        if not owned:
            logger.error("Job %s finished (%s) after its lease was lost; the job state was left unchanged",
                         job.job_id, status.value)
            return
        logger.info("Job %s -> %s (runs %s, inserted %d, rejected %d)%s%s", job.job_id, status.value,
                    outcome.ingestion_run_ids, outcome.records_inserted, outcome.records_rejected,
                    f", retry in {delay:.0f}s" if delay is not None else "",
                    f", error: {outcome.error}" if outcome.error else "")

    def _heartbeat(self, job: Job, owner: str, stop: threading.Event) -> None:
        interval = self.settings.worker_heartbeat_seconds
        while not stop.wait(interval):
            try:
                if not self.repository.renew_lease(job.job_id, owner, self.clock.now(),
                                                   self.settings.worker_lease_seconds):
                    logger.error("Job %s: lease could not be renewed (recovered by the scheduler?)", job.job_id)
                    return
            except Exception:
                logger.exception("Job %s: lease renewal failed; retrying", job.job_id)

    # ------------------------------------------------------------------ main loop
    def run(self, stop: StopSignal, concurrency: int | None = None) -> None:
        slots = concurrency or self.settings.worker_concurrency
        logger.info("pi-worker %s started with %d slot(s)", self.worker_id, slots)
        try:
            self.repository.recover_expired_leases(self.clock.now())
        except Exception:
            logger.exception("Lease recovery at start failed; the scheduler will retry it")
        threads = [threading.Thread(target=self._slot_loop, args=(slot, stop), name=f"pi-worker-slot{slot}")
                   for slot in range(slots)]
        for thread in threads:
            thread.start()
        while not stop.is_set():
            try:
                self.repository.beat(self.worker_id, "worker", self.clock.now(), {"slots": slots})
            except Exception:
                logger.exception("Heartbeat failed")
            stop.wait(self.settings.worker_heartbeat_seconds)
        grace = self.settings.worker_shutdown_grace_seconds
        logger.info("Waiting up to %ds for running jobs to finish", grace)
        deadline = time.monotonic() + grace
        for thread in threads:     # one shared deadline, not grace per thread
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        alive = [t.name for t in threads if t.is_alive()]
        if alive:
            logger.warning("Stopping with running jobs in %s; their leases expire and the scheduler requeues them",
                           alive)
        logger.info("pi-worker %s stopped (%s)", self.worker_id, stop.reason)

    def _slot_loop(self, slot: int, stop: StopSignal) -> None:
        while not stop.is_set():
            try:
                job = self.run_once(slot)
            except Exception:
                logger.exception("Worker slot %d: unexpected error; continuing", slot)
                job = None
            if job is None and stop.wait(self.settings.worker_poll_seconds):
                break
