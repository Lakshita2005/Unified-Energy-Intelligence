"""Scheduler and worker behaviour on an in-memory repository with a fake clock (no database, no network)."""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from src.common.exceptions import HttpRequestError
from src.ingestion.models import IngestionResult, RunStatus
from src.scheduler.clock import FakeClock
from src.scheduler.config import SchedulerConfig
from src.scheduler.executors import Executor
from src.scheduler.locks import InProcessLockFactory
from src.scheduler.models import CheckOutcome, ErrorClass, JobStatus, WatermarkStatus
from src.scheduler.process import StopSignal
from src.scheduler.scheduler import Scheduler, SchedulerAlreadyRunning
from src.scheduler.worker import Worker
from tests.support.scheduler import (
    FakeState,
    InMemoryJobRepository,
    ScriptedChecker,
    detection,
    new_data,
    schedule,
    settings,
)

IST = ZoneInfo("Asia/Kolkata")
T0 = datetime(2026, 11, 25, 10, 0, tzinfo=IST)


class _NoHttp:
    def close(self) -> None:
        pass


def make_scheduler(schedules, results=None, *, repo=None, clock=None, cfg=None, locks=None):
    config = SchedulerConfig({s.key: s for s in schedules})
    checkers = {s.key: ScriptedChecker(s, list((results or {}).get(s.key, []))) for s in schedules}
    repo = repo or InMemoryJobRepository()
    clock = clock or FakeClock(T0)
    sched = Scheduler(config=config, checkers=checkers, repository=repo, clock=clock, settings=cfg or settings(),
                      http_factory=_NoHttp, state=FakeState(), locks=locks)
    return sched, repo, clock, checkers


def ok_run(run_id: int = 1, *, status: RunStatus = RunStatus.SUCCESS, rejected: int = 0, error: str | None = None):
    result = IngestionResult(ingestion_run_id=run_id, dataset_code="X", status=status, records_read=10,
                             records_inserted=10 - rejected, records_rejected=rejected, documents_processed=1,
                             error=error)
    return lambda target, cfg: ([(status, result, error)], {})


def make_worker(repo, clock, runner, *, locks=None, cfg=None):
    return Worker(repository=repo, executor=Executor(cfg or settings(), runners={"fake": runner}),
                  locks=locks or InProcessLockFactory(), clock=clock, settings=cfg or settings(), worker_id="w1")


# =========================================================================== scheduler: gating and timing
def test_nothing_is_checked_while_the_global_switch_or_the_schedule_is_off() -> None:
    on, off = schedule("ON_KEY"), schedule("OFF_KEY", enabled=False)
    sched, repo, _clock, checkers = make_scheduler([on, off], cfg=settings(scheduler_global_enable=False))
    assert sched.tick(force=True) == [] and repo.checks == []
    sched2, repo2, _c, checkers2 = make_scheduler([on, off])
    assert [r.schedule_key for r in sched2.tick()] == ["ON_KEY"] and checkers2["OFF_KEY"].contexts == []
    repo3 = InMemoryJobRepository()
    repo3.db = {"ON_KEY": (False, on.hash)}              # synced, but not enabled in pi_jobs.schedule
    sched3, *_ = make_scheduler([on], repo=repo3)
    assert sched3.tick(force=True) == []


def test_checks_follow_the_interval_and_windows() -> None:
    s = schedule("WIN_KEY", every="off", windows=[{"start": "10:00", "end": "12:00", "every": "15m"}])
    sched, repo, clock, _ = make_scheduler([s])
    assert len(sched.tick()) == 1
    clock.advance(minutes=14)
    assert sched.tick() == []
    clock.advance(minutes=1)
    assert len(sched.tick()) == 1
    clock.set(datetime(2026, 11, 25, 12, 30, tzinfo=IST))   # outside the window, every = off
    assert sched.tick() == []
    assert [c["outcome"] for c in repo.checks] == [CheckOutcome.NO_NEW_DATA] * 2


# =========================================================================== scheduler: jobs and de-duplication
def test_detection_creates_one_job_and_repeated_detection_does_not_duplicate() -> None:
    s = schedule("DUP_KEY", every="1m")
    d = detection("2026-11-24", "sha-a")
    sched, repo, clock, _ = make_scheduler([s], {"DUP_KEY": [new_data(d), new_data(d)]})
    [report] = sched.tick()
    assert report.outcome is CheckOutcome.NEW_DATA and len(report.jobs_created) == 1
    job = repo.job(report.jobs_created[0])
    assert (job.status, job.dedupe_key) == (JobStatus.PENDING, "DUP_KEY|2026-11-24|sha-a")
    assert repo.get_watermark("DUP_KEY", "2026-11-24").status is WatermarkStatus.QUEUED
    clock.advance(minutes=1)
    [again] = sched.tick()
    assert again.jobs_created == [] and "active job" in again.skipped["2026-11-24"]
    assert len(repo.jobs()) == 1


def test_watermark_guards_ingested_quarantined_cooldown_and_revisions() -> None:
    s = schedule("WM_KEY", every="1m", failure_cooldown="6h")
    item = "2026-11-24"
    sched, repo, clock, checkers = make_scheduler([s])
    checker = checkers["WM_KEY"]

    def tick_with(version):
        checker.results.append(new_data(detection(item, version)))
        clock.advance(minutes=1)
        return sched.tick()[0]

    repo.set_watermark("WM_KEY", item, "v1", WatermarkStatus.INGESTED, 1, clock.now())
    assert "already ingested" in tick_with("v1").skipped[item]
    assert tick_with("v2").jobs_created                         # revised version -> new job
    repo.set_watermark("WM_KEY", item, "v3", WatermarkStatus.QUARANTINED, 2, clock.now())
    assert "quarantined" in tick_with("v3").skipped[item]
    repo.set_watermark("WM_KEY", item, "v4", WatermarkStatus.FAILED, 3, clock.now())
    assert "cool-down" in tick_with("v4").skipped[item]
    clock.advance(hours=6)
    assert tick_with("v4").jobs_created                         # after the cool-down it is retried
    repo.set_watermark("WM_KEY", item, "v5", WatermarkStatus.NOT_PUBLISHED, 4, clock.now())
    assert tick_with("v5").jobs_created                         # not published earlier -> tried again


def test_checker_exception_is_recorded_and_other_schedules_still_run() -> None:
    a, b = schedule("A_KEY", priority=1), schedule("B_KEY", priority=2)
    sched, repo, _clock, _ = make_scheduler(
        [a, b], {"A_KEY": [HttpRequestError("connection reset", url="x")], "B_KEY": [new_data(detection())]})
    reports = sched.tick()
    assert [(r.schedule_key, r.outcome) for r in reports] == [("A_KEY", CheckOutcome.SOURCE_ERROR),
                                                              ("B_KEY", CheckOutcome.NEW_DATA)]
    assert repo.checks[0]["error_class"] is ErrorClass.TRANSIENT and "connection reset" in repo.checks[0]["error"]


def test_restart_catches_up_immediately_and_recovers_expired_leases() -> None:
    s = schedule("CATCH_KEY", every="off", windows=[{"start": "12:00", "end": "13:00", "every": "15m"}])
    repo = InMemoryJobRepository()
    sched, repo, clock, _ = make_scheduler([s], {"CATCH_KEY": [new_data(detection("d1", "v1"))]}, repo=repo)
    sched.tick(force=True)                                       # outside the window, forced (start-up catch-up)
    job_id = repo.jobs()[0].job_id
    claimed = repo.claim("dead-worker", clock.now(), 600)       # a worker claims it, then the process dies
    assert claimed.job_id == job_id
    clock.advance(minutes=11)                                    # lease (10 min) expires
    # a NEW scheduler process (restart) sharing the same database
    sched2, _repo, _clock, checkers2 = make_scheduler([s], {"CATCH_KEY": [new_data(detection("d2", "v1"))]},
                                                      repo=repo, clock=clock)
    stop = StopSignal()
    beat = repo.beat

    def beat_then_stop(*args, **kwargs):                        # first heartbeat = after the catch-up check
        beat(*args, **kwargs)
        stop.request("test")

    repo.beat = beat_then_stop
    sched2.run(stop)
    assert repo.job(job_id).status is JobStatus.RETRY_WAIT and "lease expired" in repo.job(job_id).last_error
    assert len(checkers2["CATCH_KEY"].contexts) == 1             # catch-up check at start, outside the window
    assert {j.item_key for j in repo.jobs()} == {"d1", "d2"}
    assert repo.heartbeats


def test_only_one_scheduler_runs() -> None:
    locks = InProcessLockFactory()
    held = locks.try_acquire("scheduler")
    sched, *_ = make_scheduler([schedule()], locks=locks)
    with pytest.raises(SchedulerAlreadyRunning):
        sched.run(StopSignal())
    held.release()


# =========================================================================== worker
def enqueue(repo, clock, *, key="JOB_KEY", group="G", max_attempts=5, version="v1", item="2026-11-24"):
    s = schedule(key, lock_group=group, max_attempts=max_attempts)
    return repo.enqueue(s, detection(item, version), clock.now()), s


def test_successful_job_is_recorded_with_runs_and_watermark() -> None:
    repo, clock = InMemoryJobRepository(), FakeClock(T0)
    job_id, _ = enqueue(repo, clock)
    worker = make_worker(repo, clock, ok_run(41))
    assert worker.run_once().job_id == job_id
    job = repo.job(job_id)
    assert job.status is JobStatus.SUCCEEDED and repo.rows[job_id].runs == [41] and job.lease_owner is None
    assert repo.get_watermark("JOB_KEY", "2026-11-24").status is WatermarkStatus.INGESTED
    assert repo.attempts[-1]["status"] is JobStatus.SUCCEEDED
    assert worker.run_once() is None                             # nothing else due


def test_partial_is_final_and_marked() -> None:
    repo, clock = InMemoryJobRepository(), FakeClock(T0)
    job_id, _ = enqueue(repo, clock)
    make_worker(repo, clock, ok_run(status=RunStatus.PARTIAL, rejected=2)).run_once()
    assert repo.job(job_id).status is JobStatus.PARTIAL and repo.rows[job_id].rejected == 2
    assert repo.get_watermark("JOB_KEY", "2026-11-24").status is WatermarkStatus.INGESTED_WITH_REJECTS


def test_transient_failures_back_off_exponentially_then_fail() -> None:
    repo, clock = InMemoryJobRepository(), FakeClock(T0)
    job_id, s = enqueue(repo, clock, max_attempts=3)
    worker = make_worker(repo, clock, ok_run(status=RunStatus.FAILED, error="IexApiError: IEX returned HTTP 503"))
    delays = []
    for _ in range(3):
        before = clock.now()
        assert worker.run_once() is not None
        job = repo.job(job_id)
        if job.status is JobStatus.RETRY_WAIT:
            delays.append((job.next_attempt_at - before).total_seconds())
            assert worker.run_once() is None                     # not due yet
            clock.set(job.next_attempt_at)
    assert delays == [s.backoff_base_seconds, 2 * s.backoff_base_seconds]   # 120 s, 240 s
    assert repo.job(job_id).status is JobStatus.FAILED and repo.job(job_id).attempts == 3
    assert repo.get_watermark("JOB_KEY", "2026-11-24").status is WatermarkStatus.FAILED
    assert len(repo.attempts) == 3


def test_permanent_failure_is_quarantined_until_requeued_then_recovers() -> None:
    repo, clock = InMemoryJobRepository(), FakeClock(T0)
    job_id, _ = enqueue(repo, clock)
    failing = make_worker(repo, clock, ok_run(status=RunStatus.FAILED,
                                              error="InvalidWorkbookError: section C columns not found"))
    failing.run_once()
    assert repo.job(job_id).status is JobStatus.FAILED and repo.job(job_id).attempts == 1   # no retry
    assert repo.get_watermark("JOB_KEY", "2026-11-24").status is WatermarkStatus.QUARANTINED
    assert repo.requeue(job_id, clock.now())                     # after the parser fix
    make_worker(repo, clock, ok_run(77)).run_once()
    assert repo.job(job_id).status is JobStatus.SUCCEEDED
    assert repo.get_watermark("JOB_KEY", "2026-11-24").status is WatermarkStatus.INGESTED


def test_not_published_and_raised_exceptions_are_classified() -> None:
    repo, clock = InMemoryJobRepository(), FakeClock(T0)
    a, _ = enqueue(repo, clock, key="NP_KEY")
    make_worker(repo, clock, ok_run(status=RunStatus.FAILED, error="EmptyResponseError: data: []")).run_once()
    assert repo.job(a).status is JobStatus.NOT_PUBLISHED

    def boom(target, cfg):
        raise HttpRequestError("timed out after 4 attempts", url="x")

    b, _ = enqueue(repo, clock, key="EXC_KEY")
    make_worker(repo, clock, boom).run_once()
    assert repo.job(b).status is JobStatus.RETRY_WAIT and repo.rows[b].error_class == "TRANSIENT"


def test_source_group_lock_prevents_overlap_and_releases_without_using_an_attempt() -> None:
    repo, clock = InMemoryJobRepository(), FakeClock(T0)
    first, _ = enqueue(repo, clock, key="IEX_DAM", group="IEX")
    second, _ = enqueue(repo, clock, key="IEX_RTM", group="IEX")
    other, _ = enqueue(repo, clock, key="NPP_KEY", group="NPP")
    repo.claim("other-process", clock.now(), 600)                # IEX job 1 running elsewhere
    claimed_ids = []
    worker = make_worker(repo, clock, ok_run())
    while (job := worker.run_once()) is not None:
        claimed_ids.append(job.job_id)
    assert claimed_ids == [other]                                # the second IEX job waits for the first
    assert repo.job(second).status is JobStatus.PENDING and repo.job(second).attempts == 0
    locks = InProcessLockFactory()
    held = locks.try_acquire("group:NPP")                        # lock held by another worker process
    third, _ = enqueue(repo, clock, key="NPP2_KEY", group="NPP")
    assert make_worker(repo, clock, ok_run(), locks=locks).run_once() is None
    assert repo.job(third).status is JobStatus.PENDING and repo.job(third).attempts == 0
    held.release()


def test_lost_lease_leaves_the_recovered_job_alone() -> None:
    repo, clock = InMemoryJobRepository(), FakeClock(T0)
    job_id, _ = enqueue(repo, clock)

    def slow(target, cfg):
        clock.advance(minutes=20)                                # lease (10 min) expires while running
        repo.recover_expired_leases(clock.now())                 # the scheduler recovers it meanwhile
        return ok_run()(target, cfg)

    make_worker(repo, clock, slow).run_once()
    assert repo.job(job_id).status is JobStatus.RETRY_WAIT       # not overwritten by the late finish
    assert "lease lost" in repo.attempts[-1]["status"]
    assert repo.renew_lease(job_id, "w1/slot0", clock.now(), 600) is False


def test_worker_stops_gracefully(tmp_path) -> None:
    repo, clock = InMemoryJobRepository(), FakeClock(T0)
    stop_file = tmp_path / "STOP"
    stop = StopSignal(stop_file)
    stop_file.write_text("stop")
    make_worker(repo, clock, ok_run(), cfg=settings(worker_shutdown_grace_seconds=1)).run(stop, concurrency=2)
    assert stop.reason and "stop file" in stop.reason
