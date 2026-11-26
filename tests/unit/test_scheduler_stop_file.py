"""A stop request (stop file or signal) is honoured BEFORE the scheduler's start-up work.

Task Scheduler deployment: the watchdog trigger may start pi-scheduler while C:\\PI\\run\\pi-scheduler.stop exists.
The process must then exit without taking the lock, recovering leases or running the catch-up check.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from src.scheduler.clock import FakeClock
from src.scheduler.config import SchedulerConfig
from src.scheduler.locks import SCHEDULER_LOCK, InProcessLockFactory
from src.scheduler.models import JobStatus
from src.scheduler.process import StopSignal
from src.scheduler.scheduler import Scheduler
from tests.support.scheduler import (
    FakeState,
    InMemoryJobRepository,
    ScriptedChecker,
    detection,
    new_data,
    schedule,
    settings,
)

T0 = datetime(2026, 11, 25, 10, 0, tzinfo=ZoneInfo("Asia/Kolkata"))


class _NoHttp:
    def close(self) -> None:
        pass


class _SpyLocks(InProcessLockFactory):
    def __init__(self) -> None:
        super().__init__()
        self.requested: list[str] = []

    def try_acquire(self, name: str):  # noqa: ANN201
        self.requested.append(name)
        return super().try_acquire(name)


def _scheduler(keys, repo, clock, locks, results=None):
    schedules = [schedule(k) for k in keys]
    checkers = {s.key: ScriptedChecker(s, list((results or {}).get(s.key, []))) for s in schedules}
    sched = Scheduler(config=SchedulerConfig({s.key: s for s in schedules}), checkers=checkers, repository=repo,
                      clock=clock, settings=settings(), http_factory=_NoHttp, state=FakeState(), locks=locks)
    return sched, checkers


def _expired_job(repo: InMemoryJobRepository, clock: FakeClock) -> int:
    """A RUNNING job whose lease has expired (what start-up recovery would requeue)."""
    job_id = repo.enqueue(schedule("OLD"), detection("d0", "v1"), clock.now())
    repo.claim("dead-worker", clock.now(), 60)
    clock.advance(minutes=5)
    return job_id


def test_stop_file_present_at_start_means_no_start_up_work(tmp_path) -> None:
    repo, clock, locks = InMemoryJobRepository(), FakeClock(T0), _SpyLocks()
    stale = _expired_job(repo, clock)
    sched, checkers = _scheduler(["STOP_A"], repo, clock, locks, {"STOP_A": [new_data(detection("d1", "v1"))]})
    stop_file = tmp_path / "pi-scheduler.stop"
    stop_file.write_text("stop")
    stop = StopSignal(stop_file)

    sched.run(stop)

    assert locks.requested == []                                   # the scheduler lock was never requested
    assert checkers["STOP_A"].contexts == []                            # no catch-up check
    assert repo.checks == []
    assert repo.job(stale).status is JobStatus.RUNNING             # no lease recovery
    assert [j.item_key for j in repo.jobs()] == ["d0"]             # no new job
    assert repo.heartbeats == {}
    assert "stop file" in stop.reason


def test_stop_requested_during_start_up_recovery_skips_the_catch_up(tmp_path) -> None:
    repo, clock, locks = InMemoryJobRepository(), FakeClock(T0), _SpyLocks()
    stale = _expired_job(repo, clock)
    sched, checkers = _scheduler(["STOP_A"], repo, clock, locks, {"STOP_A": [new_data(detection("d1", "v1"))]})
    stop = StopSignal()
    recover = sched.recover

    def recover_then_stop(now):  # noqa: ANN001, ANN202
        result = recover(now)
        stop.request("test")
        return result

    sched.recover = recover_then_stop
    sched.run(stop)

    assert locks.requested == [SCHEDULER_LOCK] and locks.held == set()   # taken, then released
    assert repo.job(stale).status is JobStatus.RETRY_WAIT                # recovery itself completed
    assert checkers["STOP_A"].contexts == []                                  # but no catch-up check
    assert [j.item_key for j in repo.jobs()] == ["d0"]


def test_stop_requested_during_the_catch_up_skips_the_remaining_checks() -> None:
    repo, clock, locks = InMemoryJobRepository(), FakeClock(T0), _SpyLocks()
    sched, checkers = _scheduler(["A_FIRST", "B_SECOND"], repo, clock, locks,
                                 {"A_FIRST": [new_data(detection("d1", "v1"))],
                                  "B_SECOND": [new_data(detection("d2", "v1"))]})
    stop = StopSignal()
    check = checkers["A_FIRST"].check

    def check_then_stop(ctx):  # noqa: ANN001, ANN202
        result = check(ctx)
        stop.request("test")
        return result

    checkers["A_FIRST"].check = check_then_stop
    sched.run(stop)

    assert len(checkers["A_FIRST"].contexts) == 1                  # the running check completes (atomic)
    assert [j.item_key for j in repo.jobs()] == ["d1"]             # ... and its job is queued
    assert checkers["B_SECOND"].contexts == []                     # the next check is not started
    assert locks.held == set()


def test_without_a_stop_the_start_up_still_recovers_and_catches_up() -> None:
    repo, clock, locks = InMemoryJobRepository(), FakeClock(T0), _SpyLocks()
    stale = _expired_job(repo, clock)
    sched, checkers = _scheduler(["STOP_A"], repo, clock, locks, {"STOP_A": [new_data(detection("d1", "v1"))]})
    stop = StopSignal()
    beat = repo.beat

    def beat_then_stop(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        beat(*args, **kwargs)
        stop.request("test")

    repo.beat = beat_then_stop
    sched.run(stop)

    assert repo.job(stale).status is JobStatus.RETRY_WAIT
    assert len(checkers["STOP_A"].contexts) == 1
    assert {j.item_key for j in repo.jobs()} == {"d0", "d1"}
    assert repo.heartbeats and clock.now() - T0 == timedelta(minutes=5)
