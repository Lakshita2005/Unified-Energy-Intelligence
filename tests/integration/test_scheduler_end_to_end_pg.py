"""Scheduler -> queue -> worker against the real pi_jobs schema, every row inside ONE rolled-back transaction.

Covers what tests/integration/test_scheduler_queue.py does not: the real scheduler lock name, a check recorded
and its job queued through ``Scheduler.run_check``, the worker claiming and executing a job end to end (attempt
row, ingestion run ids, counts, watermark) and both IEX outcomes stored. Runners and checkers are fakes: no
network, no ingestion, no pi.* table is written. Skipped while migration 0011_pi_jobs is not applied.
Committed side effects: none, except that the identity sequences of pi_jobs advance (rollback does not undo
sequences) and two short extra sessions take and release advisory locks.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text

from config.settings import Settings
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.ingestion.models import IngestionResult, RunStatus
from src.scheduler.clock import FakeClock
from src.scheduler.config import SchedulerConfig
from src.scheduler.executors import Executor
from src.scheduler.locks import SCHEDULER_LOCK, AdvisoryLockFactory
from src.scheduler.models import CheckOutcome, JobStatus, WatermarkStatus
from src.scheduler.repository import PostgresJobRepository
from src.scheduler.runtime import load_config
from src.scheduler.scheduler import Scheduler
from src.scheduler.worker import Worker
from tests.support.scheduler import FakeState, ScriptedChecker, detection, new_data, schedule, settings

pytestmark = pytest.mark.integration
NOW = datetime(2026, 11, 25, 10, 0, tzinfo=ZoneInfo("Asia/Kolkata"))
QUIET = settings(worker_heartbeat_seconds=3600)      # the lease thread never touches the shared test connection


class _NoHttp:
    def close(self) -> None:
        pass


@pytest.fixture(scope="module")
def manager() -> Iterator[DatabaseManager]:
    try:
        db = DatabaseManager.from_settings(Settings())
    except ConfigurationError as exc:
        pytest.skip(f"PostgreSQL not configured: {exc}")
    yield db
    db.dispose()


@pytest.fixture
def connection(manager: DatabaseManager):
    """Outer transaction begun FIRST and always rolled back (same pattern as test_scheduler_queue.py)."""
    with manager.engine.connect() as conn:
        outer = conn.begin()
        try:
            if conn.execute(text("SELECT to_regclass('pi_jobs.job')")).scalar() is None:
                pytest.skip("pi_jobs schema not present (migration 0011_pi_jobs not applied)")
            yield conn
        finally:
            outer.rollback()


@pytest.fixture
def repo(connection) -> PostgresJobRepository:
    return PostgresJobRepository(SavepointDatabase(connection))


def _result(run_id: int, status: RunStatus = RunStatus.SUCCESS, error: str | None = None) -> IngestionResult:
    return IngestionResult(ingestion_run_id=run_id, dataset_code="QA", status=status, records_read=10,
                           records_inserted=10, records_rejected=0, documents_processed=1, error=error)


def _worker(repo, manager, executor) -> Worker:
    return Worker(repository=repo, executor=executor, locks=AdvisoryLockFactory(manager.engine),
                  clock=FakeClock(NOW), settings=QUIET, worker_id="qa-e2e")


def _row(connection, sql: str, **params):
    return connection.execute(text(sql), params).mappings().one()


def test_real_scheduler_lock_is_acquired_and_exclusive(manager: DatabaseManager) -> None:
    first = AdvisoryLockFactory(manager.engine).try_acquire(SCHEDULER_LOCK)
    if first is None:
        pytest.skip("the scheduler lock is held: a pi-scheduler process is running")
    try:
        assert first.is_held()
        assert AdvisoryLockFactory(manager.engine).try_acquire(SCHEDULER_LOCK) is None   # a second scheduler
    finally:
        first.release()
    again = AdvisoryLockFactory(manager.engine).try_acquire(SCHEDULER_LOCK)
    assert again is not None
    again.release()


def test_check_is_recorded_and_its_job_queued_once(repo: PostgresJobRepository, connection) -> None:
    s = schedule("QA_E2E_CHECK", lock_group="QA")
    repo.sync_schedules([s], NOW)
    checker = ScriptedChecker(s, [new_data(detection("d1", "v1")), new_data(detection("d1", "v1"))])
    sched = Scheduler(config=SchedulerConfig({s.key: s}), checkers={s.key: checker}, repository=repo,
                      clock=FakeClock(NOW), settings=settings(), http_factory=_NoHttp, state=FakeState(), locks=None)
    first = sched.run_check(s, NOW)
    second = sched.run_check(s, NOW)                                 # same version detected again
    assert len(first.jobs_created) == 1 and second.jobs_created == [] and "d1" in second.skipped
    checks = connection.execute(text("SELECT outcome, detections, jobs_created FROM pi_jobs.availability_check "
                                     "WHERE schedule_key = 'QA_E2E_CHECK' ORDER BY check_id")).all()
    assert [tuple(c) for c in checks] == [(CheckOutcome.NEW_DATA.value, 1, 1), (CheckOutcome.NEW_DATA.value, 1, 0)]
    job = _row(connection, "SELECT status, check_id FROM pi_jobs.job WHERE schedule_key = 'QA_E2E_CHECK'")
    assert job["status"] == JobStatus.PENDING.value and job["check_id"] == first.check_id
    assert repo.get_watermark("QA_E2E_CHECK", "d1").status is WatermarkStatus.QUEUED


def test_worker_claims_executes_and_records_attempt_runs_and_watermark(repo, connection, manager) -> None:
    s = schedule("QA_E2E_WORK", lock_group="QA")
    repo.sync_schedules([s], NOW)
    job_id = repo.enqueue(s, detection("d1", "v1"), NOW)
    calls = []

    def runner(target, cfg):  # noqa: ANN001, ANN202
        calls.append(target)
        return [(RunStatus.SUCCESS, _result(101), None)], {}

    job = _worker(repo, manager, Executor(QUIET, runners={"fake": runner})).run_once()
    assert job is not None and job.job_id == job_id and len(calls) == 1
    row = _row(connection, "SELECT status, attempts, ingestion_run_ids, records_inserted, lease_owner "
                           "FROM pi_jobs.job WHERE job_id = :j", j=job_id)
    assert (row["status"], row["attempts"], list(row["ingestion_run_ids"]), row["records_inserted"],
            row["lease_owner"]) == (JobStatus.SUCCEEDED.value, 1, [101], 10, None)
    attempt = _row(connection, "SELECT attempt_no, status, ingestion_run_ids FROM pi_jobs.job_attempt "
                               "WHERE job_id = :j", j=job_id)
    assert (attempt["attempt_no"], attempt["status"], list(attempt["ingestion_run_ids"])) == \
           (1, JobStatus.SUCCEEDED.value, [101])
    assert repo.get_watermark("QA_E2E_WORK", "d1").status is WatermarkStatus.INGESTED
    assert _worker(repo, manager, Executor(QUIET, runners={"fake": runner})).run_once() is None   # nothing left


@pytest.mark.parametrize(("snapshot", "skipped", "status", "runs", "mark"), [
    (RunStatus.SUCCESS, None, JobStatus.SUCCEEDED, [201, 202], WatermarkStatus.INGESTED),
    (None, "pi.iex_market_snapshot missing", JobStatus.FAILED, [201], WatermarkStatus.QUARANTINED),
])
def test_both_iex_outcomes_are_stored(repo, connection, manager, monkeypatch, snapshot, skipped, status, runs,
                                      mark) -> None:
    summary = SimpleNamespace(status=RunStatus.SUCCESS, result=_result(201), error=None,
                              snapshot=None if snapshot is None else _result(202, snapshot), snapshot_skipped=skipped)
    monkeypatch.setattr("src.pipelines.iex.runner.run_iex", lambda *a, **k: summary)   # no network, no load
    s = schedule("QA_E2E_IEX", lock_group="QA_IEX")
    repo.sync_schedules([s], NOW)
    job_id = repo.enqueue(s, detection("2026-11-24", "v1", runner="iex", kind="dam", start_date="2026-11-24",
                                       end_date="2026-11-24"), NOW)
    _worker(repo, manager, Executor(QUIET)).run_once()
    row = _row(connection, "SELECT status, ingestion_run_ids, result FROM pi_jobs.job WHERE job_id = :j", j=job_id)
    assert row["status"] == status.value and list(row["ingestion_run_ids"]) == runs
    components = row["result"]["components"]
    assert components["price"]["ingestion_run_id"] == 201 and components["price"]["status"] == "SUCCESS"
    if snapshot is None:
        assert components["snapshot"]["status"] != "SUCCESS"
    else:
        assert components["snapshot"]["ingestion_run_id"] == 202
    assert repo.get_watermark("QA_E2E_IEX", "2026-11-24").status is mark


APPROVED_ENABLED = {"IEX_DAM", "IEX_GDAM", "IEX_RTM", "GRID_INDIA_PSP",           # approved 2026-11-26
                    "CEA_COAL_DAILY", "CEA_DGR_DAILY", "CEA_PSP_MONTHLY"}         # approved 2026-11-26


def test_only_approved_schedules_are_enabled(connection) -> None:
    config, _ = load_config(Settings())
    assert {s.key for s in config.enabled()} <= APPROVED_ENABLED                              # file
    enabled_in_db = {r[0] for r in connection.execute(text("SELECT schedule_key FROM pi_jobs.schedule WHERE enabled"))}
    assert enabled_in_db <= APPROVED_ENABLED                                                  # database
