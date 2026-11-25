"""PostgresJobRepository against the real pi_jobs schema, inside one transaction that is always rolled back.

Skipped while migration 0011_pi_jobs is not applied. NOT run as part of the scheduler implementation:
running it needs separate approval. Every row it writes is written inside ONE outer transaction that is
always rolled back (``repo`` fixture); nothing is committed. ``test_lost_session_*`` opens one extra
session (advisory lock, no rows) and terminates it.

Fixture order (SQLAlchemy 2 autobegin): the outer transaction is begun BEFORE the first statement on
the connection; executing first and calling ``begin()`` afterwards raises ``InvalidRequestError``.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from config.settings import Settings
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.scheduler.locks import AdvisoryLockFactory
from src.scheduler.models import CheckOutcome, ErrorClass, ExecutionOutcome, JobStatus, WatermarkStatus
from src.scheduler.repository import PostgresJobRepository
from tests.support.scheduler import detection, schedule

pytestmark = pytest.mark.integration
NOW = datetime(2026, 11, 25, 10, 0, tzinfo=ZoneInfo("Asia/Kolkata"))
TABLES = ("schedule", "availability_check", "job", "job_attempt", "source_watermark", "process_heartbeat")


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
    """A connection whose outer transaction is begun FIRST and always rolled back."""
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


def _sync(repo: PostgresJobRepository, *keys: str, group: str = "G") -> list:
    schedules = [schedule(k, lock_group=group) for k in keys]
    repo.sync_schedules(schedules, NOW)
    return schedules


def _finish(repo, job, worker, status, mark, error_class=None):
    return repo.finish(job, worker, NOW, status, ExecutionOutcome(status, error_class=error_class,
                                                                  error="x" if error_class else None),
                       None, watermark=mark)


def test_enqueue_dedupes_active_jobs_and_claim_respects_groups(repo: PostgresJobRepository) -> None:
    a, b = _sync(repo, "QA_ONE", "QA_TWO")
    first = repo.enqueue(a, detection("d1", "v1"), NOW)
    assert first is not None and repo.enqueue(a, detection("d1", "v1"), NOW) is None
    second = repo.enqueue(b, detection("d1", "v1"), NOW)
    job = repo.claim("w1", NOW, 600)
    assert job.job_id == first and job.status is JobStatus.RUNNING and job.attempts == 1
    assert repo.claim("w2", NOW, 600) is None            # same lock group already running
    assert repo.renew_lease(first, "w1", NOW, 600) and not repo.renew_lease(first, "w2", NOW, 600)
    assert _finish(repo, job, "w1", JobStatus.SUCCEEDED, WatermarkStatus.INGESTED)
    assert repo.claim("w2", NOW, 600).job_id == second


def test_enqueue_writes_the_queued_watermark_atomically(repo: PostgresJobRepository) -> None:
    [s] = _sync(repo, "QA_ATOMIC")
    job_id = repo.enqueue(s, detection("d1", "v1"), NOW)
    mark = repo.get_watermark("QA_ATOMIC", "d1")
    assert (mark.version_token, mark.status, mark.last_job_id) == ("v1", WatermarkStatus.QUEUED, job_id)
    assert repo.enqueue(s, detection("d1", "v1"), NOW + timedelta(minutes=1)) is None
    assert repo.get_watermark("QA_ATOMIC", "d1").updated_at == mark.updated_at   # duplicate: untouched


def test_finish_marks_only_its_own_version(repo: PostgresJobRepository) -> None:
    [s] = _sync(repo, "QA_VERSION")
    repo.enqueue(s, detection("d1", "v1"), NOW)
    old = repo.claim("w1", NOW, 600)
    newer = repo.enqueue(s, detection("d1", "v2"), NOW)
    assert _finish(repo, old, "w1", JobStatus.SUCCEEDED, WatermarkStatus.INGESTED)
    mark = repo.get_watermark("QA_VERSION", "d1")
    assert (mark.version_token, mark.status, mark.last_job_id) == ("v2", WatermarkStatus.QUEUED, newer)


def test_requeue_is_version_aware(repo: PostgresJobRepository) -> None:
    [s] = _sync(repo, "QA_REQUEUE")
    old = repo.enqueue(s, detection("d1", "v1"), NOW)
    assert _finish(repo, repo.claim("w1", NOW, 600), "w1", JobStatus.FAILED, WatermarkStatus.QUARANTINED,
                   ErrorClass.PERMANENT)
    repo.enqueue(s, detection("d1", "v2"), NOW)
    assert _finish(repo, repo.claim("w1", NOW, 600), "w1", JobStatus.SUCCEEDED, WatermarkStatus.INGESTED)
    done, message = repo.requeue(old, NOW)
    assert not done and "v2" in message
    assert repo.requeue(old, NOW, force=True) == (True, "requeued")
    assert repo.get_watermark("QA_REQUEUE", "d1").version_token == "v2"         # newer version untouched


def test_recovery_watermarks_checks_and_admin(repo: PostgresJobRepository) -> None:
    [s] = _sync(repo, "QA_REC")
    job_id = repo.enqueue(s, detection("d1", "v1"), NOW)
    job = repo.claim("w1", NOW, 60)
    recovered = repo.recover_expired_leases(NOW + timedelta(minutes=5))
    assert [j.job_id for j in recovered] == [job_id] and recovered[0].status is JobStatus.RETRY_WAIT
    assert repo.finish(job, "w1", NOW, JobStatus.SUCCEEDED, ExecutionOutcome(JobStatus.SUCCEEDED), None,
                       watermark=WatermarkStatus.INGESTED) is False
    assert repo.get_watermark("QA_REC", "d1").status is WatermarkStatus.QUEUED   # lost lease: no relabel
    check_id = repo.record_check("QA_REC", NOW, CheckOutcome.NEW_DATA, duration_ms=5, detections=1, jobs_created=1,
                                 detail={"x": 1}, error=None, error_class=None)
    assert check_id and repo.last_check_times()["QA_REC"] == NOW
    assert repo.cancel(job_id, NOW) and repo.requeue(job_id, NOW)[0]
    assert any(r["job_id"] == job_id for r in repo.list_jobs(schedule_key="QA_REC"))


def test_sync_stores_the_config_hash_and_never_enables(repo: PostgresJobRepository) -> None:
    on = schedule("QA_HASH", enabled=True)
    assert repo.sync_schedules([on], NOW)["QA_HASH"].startswith("inserted")
    assert repo.db_schedules()["QA_HASH"] == (False, on.hash)          # JSONB round trip keeps the hash
    assert repo.sync_schedules([on], NOW)["QA_HASH"] == "unchanged"
    changed = schedule("QA_HASH", enabled=True, every="45m")
    assert repo.sync_schedules([changed], NOW)["QA_HASH"].startswith("updated")
    assert repo.db_schedules()["QA_HASH"] == (False, changed.hash)


def test_application_role_privileges(connection) -> None:
    role = connection.execute(text("SELECT current_user")).scalar_one()
    if connection.execute(text("SELECT has_column_privilege('pi_jobs.schedule', 'enabled', 'UPDATE')")).scalar():
        pytest.skip(f"connected as {role}, which may update 'enabled' (owner/admin); run as pi_app")
    for table in TABLES:
        assert not connection.execute(text("SELECT has_table_privilege(:t, 'DELETE') OR "
                                           "has_table_privilege(:t, 'TRUNCATE')"), {"t": f"pi_jobs.{table}"}).scalar()
    repo = PostgresJobRepository(SavepointDatabase(connection))
    _sync(repo, "QA_PRIV")
    with pytest.raises(ProgrammingError, match="permission denied"):
        repo.set_enabled("QA_PRIV", True, NOW)                          # savepoint rolled back; outer intact
    assert repo.db_schedules()["QA_PRIV"][0] is False


def test_retention_foreign_keys_set_null_and_cascade(connection) -> None:
    if not connection.execute(text("SELECT has_table_privilege('pi_jobs.availability_check', 'DELETE') AND "
                                   "has_table_privilege('pi_jobs.job', 'DELETE')")).scalar():
        pytest.skip("needs the schema owner/admin (pi_app has no DELETE by design)")
    repo = PostgresJobRepository(SavepointDatabase(connection))
    [s] = _sync(repo, "QA_RETAIN")
    check_id = repo.record_check("QA_RETAIN", NOW, CheckOutcome.NEW_DATA, duration_ms=1, detections=1,
                                 jobs_created=1, detail={}, error=None, error_class=None)
    job_id = repo.enqueue(s, detection("d1", "v1"), NOW, check_id)
    _finish(repo, repo.claim("w1", NOW, 600), "w1", JobStatus.SUCCEEDED, WatermarkStatus.INGESTED)
    connection.execute(text("DELETE FROM pi_jobs.availability_check WHERE check_id = :c"), {"c": check_id})
    assert connection.execute(text("SELECT check_id FROM pi_jobs.job WHERE job_id = :j"), {"j": job_id}).scalar() is None
    connection.execute(text("DELETE FROM pi_jobs.job WHERE job_id = :j"), {"j": job_id})
    assert repo.get_watermark("QA_RETAIN", "d1").last_job_id is None
    assert connection.execute(text("SELECT count(*) FROM pi_jobs.job_attempt WHERE job_id = :j"),
                              {"j": job_id}).scalar() == 0


def test_lost_session_releases_the_scheduler_lock(manager: DatabaseManager) -> None:
    lock = AdvisoryLockFactory(manager.engine).try_acquire("QA_INTEGRATION_LOCK")   # never the real scheduler lock
    assert lock is not None and lock.is_held()
    pid = lock._connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
    lock._connection.commit()
    with manager.engine.connect() as other:
        assert other.execute(text("SELECT pg_terminate_backend(:p)"), {"p": pid}).scalar()
        other.commit()
    assert lock.is_held() is False
    lock.release()                                                       # tolerated on a dead session
    again = AdvisoryLockFactory(manager.engine).try_acquire("QA_INTEGRATION_LOCK")
    assert again is not None
    again.release()


def test_connection_fixture_is_rolled_back(manager: DatabaseManager) -> None:
    with manager.engine.connect() as conn:
        outer = conn.begin()
        try:
            if conn.execute(text("SELECT to_regclass('pi_jobs.job')")).scalar() is None:
                pytest.skip("pi_jobs schema not present")
            assert conn.execute(text("SELECT count(*) FROM pi_jobs.schedule WHERE schedule_key LIKE 'QA_%'"
                                     )).scalar() == 0
        finally:
            outer.rollback()
