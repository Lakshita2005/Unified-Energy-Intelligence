"""Approved review fixes (2026-11-25): MOSDAC split, atomic queue/watermark writes, version-aware requeue,
scheduler lock loss, configuration-hash gate, lease renewal, executor adapters, Grid-India identical
revisions, stop latency and process exit codes.

No database, no network, no ingestion: in-memory repositories, a SQL-recording fake connection for
:class:`PostgresJobRepository`, mocked HTTP and fake runners.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import httpx
import pytest
from pydantic import SecretStr

from config.settings import PROJECT_ROOT
from src.common.exceptions import ConfigurationError, HttpRequestError
from src.ingestion.models import IngestionResult, RunStatus
from src.scheduler.checkers.grid_india import GridIndiaPspChecker
from src.scheduler.checkers.mosdac import MosdacChecker
from src.scheduler.clock import FakeClock
from src.scheduler.config import SchedulerConfig, config_hash, load_scheduler_config
from src.scheduler.executors import Executor, RUNNERS
from src.scheduler.locks import SCHEDULER_LOCK, AdvisoryLock, InProcessLockFactory, advisory_key
from src.scheduler.models import CheckOutcome, ErrorClass, ExecutionOutcome, JobStatus, WatermarkStatus
from src.scheduler.process import EXIT_CONFIGURATION, EXIT_OK, EXIT_RUNTIME_FAILURE, StopSignal
from src.scheduler.repository import PostgresJobRepository
from src.scheduler.scheduler import Scheduler, SchedulerLockLost, gate_refusal, sync_report
from src.scheduler.worker import Worker
from tests.support import mosdac as mosdac_support
from tests.support.grid_india import FileSpec
from tests.support.scheduler import (
    FakeState,
    InMemoryJobRepository,
    ScriptedChecker,
    detection,
    http,
    new_data,
    schedule,
    settings,
)

IST = ZoneInfo("Asia/Kolkata")
T0 = datetime(2026, 11, 25, 10, 0, tzinfo=IST)
XLS = b"\xd0\xcf\x11\xe0" + b"workbook"


def _result(run_id: int, status: RunStatus = RunStatus.SUCCESS, *, documents: int = 1,
            error: str | None = None) -> IngestionResult:
    return IngestionResult(ingestion_run_id=run_id, dataset_code="X", status=status, records_read=5,
                           records_inserted=5 if documents else 0, documents_processed=documents, error=error)


def _worker(repo, clock, runners: dict, cfg=None, locks=None) -> Worker:
    cfg = cfg or settings()
    return Worker(repository=repo, executor=Executor(cfg, runners=runners), locks=locks or InProcessLockFactory(),
                  clock=clock, settings=cfg, worker_id="w1")


class _NoHttp:
    def close(self) -> None:
        pass


# =========================================================================== MOSDAC solar / wind split
@pytest.fixture
def mosdac_setup(tmp_path):
    locations = tmp_path / "mosdac.toml"
    locations.write_text('[[location]]\ncode = "ESEPL"\nname = "ESEPL"\nlatitude = 17.4042\nlongitude = 77.3753\n'
                         'coordinate_source = "test"\n', encoding="utf-8")
    cfg = settings(mosdac_locations_file=locations)
    solar = schedule("MOSDAC_SOLAR", checker="mosdac", every="3h", lock_group="MOSDAC", params={"kind": "solar"})
    wind = schedule("MOSDAC_WIND", checker="mosdac", every="1h", lock_group="MOSDAC", params={"kind": "wind"})
    mock = mosdac_support.MockMosdac()
    repo, clock, state = InMemoryJobRepository(), FakeClock(T0), FakeState()
    sched = Scheduler(config=SchedulerConfig({s.key: s for s in (solar, wind)}),
                      checkers={"MOSDAC_SOLAR": MosdacChecker(solar), "MOSDAC_WIND": MosdacChecker(wind)},
                      repository=repo, clock=clock, settings=cfg, http_factory=lambda: http(mock, cfg), state=state)
    executed: list[str] = []
    return SimpleNamespace(cfg=cfg, mock=mock, repo=repo, clock=clock, state=state, sched=sched, executed=executed)


def test_mosdac_checker_requires_one_kind_and_reads_the_wind_table_for_wind() -> None:
    with pytest.raises(ValueError, match="kind"):
        MosdacChecker(schedule("MOSDAC_X", checker="mosdac"))
    with pytest.raises(ValueError, match="kind"):
        MosdacChecker(schedule("MOSDAC_X", checker="mosdac", params={"kind": "all"}))
    state = FakeState(mosdac={"P": datetime(2030, 1, 1, tzinfo=IST)})
    assert state.mosdac_latest_series("P", "wind") is None        # solar data never hides a missing wind series


def test_mosdac_wind_outage_does_not_fail_or_hide_solar_and_wind_recovers_independently(mosdac_setup) -> None:
    m = mosdac_setup
    m.mock.set("solar", mosdac_support.solar_records(289))
    m.mock.set("wind", mosdac_support.server_error)

    def runner(target, cfg):
        m.executed.append(target["command"])
        return [(RunStatus.SUCCESS, _result(len(m.executed)), None)], {}

    worker = _worker(m.repo, m.clock, {"mosdac": runner}, cfg=m.cfg)
    reports = {r.schedule_key: r for r in m.sched.tick(force=True)}
    assert reports["MOSDAC_SOLAR"].outcome is CheckOutcome.NEW_DATA and len(reports["MOSDAC_SOLAR"].jobs_created) == 1
    assert reports["MOSDAC_WIND"].outcome is CheckOutcome.SOURCE_ERROR and reports["MOSDAC_WIND"].jobs_created == []
    [solar_job] = m.repo.jobs()
    assert solar_job.target == {"runner": "mosdac", "command": "solar"}     # solar only: wind cannot fail it
    worker.run_once()
    assert m.repo.job(solar_job.job_id).status is JobStatus.SUCCEEDED and m.executed == ["solar"]
    assert m.repo.get_watermark("MOSDAC_SOLAR", solar_job.item_key).status is WatermarkStatus.INGESTED
    assert not [k for k in m.repo.watermarks if k[0] == "MOSDAC_WIND"]

    # the wind endpoint recovers; there is NO new solar series - wind must not wait for one
    m.state.mosdac["ESEPL"] = datetime.fromisoformat(solar_job.version_token)
    m.mock.set("wind", mosdac_support.wind_records(289))
    m.clock.advance(hours=1, minutes=1)
    reports = {r.schedule_key: r for r in m.sched.tick()}
    assert set(reports) == {"MOSDAC_WIND"}                                  # solar not even due (3 h)
    [wind_id] = reports["MOSDAC_WIND"].jobs_created
    worker.run_once()
    assert m.repo.job(wind_id).status is JobStatus.SUCCEEDED and m.executed == ["solar", "wind"]
    assert m.repo.get_watermark("MOSDAC_SOLAR", solar_job.item_key).status is WatermarkStatus.INGESTED


def test_mosdac_failing_wind_job_retries_alone_while_solar_is_ingested(mosdac_setup) -> None:
    m = mosdac_setup
    m.mock.set("solar", mosdac_support.solar_records(289))
    m.mock.set("wind", mosdac_support.wind_records(289))
    wind_attempts = []

    def runner(target, cfg):
        m.executed.append(target["command"])
        if target["command"] == "wind" and not wind_attempts:
            wind_attempts.append(1)
            raise HttpRequestError("wind endpoint: connection reset")
        return [(RunStatus.SUCCESS, _result(len(m.executed)), None)], {}

    worker = _worker(m.repo, m.clock, {"mosdac": runner}, cfg=m.cfg)
    reports = {r.schedule_key: r for r in m.sched.tick(force=True)}
    [solar_id], [wind_id] = reports["MOSDAC_SOLAR"].jobs_created, reports["MOSDAC_WIND"].jobs_created
    worker.run_once()
    worker.run_once()
    solar, wind = m.repo.job(solar_id), m.repo.job(wind_id)
    assert solar.status is JobStatus.SUCCEEDED and wind.status is JobStatus.RETRY_WAIT
    assert m.repo.get_watermark("MOSDAC_SOLAR", solar.item_key).status is WatermarkStatus.INGESTED
    assert m.repo.get_watermark("MOSDAC_WIND", wind.item_key).status is WatermarkStatus.QUEUED
    m.clock.set(wind.next_attempt_at + timedelta(seconds=1))
    worker.run_once()                                                       # retried on its own backoff
    assert m.repo.job(wind_id).status is JobStatus.SUCCEEDED
    assert m.executed.count("solar") == 1 and m.executed.count("wind") == 2
    assert len(m.repo.checks) == 2                                          # no new solar check was needed


# =========================================================================== atomic enqueue + watermark
class _NoSeparateWatermark(InMemoryJobRepository):
    def set_watermark(self, *args, **kwargs):   # the scheduler and worker must not need a second write
        raise AssertionError("watermark written outside the job transaction")


def test_scheduler_and_worker_never_write_the_watermark_separately() -> None:
    s = schedule("ATOMIC")
    repo, clock = _NoSeparateWatermark(), FakeClock(T0)
    sched = Scheduler(config=SchedulerConfig({s.key: s}), checkers={s.key: ScriptedChecker(s, [new_data(detection())])},
                      repository=repo, clock=clock, settings=settings(), http_factory=_NoHttp, state=FakeState())
    [report] = sched.tick(force=True)
    [job_id] = report.jobs_created
    mark = repo.get_watermark("ATOMIC", "2026-11-24")
    assert (mark.status, mark.last_job_id) == (WatermarkStatus.QUEUED, job_id)   # exists as soon as the job does
    _worker(repo, clock, {"fake": lambda t, c: ([(RunStatus.SUCCESS, _result(1), None)], {})}).run_once()
    assert repo.get_watermark("ATOMIC", "2026-11-24").status is WatermarkStatus.INGESTED


def test_late_finish_of_an_old_version_never_relabels_a_newer_version() -> None:
    s = schedule("VERS")
    repo, clock = InMemoryJobRepository(), FakeClock(T0)
    old = repo.enqueue(s, detection(version="v1"), clock.now())
    claimed = repo.claim("w", clock.now(), 600)
    assert claimed.job_id == old
    new = repo.enqueue(s, detection(version="v2"), clock.now())          # a revision arrives meanwhile
    assert repo.finish(claimed, "w", clock.now(), JobStatus.SUCCEEDED, ExecutionOutcome(JobStatus.SUCCEEDED), None,
                       watermark=WatermarkStatus.INGESTED)
    mark = repo.get_watermark("VERS", "2026-11-24")
    assert (mark.version_token, mark.status, mark.last_job_id) == ("v2", WatermarkStatus.QUEUED, new)


# ---- PostgresJobRepository SQL, recorded per transaction (no database)
class _Result:
    def __init__(self, rows=None) -> None:
        self.rows = list(rows or [])

    def scalar_one_or_none(self):
        return self.rows[0][0] if self.rows else None

    def scalar_one(self):
        return self.rows[0][0]

    def scalar(self):
        return self.rows[0][0] if self.rows else None

    def first(self):
        return self.rows[0] if self.rows else None

    def all(self):
        return self.rows

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self.rows)


class RecordingDatabase:
    """``connect()`` = one transaction; every statement is recorded with its transaction number."""

    def __init__(self, responses: dict[str, list] | None = None) -> None:
        self.responses = responses or {}
        self.statements: list[tuple[int, str, dict]] = []
        self.transactions = 0

    @contextmanager
    def connect(self):
        self.transactions += 1
        txn = self.transactions
        database = self

        class _Conn:
            def execute(self, clause, params=None):
                sql = " ".join(str(clause).split())
                database.statements.append((txn, sql, dict(params or {})))
                for marker, rows in database.responses.items():
                    if marker in sql:
                        return _Result(rows)
                return _Result()

        yield _Conn()

    def sql(self, marker: str) -> list[tuple[int, str, dict]]:
        return [s for s in self.statements if marker in s[1]]


def test_postgres_enqueue_inserts_the_job_and_the_queued_watermark_in_one_transaction() -> None:
    db = RecordingDatabase({"INSERT INTO pi_jobs.job ": [(41,)]})
    assert PostgresJobRepository(db).enqueue(schedule("PG"), detection(version="v9"), T0, check_id=7) == 41
    assert db.transactions == 1
    [(t1, _sql, _p)] = db.sql("INSERT INTO pi_jobs.job ")
    [(t2, mark_sql, params)] = db.sql("INSERT INTO pi_jobs.source_watermark")
    assert t1 == t2 == 1 and params["s"] == "QUEUED" and params["j"] == 41 and params["v"] == "v9"
    assert "WHERE pi_jobs.source_watermark.version_token" not in mark_sql   # a new detection always takes over

    duplicate = RecordingDatabase()                                       # ON CONFLICT DO NOTHING -> no row
    assert PostgresJobRepository(duplicate).enqueue(schedule("PG"), detection(), T0) is None
    assert duplicate.sql("source_watermark") == []                         # and no watermark change


def test_postgres_finish_updates_the_watermark_in_the_same_transaction_for_the_same_version_only() -> None:
    from src.scheduler.models import Job

    job = Job(5, "PG", "PG|i|v1", "i", "v1", {}, "G", JobStatus.RUNNING, 1, 5, 120.0, 1800.0, T0, T0, "w", T0)
    db = RecordingDatabase({"UPDATE pi_jobs.job SET status = :status": [(5,)]})
    assert PostgresJobRepository(db).finish(job, "w", T0, JobStatus.SUCCEEDED, ExecutionOutcome(JobStatus.SUCCEEDED),
                                            None, watermark=WatermarkStatus.INGESTED)
    assert db.transactions == 1 and {t for t, _s, _p in db.statements} == {1}
    [(_t, mark_sql, params)] = db.sql("INSERT INTO pi_jobs.source_watermark")
    assert params["s"] == "INGESTED" and params["v"] == "v1"
    assert mark_sql.endswith("WHERE pi_jobs.source_watermark.version_token = EXCLUDED.version_token")

    lost = RecordingDatabase()                                            # lease lost: UPDATE matched no row
    assert not PostgresJobRepository(lost).finish(job, "w", T0, JobStatus.SUCCEEDED,
                                                  ExecutionOutcome(JobStatus.SUCCEEDED), None,
                                                  watermark=WatermarkStatus.INGESTED)
    assert lost.sql("source_watermark") == [] and lost.sql("INSERT INTO pi_jobs.job_attempt")


def test_postgres_sync_never_writes_enablement() -> None:
    db = RecordingDatabase({"SELECT schedule_key, config FROM": [("OLD_KEY", {"x": 1})]})
    changes = PostgresJobRepository(db).sync_schedules([schedule("NEW_KEY", enabled=True)], T0)
    assert changes["NEW_KEY"].startswith("inserted") and "left unchanged" in changes["OLD_KEY"]
    writes = [s for _t, s, _p in db.statements if s.startswith(("INSERT", "UPDATE"))]
    assert writes and all("enabled" not in s for s in writes)


# =========================================================================== version-aware requeue
def _failed_job(repo, clock, s, version):
    job_id = repo.enqueue(s, detection(version=version), clock.now())
    job = repo.claim("w", clock.now(), 600)
    repo.finish(job, "w", clock.now(), JobStatus.FAILED,
                ExecutionOutcome(JobStatus.FAILED, error="bad layout", error_class=ErrorClass.PERMANENT), None,
                watermark=WatermarkStatus.QUARANTINED)
    return job_id


def test_requeue_is_refused_when_a_newer_version_exists_unless_forced() -> None:
    s, repo, clock = schedule("RQ"), InMemoryJobRepository(), FakeClock(T0)
    old = _failed_job(repo, clock, s, "v1")
    newer = repo.enqueue(s, detection(version="v2"), clock.now())
    job = repo.claim("w", clock.now(), 600)
    repo.finish(job, "w", clock.now(), JobStatus.SUCCEEDED, ExecutionOutcome(JobStatus.SUCCEEDED), None,
                watermark=WatermarkStatus.INGESTED)
    done, message = repo.requeue(old, clock.now())
    assert not done and "v2" in message and "--force" in message
    assert repo.job(old).status is JobStatus.FAILED
    done, _ = repo.requeue(old, clock.now(), force=True)
    assert done and repo.job(old).status is JobStatus.PENDING
    mark = repo.get_watermark("RQ", "2026-11-24")
    assert (mark.version_token, mark.status, mark.last_job_id) == ("v2", WatermarkStatus.INGESTED, newer)


def test_requeue_of_the_current_version_marks_it_queued_again() -> None:
    s, repo, clock = schedule("RQ2"), InMemoryJobRepository(), FakeClock(T0)
    job_id = _failed_job(repo, clock, s, "v1")
    assert repo.get_watermark("RQ2", "2026-11-24").status is WatermarkStatus.QUARANTINED
    assert repo.requeue(job_id, clock.now()) == (True, "requeued")
    assert repo.get_watermark("RQ2", "2026-11-24").status is WatermarkStatus.QUEUED
    assert repo.requeue(job_id, clock.now())[0] is False                     # PENDING cannot be requeued
    assert repo.requeue(999, clock.now()) == (False, "no such job")


def test_postgres_requeue_checks_the_watermark_version_in_the_same_transaction() -> None:
    job_row = {"job_id": 3, "schedule_key": "PG", "item_key": "i", "version_token": "v1", "status": "FAILED",
               "dedupe_key": "PG|i|v1"}
    newer = {"FROM pi_jobs.job WHERE job_id = :id FOR UPDATE": [job_row],
             "FROM pi_jobs.source_watermark WHERE": [("v2", "INGESTED")]}
    db = RecordingDatabase(newer)
    done, message = PostgresJobRepository(db).requeue(3, T0)
    assert not done and "v2" in message and db.sql("UPDATE pi_jobs.job SET status = 'PENDING'") == []
    forced = RecordingDatabase(newer)
    assert PostgresJobRepository(forced).requeue(3, T0, force=True)[0]
    assert forced.sql("UPDATE pi_jobs.job SET status = 'PENDING'") and forced.sql("INSERT INTO pi_jobs.source_watermark") == []
    same = RecordingDatabase({**newer, "FROM pi_jobs.source_watermark WHERE": [("v1", "QUARANTINED")]})
    assert PostgresJobRepository(same).requeue(3, T0)[0]
    [(_t, _s, params)] = same.sql("INSERT INTO pi_jobs.source_watermark")
    assert params["s"] == "QUEUED" and same.transactions == 1


# =========================================================================== scheduler lock ownership
def test_scheduler_stops_when_its_lock_is_lost() -> None:
    s = schedule("LOCKED")
    locks = InProcessLockFactory()
    sched = Scheduler(config=SchedulerConfig({s.key: s}), checkers={s.key: ScriptedChecker(s)},
                      repository=InMemoryJobRepository(), clock=FakeClock(T0),
                      settings=settings(scheduler_tick_seconds=1), http_factory=_NoHttp, state=FakeState(),
                      locks=locks)
    threading.Timer(0.3, locks.lose, args=(SCHEDULER_LOCK,)).start()
    started = time.monotonic()
    with pytest.raises(SchedulerLockLost):
        sched.run(StopSignal())
    assert time.monotonic() - started < 5
    assert locks.held == set()


def test_advisory_lock_is_held_asks_its_own_session_and_treats_errors_as_lost() -> None:
    calls = []

    class _Conn:
        def __init__(self, answer):
            self.answer = answer

        def execute(self, clause, params=None):
            calls.append(params)
            if isinstance(self.answer, Exception):
                raise self.answer
            return _Result([(self.answer,)])

        def commit(self):
            pass

        def close(self):
            pass

    key = advisory_key(SCHEDULER_LOCK)
    assert AdvisoryLock("scheduler", _Conn(True), key).is_held() is True
    unsigned = key & 0xFFFFFFFFFFFFFFFF
    assert calls[-1] == {"hi": unsigned >> 32, "lo": unsigned & 0xFFFFFFFF}
    assert ((calls[-1]["hi"] << 32) | calls[-1]["lo"]) == unsigned
    assert AdvisoryLock("scheduler", _Conn(False), key).is_held() is False
    assert AdvisoryLock("scheduler", _Conn(OSError("server closed the connection")), key).is_held() is False
    released = AdvisoryLock("scheduler", _Conn(True), key)
    released.release()
    assert released.is_held() is False


# =========================================================================== configuration hash gate
def test_unsynced_configuration_is_refused_with_one_warning_until_synced(caplog) -> None:
    s = schedule("HASHED")
    repo = InMemoryJobRepository()
    repo.db = {"HASHED": (True, "0" * 64)}                               # enabled, but a different configuration
    sched = Scheduler(config=SchedulerConfig({s.key: s}), checkers={s.key: ScriptedChecker(s)}, repository=repo,
                      clock=FakeClock(T0), settings=settings(), http_factory=_NoHttp, state=FakeState())
    with caplog.at_level(logging.WARNING, logger="src.scheduler.scheduler"):
        assert sched.tick(force=True) == [] and sched.tick(force=True) == []
    refusals = [r for r in caplog.records if "configuration differs" in r.getMessage()]
    assert len(refusals) == 1                                            # logged once, not every tick
    assert repo.sync_schedules([s], T0) == {"HASHED": "updated (database enablement unchanged)"}
    assert repo.db["HASHED"] == (True, s.hash)
    assert [r.schedule_key for r in sched.tick(force=True)] == ["HASHED"]


def test_gate_reasons_and_sync_report() -> None:
    s = schedule("GATE")
    cfg = settings()
    assert "sync-config" in gate_refusal(s, {}, cfg)
    assert "not enabled" in gate_refusal(s, {"GATE": (False, s.hash)}, cfg)
    assert gate_refusal(s, {"GATE": (True, s.hash)}, cfg) is None
    changed = schedule("GATE", every="45m")
    assert changed.hash != s.hash and "differs" in gate_refusal(changed, {"GATE": (True, s.hash)}, cfg)
    report = sync_report(SchedulerConfig({"GATE": changed}), {"GATE": (True, s.hash)}, cfg)
    assert report[0]["in_sync"] is False and report[0]["would_run"] is False


def test_new_synced_schedules_start_disabled_in_the_database() -> None:
    s = schedule("FRESH")
    repo = InMemoryJobRepository()
    assert repo.sync_schedules([s], T0)["FRESH"].startswith("inserted")
    assert repo.db["FRESH"] == (False, s.hash)
    assert repo.set_enabled("FRESH", True, T0) and repo.db["FRESH"][0] is True
    assert repo.sync_schedules([s], T0)["FRESH"] == "unchanged" and repo.db["FRESH"][0] is True


def test_hash_survives_the_jsonb_round_trip_for_every_shipped_schedule() -> None:
    config = load_scheduler_config(PROJECT_ROOT / "config" / "scheduler.toml")
    for s in config.schedules.values():
        stored = json.loads(json.dumps(s.as_row(), sort_keys=True, default=str))
        assert config_hash(stored) == s.hash, s.key


def _om_forecast(**params):
    return schedule("OPEN_METEO_FORECAST", checker="open_meteo_forecast", source="OPEN_METEO",
                    params={"slots": ["01:30", "07:30", "13:30", "19:30"], "forecast_days": 7,
                            "locations": [f"L{i}" for i in range(8)], **params})


def test_open_meteo_free_plan_needs_no_api_key_and_is_the_default() -> None:
    cfg = settings(open_meteo_api_key=None)
    assert cfg.open_meteo_plan == "free"
    assert gate_refusal(_om_forecast(), None, cfg) is None                 # free endpoint, no key: allowed
    assert settings(open_meteo_plan=" FREE ").open_meteo_plan == "free"


def test_open_meteo_never_switches_endpoint_silently() -> None:
    reason = gate_refusal(_om_forecast(), None, settings(open_meteo_api_key=SecretStr("k")))
    assert reason and "OPEN_METEO_PLAN=free" in reason and "customer-" in reason   # key on free plan: refused
    assert "requires OPEN_METEO_API_KEY" in gate_refusal(_om_forecast(), None,
                                                         settings(open_meteo_plan="commercial"))
    assert gate_refusal(_om_forecast(), None, settings(open_meteo_plan="commercial",
                                                       open_meteo_api_key=SecretStr("k"))) is None
    with pytest.raises(ValueError):
        settings(open_meteo_plan="paid")
    from src.pipelines.open_meteo.api import FORECAST_URL, endpoint
    assert endpoint(FORECAST_URL, None) == "https://api.open-meteo.com/v1/forecast"   # what the free plan uses


def test_open_meteo_daily_call_budget_is_enforced_on_the_free_plan() -> None:
    from src.scheduler.checkers.open_meteo import call_weight, estimated_daily_calls

    assert call_weight(8, 7) == 1 and call_weight(8, 16) == 2 and call_weight(11, 7) == 2
    s = _om_forecast()
    assert estimated_daily_calls(s, 8) == 4 * 8 * 5                        # slots x locations x attempts
    historical = schedule("OPEN_METEO_HISTORICAL", checker="open_meteo_historical", source="OPEN_METEO",
                          catchup_enabled=False, params={"locations": ["A", "B"]})
    assert estimated_daily_calls(historical, 2) == 1 * 2 * 5
    assert gate_refusal(s, None, settings(open_meteo_daily_call_limit=160)) is None
    reason = gate_refusal(s, None, settings(open_meteo_daily_call_limit=159))
    assert reason and "160 calls/day" in reason
    shared = gate_refusal(s, None, settings(open_meteo_daily_call_limit=165), enabled=[s, historical])
    assert shared and "170 calls/day" in shared                            # the budget is shared


def test_shipped_open_meteo_schedules_stay_disabled_and_within_the_free_limit() -> None:
    from src.scheduler.checkers.open_meteo import estimated_daily_calls

    shipped = load_scheduler_config(PROJECT_ROOT / "config" / "scheduler.toml").schedules
    om = [shipped[k] for k in ("OPEN_METEO_FORECAST", "OPEN_METEO_HISTORICAL")]
    assert all(not s.enabled for s in om)
    assert sum(estimated_daily_calls(s, 50) for s in om) < 10_000          # even with 50 locations


def test_validate_config_reports_the_open_meteo_plan_without_the_key(capsys) -> None:
    from scripts.pi_jobs import main

    assert main(["validate-config"], settings=settings(open_meteo_api_key=SecretStr("secret-value"))) == 0
    out = capsys.readouterr().out
    assert "OPEN_METEO_PLAN=free" in out and "NON-COMMERCIAL" in out and "CC BY 4.0" in out
    assert "API key set: yes" in out and "secret-value" not in out


# =========================================================================== worker lease renewal
def test_long_running_job_keeps_renewing_its_lease() -> None:
    renewals = []

    class _Counting(InMemoryJobRepository):
        def renew_lease(self, job_id, worker_id, now, lease_seconds):
            renewals.append(now)
            return super().renew_lease(job_id, worker_id, now, lease_seconds)

    repo, clock = _Counting(), FakeClock(T0)
    cfg = settings().model_copy(update={"worker_heartbeat_seconds": 0.05})
    repo.enqueue(schedule("LONG"), detection(), clock.now())

    def slow(target, c):
        time.sleep(0.5)
        return [(RunStatus.SUCCESS, _result(1), None)], {}

    job = _worker(repo, clock, {"fake": slow}, cfg=cfg).run_once()
    assert len(renewals) >= 3 and repo.job(job.job_id).status is JobStatus.SUCCEEDED


# =========================================================================== executor adapters (fake runners)
def _runs(status=RunStatus.SUCCESS, run_id=1, documents=1):
    return SimpleNamespace(status=status, result=_result(run_id, status, documents=documents), error=None)


@pytest.mark.parametrize("target,patch,summary,expected_ids", [
    ({"runner": "cea_daily", "commands": ["coal", "dgr10"], "start_date": "2026-11-23", "end_date": "2026-11-23"},
     "src.pipelines.cea.runner.run_cea_daily", lambda: SimpleNamespace(**vars(_runs()), missing=[]), [1, 1]),
    ({"runner": "cea_psp", "kind": "psp-energy", "month": "2026-10"},
     "src.pipelines.cea.runner.run_cea_psp", lambda: SimpleNamespace(**vars(_runs(run_id=2)), missing=[]), [2]),
    ({"runner": "iex", "kind": "rtm", "start_date": "2026-11-24", "end_date": "2026-11-24"},
     "src.pipelines.iex.runner.run_iex",
     lambda: SimpleNamespace(**vars(_runs(run_id=3)), snapshot=_result(8), snapshot_skipped=None), [3, 8]),
    ({"runner": "grid_india_psp", "start_date": "2026-11-24", "end_date": "2026-11-24", "sections": ["supply"]},
     "src.pipelines.grid_india.runner.run_grid_india_psp",
     lambda: SimpleNamespace(sections={"supply": _runs(run_id=4)}, fetch_error=None, missing=[], workbooks=["w"]),
     [4]),
    ({"runner": "npp", "kind": "demand", "start_date": "2026-11-24", "end_date": "2026-11-24"},
     "src.pipelines.npp.runner.run_npp", lambda: SimpleNamespace(outcomes=[_runs(run_id=5)]), [5]),
    ({"runner": "open_meteo", "kind": "forecast", "forecast_days": 7},
     "src.pipelines.open_meteo.runner.run_open_meteo", lambda: SimpleNamespace(outcomes=[_runs(run_id=6)]), [6]),
    ({"runner": "mosdac", "command": "wind"},
     "src.pipelines.mosdac.runner.run_mosdac",
     lambda: SimpleNamespace(runs={"P": _result(7)}, location_errors={}), [7]),
])
def test_executor_adapters_call_the_existing_runner_interfaces(monkeypatch, target, patch, summary,
                                                                expected_ids) -> None:
    calls = []

    def fake(*args, **kwargs):
        calls.append((args, kwargs))
        return summary()

    module, name = patch.rsplit(".", 1)
    monkeypatch.setattr(f"{module}.{name}", fake)
    outcome = Executor(settings()).execute(target)
    assert outcome.status is JobStatus.SUCCEEDED and outcome.ingestion_run_ids == expected_ids
    assert calls and "settings" in calls[0][1]
    if target["runner"] == "mosdac":
        assert calls[0][0] == ("wind",)                                    # the kind, never "all"


def test_mosdac_adapter_reports_location_errors_as_failures(monkeypatch) -> None:
    monkeypatch.setattr("src.pipelines.mosdac.runner.run_mosdac",
                        lambda *a, **k: SimpleNamespace(runs={}, location_errors={"P": "moved"}))
    outcome = Executor(settings()).execute({"runner": "mosdac", "command": "solar"})
    assert outcome.status is JobStatus.FAILED and "MosdacLocationError" in outcome.error


# ---- IEX: price (iex_market_price) and snapshot (iex_market_snapshot) loads of one run_iex call
IEX_TARGET = {"runner": "iex", "kind": "dam", "start_date": "2026-11-24", "end_date": "2026-11-24"}
TIMEOUT = "HttpRequestError: request timed out"


def _iex(monkeypatch, price_status, snapshot_status=None, *, price_error=None, snapshot_error=None,
         skipped=None):
    price = _result(3, price_status, error=price_error)
    snapshot = None if snapshot_status is None else _result(8, snapshot_status, error=snapshot_error)
    summary = SimpleNamespace(status=price_status, result=price, error=price_error, snapshot=snapshot,
                              snapshot_skipped=skipped)
    monkeypatch.setattr("src.pipelines.iex.runner.run_iex", lambda *a, **k: summary)
    return Executor(settings()).execute(IEX_TARGET)


def test_iex_both_loads_succeed_and_both_run_ids_are_recorded(monkeypatch) -> None:
    outcome = _iex(monkeypatch, RunStatus.SUCCESS, RunStatus.SUCCESS)
    assert outcome.status is JobStatus.SUCCEEDED and outcome.ingestion_run_ids == [3, 8]
    components = outcome.detail["components"]
    assert components["price"]["ingestion_run_id"] == 3 and components["price"]["status"] == "SUCCESS"
    assert components["snapshot"]["ingestion_run_id"] == 8 and components["snapshot"]["status"] == "SUCCESS"
    assert outcome.records_inserted == 10                                    # both loads counted


def test_iex_price_succeeds_but_snapshot_fails(monkeypatch) -> None:
    outcome = _iex(monkeypatch, RunStatus.SUCCESS, RunStatus.FAILED, snapshot_error=TIMEOUT)
    assert outcome.status is JobStatus.FAILED and outcome.error_class is ErrorClass.TRANSIENT   # retried
    assert outcome.ingestion_run_ids == [3, 8]
    assert outcome.detail["components"]["snapshot"]["error"] == TIMEOUT
    assert outcome.detail["components"]["price"]["status"] == "SUCCESS"


def test_iex_snapshot_succeeds_but_price_fails(monkeypatch) -> None:
    outcome = _iex(monkeypatch, RunStatus.FAILED, RunStatus.SUCCESS, price_error=TIMEOUT)
    assert outcome.status is JobStatus.FAILED and outcome.error_class is ErrorClass.TRANSIENT
    assert outcome.ingestion_run_ids == [3, 8]
    assert outcome.detail["components"]["price"]["error"] == TIMEOUT
    assert outcome.detail["components"]["snapshot"]["status"] == "SUCCESS"


def test_iex_both_loads_fail(monkeypatch) -> None:
    outcome = _iex(monkeypatch, RunStatus.FAILED, RunStatus.FAILED, price_error="ParseError: bad layout",
                   snapshot_error=TIMEOUT)
    assert outcome.status is JobStatus.FAILED and outcome.ingestion_run_ids == [3, 8]
    assert "ParseError" in outcome.error and "timed out" in outcome.error
    assert outcome.error_class is ErrorClass.TRANSIENT          # the most retry-worthy class wins


def test_iex_rejected_snapshot_records_make_the_job_partial(monkeypatch) -> None:
    outcome = _iex(monkeypatch, RunStatus.SUCCESS, RunStatus.PARTIAL)
    assert outcome.status is JobStatus.PARTIAL and outcome.ingestion_run_ids == [3, 8]


def test_iex_snapshot_not_loaded_after_a_successful_price_run_is_a_failure(monkeypatch) -> None:
    outcome = _iex(monkeypatch, RunStatus.SUCCESS, skipped="pi.iex_market_snapshot does not exist "
                                                           "(apply 0013_iex_market_snapshot)")
    assert outcome.status is JobStatus.FAILED and outcome.error_class is ErrorClass.PERMANENT
    assert outcome.ingestion_run_ids == [3] and "0013_iex_market_snapshot" in outcome.error
    assert outcome.detail["components"]["snapshot"]["status"] == "FAILED"


def test_iex_failed_price_run_without_snapshot_is_decided_by_the_price_error(monkeypatch) -> None:
    outcome = _iex(monkeypatch, RunStatus.FAILED, price_error=TIMEOUT,
                   skipped="no snapshot document was stored by the price run")
    assert outcome.status is JobStatus.FAILED and outcome.error_class is ErrorClass.TRANSIENT
    assert outcome.error == TIMEOUT and outcome.ingestion_run_ids == [3]
    assert outcome.detail["components"]["snapshot"]["status"] == "NOT_RUN"


def test_iex_job_with_a_failed_snapshot_is_not_marked_ingested() -> None:
    from src.scheduler.executors import iex_runs

    repo, clock = InMemoryJobRepository(), FakeClock(T0)
    repo.enqueue(schedule("IEX_DAM", lock_group="IEX"), detection(runner="iex"), clock.now())
    summary = SimpleNamespace(status=RunStatus.SUCCESS, result=_result(3), error=None,
                              snapshot=_result(8, RunStatus.FAILED, error=TIMEOUT), snapshot_skipped=None)
    job = _worker(repo, clock, {"iex": lambda t, c: iex_runs(summary)}).run_once()
    assert repo.job(job.job_id).status is JobStatus.RETRY_WAIT
    assert repo.get_watermark("IEX_DAM", job.item_key).status is WatermarkStatus.QUEUED   # not INGESTED


# =========================================================================== Grid-India identical revision
def test_grid_india_byte_identical_workbook_under_a_new_name_is_processed_once() -> None:
    d23, d24 = date(2026, 11, 23), date(2026, 11, 24)
    specs = [FileSpec(d23, XLS, suffix="723"), FileSpec(d23, XLS, datetime(2026, 11, 24, 16, 38), suffix="723_611"),
             FileSpec(d24, XLS, suffix="597")]
    name = lambda d, s: f"{d:%d.%m.%y}_NLDC_PSP_{s}.xls"   # noqa: E731
    loaded = {(d23, name(d23, "723")), (d24, name(d24, "597"))}
    state = FakeState(names={"GRID_INDIA_PSP_DAILY": set(loaded), "GRID_INDIA_PSP_STATES": set(loaded)})

    def listing(request: httpx.Request) -> httpx.Response:
        month = int(json.loads(request.content or b"{}").get("_month", 0))
        return httpx.Response(200, json={"flagType": 0, "retData": [s.entry(i) for i, s in enumerate(specs, 1)
                                                                      if s.report_date.month == month]})

    s = schedule("GRID_INDIA_PSP", checker="grid_india_psp", catchup_max_days=2,
                 params={"sections": ["supply", "states"], "late_after_hours": 16})
    repo, clock = InMemoryJobRepository(), FakeClock(datetime(2026, 11, 25, 11, 0, tzinfo=IST))
    sched = Scheduler(config=SchedulerConfig({s.key: s}), checkers={s.key: GridIndiaPspChecker(s)}, repository=repo,
                      clock=clock, settings=settings(), http_factory=lambda: http(listing), state=state)
    [report] = sched.tick(force=True)
    [job_id] = report.jobs_created
    assert repo.job(job_id).item_key == "2026-11-23|23.11.26_NLDC_PSP_723_611.xls"

    # identical bytes: the pipeline skips the document by SHA-256, so its name is never registered
    skipped = lambda t, c: ([(RunStatus.SUCCESS, _result(9, documents=0), None)], {"missing": [], "workbooks": ["w"]})  # noqa: E731
    _worker(repo, clock, {"grid_india_psp": skipped}).run_once()
    assert repo.job(job_id).status is JobStatus.SUCCEEDED
    clock.advance(hours=1)
    [again] = sched.tick(force=True)
    assert again.jobs_created == [] and "already ingested" in again.skipped["2026-11-23|23.11.26_NLDC_PSP_723_611.xls"]
    assert len(repo.jobs()) == 1


# =========================================================================== stop latency
def test_stop_file_is_noticed_within_about_one_slice() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        stop_file = __import__("pathlib").Path(tmp) / "STOP"
        stop = StopSignal(stop_file)
        threading.Timer(0.2, stop_file.write_text, args=("stop",)).start()
        started = time.monotonic()
        assert stop.wait(30) is True
        assert time.monotonic() - started < 2.5 and "stop file" in stop.reason


@pytest.mark.skipif(sys.platform != "win32", reason="Windows console control events")
def test_windows_ctrl_break_stops_a_long_wait_within_seconds(tmp_path) -> None:
    script = tmp_path / "child.py"
    script.write_text(textwrap.dedent("""
        import sys, time
        from src.scheduler.process import StopSignal, install_signal_handlers
        stop = StopSignal()
        install_signal_handlers(stop)
        print("ready", flush=True)
        started = time.monotonic()
        stopped = stop.wait(60)
        print(f"stopped={stopped} after={time.monotonic() - started:.2f} reason={stop.reason}", flush=True)
        sys.exit(0 if stopped else 1)
    """), encoding="utf-8")
    env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}
    child = subprocess.Popen([sys.executable, str(script)], cwd=PROJECT_ROOT, env=env, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True,
                             creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
    try:
        assert child.stdout.readline().strip() == "ready"
        time.sleep(0.3)
        sent = time.monotonic()
        os.kill(child.pid, signal.CTRL_BREAK_EVENT)
        output, _ = child.communicate(timeout=15)
    finally:
        if child.poll() is None:
            child.kill()
    assert child.returncode == 0, output
    assert "stopped=True" in output and "SIGBREAK" in output
    assert time.monotonic() - sent < 5


# =========================================================================== CLI commands and exit codes
class _AdminRepo(InMemoryJobRepository):
    def status_summary(self):
        return {"jobs": {}, "schedules": [], "processes": self.processes}

    def list_jobs(self, *, status=None, schedule_key=None, limit=50):
        return [{"job_id": r.job.job_id, "status": r.job.status.value} for r in self.rows.values()][:limit]


class _FakeRuntime:
    def __init__(self, repo=None, *, schema=True) -> None:
        self.repository = repo or _AdminRepo()
        self.repository.processes = []
        self.schema = schema
        self.closed = False
        self.database = SimpleNamespace()

    def require_schema(self):
        if not self.schema:
            raise ConfigurationError("schema 'pi_jobs' not found: migration 0011_pi_jobs has not been applied")

    def preview_sync(self, schedules, now):
        copy = _AdminRepo()
        copy.db = None if self.repository.db is None else dict(self.repository.db)
        return copy.sync_schedules(schedules, now)

    def close(self):
        self.closed = True


@pytest.fixture
def jobs_cli(monkeypatch):
    import scripts.pi_jobs as cli

    runtime = _FakeRuntime()
    monkeypatch.setattr(cli, "open_runtime", lambda s: runtime)
    return cli, runtime


def test_pi_jobs_sync_status_enable_and_requeue(jobs_cli, capsys) -> None:
    cli, runtime = jobs_cli
    cfg = settings(scheduler_global_enable=False)
    assert cli.main(["sync-config", "--dry-run"], settings=cfg) == EXIT_OK
    assert runtime.repository.db is None                                  # dry run changed nothing
    assert cli.main(["sync-config"], settings=cfg) == EXIT_OK
    assert len(runtime.repository.db) == 13 and not any(e for e, _h in runtime.repository.db.values())
    capsys.readouterr()
    assert cli.main(["status"], settings=cfg) == EXIT_OK
    status = json.loads(capsys.readouterr().out)
    assert all(r["in_sync"] and r["db_enabled"] is False and not r["would_run"] for r in status["configuration"])
    assert cli.main(["enable", "IEX_RTM"], settings=cfg) == EXIT_OK
    assert runtime.repository.db["IEX_RTM"][0] is True
    out = capsys.readouterr().out
    assert '"would_run": false' in out                                     # still off in the file and globally
    assert cli.main(["disable", "IEX_RTM"], settings=cfg) == EXIT_OK and runtime.repository.db["IEX_RTM"][0] is False
    assert cli.main(["enable", "NOPE"], settings=cfg) == EXIT_CONFIGURATION
    assert cli.main(["requeue", "99"], settings=cfg) == 1
    assert "no such job" in capsys.readouterr().out


def test_pi_jobs_status_warns_about_unsynced_configuration(jobs_cli, capsys) -> None:
    cli, runtime = jobs_cli
    runtime.repository.db = {"IEX_RTM": (False, "0" * 64)}
    assert cli.main(["status"], settings=settings()) == EXIT_OK
    captured = capsys.readouterr()
    assert "WARNING" in captured.err and "IEX_RTM" in captured.err
    report = {r["schedule"]: r for r in json.loads(captured.out)["configuration"]}
    assert report["IEX_RTM"]["in_sync"] is False and report["CEA_COAL_DAILY"]["db_enabled"] is None


def test_pi_jobs_health_and_permission_denied(jobs_cli, capsys, monkeypatch) -> None:
    from datetime import timezone

    from sqlalchemy.exc import ProgrammingError

    cli, runtime = jobs_cli
    now = datetime.now(timezone.utc)
    runtime.repository.processes = [{"process_kind": "scheduler", "last_seen_at": now},
                                    {"process_kind": "worker", "last_seen_at": now - timedelta(hours=1)}]
    assert cli.main(["health"], settings=settings()) == 1                   # stale worker
    runtime.repository.processes[1]["last_seen_at"] = now
    assert cli.main(["health"], settings=settings()) == EXIT_OK

    class _Denied(Exception):
        sqlstate = "42501"

    def denied(*a, **k):
        raise ProgrammingError("UPDATE pi_jobs.schedule SET enabled", {}, _Denied("permission denied for table"))

    runtime.repository.db = {}
    monkeypatch.setattr(runtime.repository, "set_enabled", denied)
    assert cli.main(["enable", "IEX_RTM"], settings=settings(db_user="pi_app")) == 1
    err = capsys.readouterr().err
    assert "permission denied" in err and "admin-only" in err


def test_pi_jobs_missing_migration_is_a_configuration_error(monkeypatch) -> None:
    import scripts.pi_jobs as cli

    monkeypatch.setattr(cli, "open_runtime", lambda s: _FakeRuntime(schema=False))
    assert cli.main(["status"], settings=settings()) == EXIT_CONFIGURATION


@pytest.fixture
def log_names(monkeypatch):
    names = []

    def fake_logging(cfg, name):
        names.append(name)
        return cfg

    import scripts.pi_scheduler as sched_cli
    import scripts.pi_worker as worker_cli

    monkeypatch.setattr(sched_cli, "configure_process_logging", fake_logging)
    monkeypatch.setattr(worker_cli, "configure_process_logging", fake_logging)
    monkeypatch.setattr(sched_cli, "install_signal_handlers", lambda stop: None)
    monkeypatch.setattr(worker_cli, "install_signal_handlers", lambda stop: None)
    return names


@pytest.mark.parametrize("error,expected", [
    (None, EXIT_OK),
    (SchedulerLockLost("lost"), EXIT_RUNTIME_FAILURE),
    (RuntimeError("unexpected"), EXIT_RUNTIME_FAILURE),
    (ConfigurationError("bad"), EXIT_CONFIGURATION),
])
def test_pi_scheduler_run_exit_codes(monkeypatch, log_names, error, expected) -> None:
    import scripts.pi_scheduler as cli

    runtime = _FakeRuntime()

    class _Sched:
        def run(self, stop):
            if error:
                raise error

    monkeypatch.setattr(cli, "open_runtime", lambda s: runtime)
    monkeypatch.setattr(cli, "build_scheduler", lambda r: _Sched())
    assert cli.main(["run"], settings=settings()) == expected
    assert runtime.closed and log_names == ["pi-scheduler.log"]


def test_pi_scheduler_run_without_migration_exits_2(monkeypatch, log_names) -> None:
    import scripts.pi_scheduler as cli

    monkeypatch.setattr(cli, "open_runtime", lambda s: _FakeRuntime(schema=False))
    assert cli.main(["run"], settings=settings()) == EXIT_CONFIGURATION


def test_pi_scheduler_check_uses_its_own_log_and_writes_nothing(monkeypatch, log_names, capsys) -> None:
    import scripts.pi_scheduler as cli

    s = schedule("IEX_RTM")
    checker = ScriptedChecker(s, [new_data(detection())])
    runtime = _FakeRuntime()
    monkeypatch.setattr(cli, "load_config", lambda cfg: (SchedulerConfig({s.key: s}), {s.key: checker}))
    monkeypatch.setattr(cli, "open_runtime", lambda cfg: runtime)
    assert cli.main(["check", "--schedule", "IEX_RTM"], settings=settings()) == EXIT_OK
    assert log_names == ["pi-scheduler-check.log"]
    out = json.loads(capsys.readouterr().out)
    assert out["outcome"] == "NEW_DATA" and out["detections"][0]["item"] == "2026-11-24"
    assert runtime.repository.rows == {} and runtime.repository.checks == []   # nothing written
    assert cli.main(["check", "--schedule", "NOPE"], settings=settings()) == EXIT_CONFIGURATION


def test_pi_worker_once_run_and_exit_codes(monkeypatch, log_names, capsys) -> None:
    import scripts.pi_worker as cli

    runtime = _FakeRuntime()
    ran = []

    class _Worker:
        def run_once(self):
            return None

        def run(self, stop, concurrency=None):
            ran.append(concurrency)

    monkeypatch.setattr(cli, "open_runtime", lambda s: runtime)
    monkeypatch.setattr(cli, "build_worker", lambda r: _Worker())
    assert cli.main(["once"], settings=settings()) == EXIT_OK and "no due job" in capsys.readouterr().out
    assert cli.main(["run", "--concurrency", "1"], settings=settings()) == EXIT_OK and ran == [1]
    assert cli.main(["run", "--concurrency", "0"], settings=settings()) == EXIT_CONFIGURATION
    assert log_names == ["pi-worker-once.log", "pi-worker.log", "pi-worker.log"]
    monkeypatch.setattr(cli, "open_runtime", lambda s: _FakeRuntime(schema=False))
    assert cli.main(["run"], settings=settings()) == EXIT_CONFIGURATION

    class _Broken(_Worker):
        def run(self, stop, concurrency=None):
            raise RuntimeError("database gone")

    monkeypatch.setattr(cli, "open_runtime", lambda s: runtime)
    monkeypatch.setattr(cli, "build_worker", lambda r: _Broken())
    assert cli.main(["run"], settings=settings()) == EXIT_RUNTIME_FAILURE


# =========================================================================== logging for services
def test_service_logging_can_drop_the_console_handler(tmp_path) -> None:
    from logging.handlers import RotatingFileHandler

    from src.common.logging_config import reset_logging, setup_logging

    root = logging.getLogger()
    before = list(root.handlers)
    try:
        setup_logging(settings(log_to_file=True, log_to_console=False, log_dir=tmp_path,
                               log_file_name="svc.log"), force=True)
        managed = [h for h in root.handlers if h not in before]
        assert managed and all(isinstance(h, RotatingFileHandler) for h in managed)
        setup_logging(settings(log_to_file=False, log_to_console=False), force=True)
        managed = [h for h in root.handlers if h not in before]
        assert len(managed) == 1 and isinstance(managed[0], logging.StreamHandler)   # never no handler at all
    finally:
        reset_logging()


def test_every_shipped_target_has_a_runner_and_settings_defaults() -> None:
    cfg = settings()
    assert cfg.log_to_console is True
    assert cfg.scheduler_check_log_file_name == "pi-scheduler-check.log"
    assert cfg.worker_once_log_file_name == "pi-worker-once.log"
    assert {"mosdac", "grid_india_psp", "iex"} <= set(RUNNERS)
