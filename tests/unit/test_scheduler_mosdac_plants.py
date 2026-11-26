"""MOSDAC jobs process every plant independently (2026-11-26: the wind endpoint returned HTTP 500 for 5 of 8 plants).

* plants that already hold the detected series are skipped, so a retry requests only the failed plants;
* each plant's outcome is recorded in the job detail; nothing is loaded twice;
* a job that keeps failing for some plants while others are stored ends PARTIAL (not FAILED), with the failed
  plants in ``last_error``; its watermark stays retryable (FAILED -> re-queued after the failure cool-down);
* a series whose plants are all stored by the time the job runs succeeds without a request.
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from src.ingestion.models import IngestionResult, RunStatus
from src.scheduler.clock import FakeClock
from src.scheduler.executors import Executor, run_mosdac_target, summarise
from src.scheduler.locks import InProcessLockFactory
from src.scheduler.models import ErrorClass, ExecutionOutcome, JobStatus, WatermarkStatus
from src.scheduler.worker import Worker, decide, watermark_for
from tests.support.scheduler import InMemoryJobRepository, detection, schedule, settings

IST = ZoneInfo("Asia/Kolkata")
T0 = datetime(2026, 11, 26, 12, 0, tzinfo=IST)
SERIES = "2026-11-25T05:30:00+05:30"
PLANTS = ["ASPL", "ESEPL", "GSPL_GAP", "GSPL_GUM", "NSPL", "PSPL", "RSPL", "TSPL"]
BROKEN = {"ASPL", "GSPL_GAP", "GSPL_GUM", "NSPL", "TSPL"}            # the 26-Nov wind outage
HTTP500 = "HttpStatusError: GET https://mosdac.gov.in/apienergy/wind returned HTTP 500"


def _run(run_id: int, ok: bool) -> IngestionResult:
    return IngestionResult(ingestion_run_id=run_id, dataset_code="MOSDAC_WIND_FORECAST",
                           status=RunStatus.SUCCESS if ok else RunStatus.FAILED, records_read=289 if ok else 0,
                           records_inserted=289 if ok else 0, records_rejected=0, documents_processed=1 if ok else 0,
                           error=None if ok else HTTP500)


@pytest.fixture
def mosdac(monkeypatch):
    """Fake MOSDAC: ``broken`` plants fail, the others load; records which plants were requested."""
    state = SimpleNamespace(stored=set(), broken=set(BROKEN), requests=[], next_id=100)

    def run_mosdac(command, *, locations=None, settings=None, **_):
        codes = list(locations) if locations is not None else list(PLANTS)
        state.requests.append(codes)
        runs = {}
        for code in codes:
            state.next_id += 1
            ok = code not in state.broken
            runs[(code, command)] = _run(state.next_id, ok)
            if ok:
                state.stored.add(code)
        return SimpleNamespace(runs=runs, location_errors={})

    monkeypatch.setattr("src.pipelines.mosdac.runner.run_mosdac", run_mosdac)
    monkeypatch.setattr("src.pipelines.mosdac.runner.load_locations", lambda s: {c: None for c in PLANTS})
    monkeypatch.setattr("src.scheduler.executors.mosdac_stored_locations", lambda s, kind, start: set(state.stored))
    return state


TARGET = {"runner": "mosdac", "command": "wind", "series_start": SERIES}


def test_each_plant_is_recorded_and_some_success_is_flagged_as_partial(mosdac) -> None:
    runs, detail = run_mosdac_target(TARGET, settings())
    assert mosdac.requests == [PLANTS]                                        # first attempt: every plant
    assert detail["plants_failed"] == sorted(BROKEN) and detail["plants_ok"] == ["ESEPL", "PSPL", "RSPL"]
    assert detail["partial_success"] is True
    outcome = summarise(runs, detail)
    assert outcome.status is JobStatus.FAILED and outcome.error_class is ErrorClass.TRANSIENT
    assert outcome.records_inserted == 3 * 289
    assert all(f"[{code}]" in outcome.error for code in BROKEN)              # failed plants named in the error


def test_a_retry_requests_only_the_plants_that_are_missing(mosdac) -> None:
    mosdac.stored = {"ESEPL", "PSPL", "RSPL"}
    runs, detail = run_mosdac_target(TARGET, settings())
    assert mosdac.requests == [sorted(BROKEN)]                                # stored plants are not re-requested
    assert {k for k, v in detail["plants"].items() if v == "already_stored"} == {"ESEPL", "PSPL", "RSPL"}
    assert summarise(runs, detail).records_inserted == 0                      # nothing loaded twice


def test_all_plants_already_stored_succeeds_without_a_request(mosdac) -> None:
    mosdac.stored = set(PLANTS)
    runs, detail = run_mosdac_target(TARGET, settings())
    assert mosdac.requests == [] and runs == [] and detail["all_stored"] is True
    outcome = summarise(runs, detail)
    assert outcome.status is JobStatus.SUCCEEDED and watermark_for(outcome.status, outcome) is WatermarkStatus.INGESTED


def test_every_plant_loaded_is_a_plain_success_as_before(mosdac) -> None:
    mosdac.broken = set()
    runs, detail = run_mosdac_target({"runner": "mosdac", "command": "solar", "series_start": SERIES}, settings())
    outcome = summarise(runs, detail)
    assert outcome.status is JobStatus.SUCCEEDED and detail["partial_success"] is False
    assert outcome.records_inserted == 8 * 289 and len(outcome.ingestion_run_ids) == 8


def test_a_target_without_series_start_keeps_the_previous_behaviour(mosdac) -> None:
    run_mosdac_target({"runner": "mosdac", "command": "wind"}, settings())
    assert mosdac.requests == [PLANTS]                                        # runner decides the plants (all)


def test_decide_partial_only_after_retries_are_used_up() -> None:
    outcome = ExecutionOutcome(JobStatus.FAILED, error=HTTP500, error_class=ErrorClass.TRANSIENT,
                               detail={"partial_success": True})
    retrying = SimpleNamespace(attempts=1, max_attempts=5, backoff_base_seconds=120, backoff_max_seconds=1800)
    assert decide(retrying, outcome)[0] is JobStatus.RETRY_WAIT
    exhausted = SimpleNamespace(attempts=5, max_attempts=5, backoff_base_seconds=120, backoff_max_seconds=1800)
    assert decide(exhausted, outcome) == (JobStatus.PARTIAL, None)
    assert watermark_for(JobStatus.PARTIAL, outcome) is WatermarkStatus.FAILED   # re-queued after the cool-down
    no_data = ExecutionOutcome(JobStatus.FAILED, error=HTTP500, error_class=ErrorClass.TRANSIENT)
    assert decide(exhausted, no_data) == (JobStatus.FAILED, None)                 # nothing stored: still FAILED
    assert watermark_for(JobStatus.PARTIAL, ExecutionOutcome(JobStatus.PARTIAL)) is WatermarkStatus.INGESTED_WITH_REJECTS


def test_worker_end_to_end_retries_only_failed_plants_then_ends_partial(mosdac) -> None:
    repo, clock = InMemoryJobRepository(), FakeClock(T0)
    s = schedule("MOSDAC_WIND", lock_group="MOSDAC", max_attempts=2)
    job_id = repo.enqueue(s, detection(f"series|{SERIES}", SERIES, runner="mosdac", command="wind",
                                       series_start=SERIES), T0)
    worker = Worker(repository=repo, executor=Executor(settings(), runners={"mosdac": run_mosdac_target}),
                    locks=InProcessLockFactory(), clock=clock, settings=settings(), worker_id="w1")
    worker.run_once()
    assert repo.job(job_id).status is JobStatus.RETRY_WAIT
    clock.advance(minutes=10)
    worker.run_once()
    job = repo.job(job_id)
    assert job.status is JobStatus.PARTIAL
    assert mosdac.requests == [PLANTS, sorted(BROKEN)]                        # attempt 2: only the failed plants
    assert all(code in job.last_error for code in BROKEN)
    assert repo.get_watermark("MOSDAC_WIND", f"series|{SERIES}").status is WatermarkStatus.FAILED
    mosdac.broken = set()                                                     # endpoint recovers later
    runs, detail = run_mosdac_target(TARGET, settings())
    assert mosdac.requests[-1] == sorted(BROKEN) and summarise(runs, detail).status is JobStatus.SUCCEEDED
