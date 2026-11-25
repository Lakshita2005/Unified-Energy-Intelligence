"""Failure classification, exponential backoff and the mapping of runner results to job outcomes."""

from __future__ import annotations

import httpx
import pytest

from src.common.exceptions import ConfigurationError, HttpRequestError, HttpStatusError
from src.ingestion.models import IngestionResult, RunStatus
from src.pipelines.grid_india.errors import InvalidWorkbookError
from src.pipelines.iex.errors import EmptyResponseError, IexApiError
from src.scheduler.executors import Executor, summarise
from src.scheduler.models import ErrorClass, JobStatus
from src.scheduler.retry import backoff_seconds, classify_error_text, classify_exception
from tests.support.scheduler import settings


def _status_error(code: int) -> HttpStatusError:
    return HttpStatusError(f"GET x returned HTTP {code}", status_code=code, url="https://x", response_text="")


@pytest.mark.parametrize(("exc", "expected"), [
    (_status_error(503), ErrorClass.TRANSIENT),
    (_status_error(429), ErrorClass.TRANSIENT),
    (_status_error(404), ErrorClass.NOT_PUBLISHED),
    (_status_error(403), ErrorClass.PERMANENT),
    (HttpRequestError("connection reset after 3 attempts", url="https://x"), ErrorClass.TRANSIENT),
    (httpx.ReadTimeout("timed out"), ErrorClass.TRANSIENT),
    (TimeoutError("slow"), ErrorClass.TRANSIENT),
    (EmptyResponseError("data: []"), ErrorClass.NOT_PUBLISHED),
    (IexApiError("IEX returned HTTP 502 for https://x"), ErrorClass.TRANSIENT),
    (InvalidWorkbookError("section C columns not found: ['max_od_ud']"), ErrorClass.PERMANENT),
    (ConfigurationError("reference rows missing"), ErrorClass.PERMANENT),
    (ValueError("2026-11-26 is in the future"), ErrorClass.PERMANENT),
])
def test_classify_exception(exc, expected) -> None:
    assert classify_exception(exc) is expected


@pytest.mark.parametrize(("text", "expected"), [
    ("HttpRequestError: GET https://x failed after 4 attempts: ConnectTimeout", ErrorClass.TRANSIENT),
    ("GridIndiaApiError: listing API returned HTTP 500 for {...}", ErrorClass.TRANSIENT),
    ("CeaHttpError: CEA/NPP returned HTTP 404 for https://x", ErrorClass.NOT_PUBLISHED),
    ("EmptyResponseError: IEX returned no records (data: [])", ErrorClass.NOT_PUBLISHED),
    ("InvalidResponseError: record fields changed", ErrorClass.PERMANENT),
    ("SomethingNew: never seen before", ErrorClass.PERMANENT),
    (None, ErrorClass.TRANSIENT),
])
def test_classify_error_text(text, expected) -> None:
    assert classify_error_text(text) is expected


def test_backoff_doubles_and_is_capped() -> None:
    delays = [backoff_seconds(n, base=120, maximum=1800) for n in range(1, 7)]
    assert delays == [120, 240, 480, 960, 1800, 1800]
    assert backoff_seconds(0, base=120, maximum=1800) == 0


# --------------------------------------------------------------------------- summarise
def _result(run_id: int, status: RunStatus, **counts) -> IngestionResult:
    return IngestionResult(ingestion_run_id=run_id, dataset_code="X", status=status, **counts)


def test_all_success_aggregates_runs_and_counts() -> None:
    runs = [(RunStatus.SUCCESS, _result(1, RunStatus.SUCCESS, records_read=10, records_inserted=8,
                                        records_unchanged=2, documents_processed=1), None),
            (RunStatus.SUCCESS, _result(2, RunStatus.SUCCESS, records_read=5, records_inserted=5,
                                        documents_processed=1), None)]
    outcome = summarise(runs, {})
    assert outcome.status is JobStatus.SUCCEEDED and outcome.ingestion_run_ids == [1, 2]
    assert (outcome.records_read, outcome.records_inserted, outcome.records_unchanged) == (15, 13, 2)


def test_partial_and_failures_are_classified() -> None:
    partial = summarise([(RunStatus.PARTIAL, _result(3, RunStatus.PARTIAL, records_rejected=2,
                                                     documents_processed=1), None)], {})
    assert partial.status is JobStatus.PARTIAL and partial.records_rejected == 2
    mixed = summarise([(RunStatus.SUCCESS, _result(4, RunStatus.SUCCESS, documents_processed=1), None),
                       (RunStatus.FAILED, _result(5, RunStatus.FAILED), "IexApiError: IEX returned HTTP 503"),
                       (RunStatus.FAILED, None, "InvalidResponseError: changed")], {})
    assert mixed.status is JobStatus.FAILED and mixed.error_class is ErrorClass.TRANSIENT   # transient wins
    permanent = summarise([(RunStatus.FAILED, None, "InvalidWorkbookError: section C columns not found")], {})
    assert permanent.error_class is ErrorClass.PERMANENT
    not_published = summarise([(RunStatus.FAILED, None, "EmptyResponseError: data: []")], {})
    assert not_published.status is JobStatus.NOT_PUBLISHED


def test_missing_report_without_documents_is_not_published_not_success() -> None:
    runs = [(RunStatus.SUCCESS, _result(6, RunStatus.SUCCESS, documents_processed=0, data_quality_issues=1), None)]
    assert summarise(runs, {"missing": ["coal:2026-11-24"]}).status is JobStatus.NOT_PUBLISHED
    loaded = [(RunStatus.SUCCESS, _result(7, RunStatus.SUCCESS, documents_processed=1), None)]
    assert summarise(loaded, {"missing": ["coal:2026-11-23"]}).status is JobStatus.SUCCEEDED
    assert summarise([], {}).status is JobStatus.FAILED


def test_executor_dispatches_to_runner_and_rejects_unknown() -> None:
    seen = []

    def fake(target, cfg):
        seen.append(target)
        return [(RunStatus.SUCCESS, _result(9, RunStatus.SUCCESS, documents_processed=1), None)], {}

    executor = Executor(settings(), runners={"iex": fake})
    assert executor.execute({"runner": "iex", "kind": "rtm"}).ingestion_run_ids == [9]
    assert seen == [{"runner": "iex", "kind": "rtm"}]
    with pytest.raises(ValueError, match="unknown job runner"):
        executor.execute({"runner": "nope"})


def test_every_checker_target_has_a_runner() -> None:
    from src.scheduler.executors import RUNNERS

    assert set(RUNNERS) == {"cea_daily", "cea_psp", "iex", "grid_india_psp", "npp", "open_meteo", "mosdac"}
