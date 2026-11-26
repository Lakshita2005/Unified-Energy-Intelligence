"""Execute a job target by calling the existing ingestion runner (pipelines are not modified).

Each runner returns its own summary type; they are normalised to (status, IngestionResult, error) per
ingestion run and aggregated into one :class:`~src.scheduler.models.ExecutionOutcome`:

* every run SUCCESS -> ``SUCCEEDED``; any PARTIAL (rejected records) -> ``PARTIAL`` (final, not retried);
* any FAILED -> ``FAILED`` with the most retry-worthy classification of the errors (TRANSIENT beats
  PERMANENT); if every failure means "not published" -> ``NOT_PUBLISHED``;
* a CEA/Grid-India run that found the report missing (``MISSING_DAY``) and loaded no document ->
  ``NOT_PUBLISHED`` (the item is detected again later, never an error);
* IEX: one job = the price load and the snapshot load of one ``run_iex`` call; both run ids are recorded and
  the job succeeds only if both succeeded (:func:`iex_runs`).

Re-running a job is always safe: the pipelines skip byte-identical documents and unchanged records.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from typing import Any

from config.settings import Settings
from src.ingestion.models import IngestionResult, RunStatus
from src.scheduler.models import ErrorClass, ExecutionOutcome, JobStatus
from src.scheduler.retry import classify_error_text

RunTriple = tuple[RunStatus, IngestionResult | None, str | None]
Runner = Callable[[dict[str, Any], Settings], tuple[list[RunTriple], dict[str, Any]]]


def _d(value: str) -> date:
    return date.fromisoformat(value)


# ------------------------------------------------------------------------------- runner adapters

def run_cea_daily_target(target: dict[str, Any], settings: Settings) -> tuple[list[RunTriple], dict[str, Any]]:
    from src.pipelines.cea.runner import run_cea_daily

    runs, missing = [], []
    for command in target["commands"]:
        summary = run_cea_daily(command, start_date=_d(target["start_date"]), end_date=_d(target["end_date"]),
                                settings=settings)
        runs.append((summary.status, summary.result, summary.error))
        missing += [f"{command}:{m}" for m in summary.missing]
    return runs, {"missing": missing}


def run_cea_psp_target(target: dict[str, Any], settings: Settings) -> tuple[list[RunTriple], dict[str, Any]]:
    from src.pipelines.cea.api import parse_month
    from src.pipelines.cea.runner import run_cea_psp

    month = parse_month(target["month"])
    summary = run_cea_psp(target["kind"], start_month=month, end_month=month, settings=settings)
    return [(summary.status, summary.result, summary.error)], {"missing": list(summary.missing)}


def run_iex_target(target: dict[str, Any], settings: Settings) -> tuple[list[RunTriple], dict[str, Any]]:
    from src.pipelines.iex.runner import run_iex

    summary = run_iex(target["kind"], start_date=_d(target["start_date"]), end_date=_d(target["end_date"]),
                      settings=settings)
    return iex_runs(summary)


def _component(status: RunStatus, result: IngestionResult | None, error: str | None,
               **extra: Any) -> dict[str, Any]:
    return {"status": status.value, "ingestion_run_id": result.ingestion_run_id if result else None,
            "records_inserted": result.records_inserted if result else 0,
            "records_rejected": result.records_rejected if result else 0, "error": error, **extra}


def iex_runs(summary: Any) -> tuple[list[RunTriple], dict[str, Any]]:
    """Both IEX loads of one ``run_iex`` call: ``iex_market_price`` and ``iex_market_snapshot``.

    * Both run ids are recorded; each load's outcome is kept in ``detail["components"]``.
    * The job is SUCCEEDED only if BOTH loads succeeded: a failed snapshot load fails the job (retried by
      its error class), a snapshot load with rejected records makes it PARTIAL, and a snapshot that was not
      loaded although the price run succeeded (table missing, no snapshot document) fails it permanently.
    * If the price run failed, a skipped snapshot is only reported; the price failure decides the retry.
    A retry is safe: the price document is then a duplicate (skipped by SHA-256) while its stored snapshot
    document is offered to the snapshot run again, which loads it once.
    """
    price = (summary.status, summary.result, summary.error)
    runs: list[RunTriple] = [price]
    components: dict[str, Any] = {"price": _component(*price)}
    if summary.snapshot is not None:
        snapshot = (summary.snapshot.status, summary.snapshot, summary.snapshot.error)
        runs.append(snapshot)
        components["snapshot"] = _component(*snapshot)
    elif summary.status is RunStatus.FAILED:
        components["snapshot"] = {"status": "NOT_RUN", "ingestion_run_id": None,
                                  "skipped": summary.snapshot_skipped}
    else:
        error = f"ConfigurationError: snapshot volumes not loaded: {summary.snapshot_skipped or 'no snapshot run'}"
        runs.append((RunStatus.FAILED, None, error))
        components["snapshot"] = {"status": RunStatus.FAILED.value, "ingestion_run_id": None, "error": error,
                                  "skipped": summary.snapshot_skipped}
    return runs, {"components": components}


def run_grid_india_target(target: dict[str, Any], settings: Settings) -> tuple[list[RunTriple], dict[str, Any]]:
    from src.pipelines.grid_india.runner import run_grid_india_psp

    summary = run_grid_india_psp(start_date=_d(target["start_date"]), end_date=_d(target["end_date"]),
                                 sections=target.get("sections"), settings=settings)
    runs = [(s.status, s.result, s.error) for s in summary.sections.values()]
    if summary.fetch_error and not runs:
        runs = [(RunStatus.FAILED, None, summary.fetch_error)]
    return runs, {"missing": list(summary.missing), "workbooks": list(summary.workbooks)}


def run_npp_target(target: dict[str, Any], settings: Settings) -> tuple[list[RunTriple], dict[str, Any]]:
    from src.pipelines.npp.runner import run_npp

    summary = run_npp(target["kind"], start_date=_d(target["start_date"]), end_date=_d(target["end_date"]),
                      settings=settings)
    return [(o.status, o.result, o.error) for o in summary.outcomes], {}


def run_open_meteo_target(target: dict[str, Any], settings: Settings) -> tuple[list[RunTriple], dict[str, Any]]:
    from src.pipelines.open_meteo.runner import run_open_meteo

    kwargs: dict[str, Any] = {"locations": target.get("locations"), "settings": settings}
    if target["kind"] == "forecast":
        kwargs["forecast_days"] = int(target.get("forecast_days", 7))
    else:
        kwargs["start_date"], kwargs["end_date"] = _d(target["start_date"]), _d(target["end_date"])
    summary = run_open_meteo(target["kind"], **kwargs)
    return [(o.status, o.result, o.error) for o in summary.outcomes], {}


def mosdac_stored_locations(settings: Settings, kind: str, series_start: datetime) -> set[str]:
    """Plants that already hold the forecast series (read-only; replaced in tests)."""
    from src.common.database import DatabaseManager
    from src.scheduler.checkers.state import PostgresSourceState

    database = DatabaseManager.from_settings(settings)
    try:
        return PostgresSourceState(database, settings.db_schema).mosdac_series_locations(kind, series_start)
    finally:
        database.dispose()


def run_mosdac_target(target: dict[str, Any], settings: Settings) -> tuple[list[RunTriple], dict[str, Any]]:
    """Every plant independently (one ingestion run per plant and kind, as before).

    With the detected ``series_start`` in the target, plants that already hold that series are skipped: a retry
    requests only the plants that failed, and nothing is loaded twice. ``detail["plants"]`` records each plant
    (``loaded`` / ``already_stored`` / ``failed``); ``partial_success`` tells :func:`worker.decide` that some
    plants are stored although others failed (final status PARTIAL instead of FAILED once retries are used up).
    """
    from src.pipelines.mosdac.runner import load_locations, run_mosdac

    command = target.get("command", "all")
    single_kind = command in ("solar", "wind")
    series = target.get("series_start")
    todo = target.get("locations")                 # None = every configured plant (the runner decides)
    plants: dict[str, str] = {}
    detail: dict[str, Any] = {"series_start": series, "plants": plants}
    if series and single_kind:
        codes = list(todo or sorted(load_locations(settings)))
        already = mosdac_stored_locations(settings, command, datetime.fromisoformat(series)) & set(codes)
        plants.update({code: "already_stored" for code in sorted(already)})
        todo = [code for code in codes if code not in already]
        if not todo:
            detail["all_stored"] = True
            return [], detail
    summary = run_mosdac(command, locations=todo, settings=settings)
    runs: list[RunTriple] = []
    for run_key, result in summary.runs.items():
        code, kind = run_key if isinstance(run_key, tuple) else (run_key, command)
        key = code if single_kind else f"{code}/{kind}"
        failed = result.status is RunStatus.FAILED
        plants[key] = "failed" if failed else "loaded"
        error = f"{result.error or 'unknown error'} [{key}]" if failed else result.error
        runs.append((result.status, result, error))
    for code, error in summary.location_errors.items():
        plants[code] = "failed"
        runs.append((RunStatus.FAILED, None, f"MosdacLocationError: {code}: {error}"))
    detail["plants_failed"] = sorted(k for k, v in plants.items() if v == "failed")
    detail["plants_ok"] = sorted(k for k, v in plants.items() if v != "failed")
    detail["partial_success"] = bool(detail["plants_failed"] and detail["plants_ok"])
    return runs, detail


RUNNERS: dict[str, Runner] = {
    "cea_daily": run_cea_daily_target,
    "cea_psp": run_cea_psp_target,
    "iex": run_iex_target,
    "grid_india_psp": run_grid_india_target,
    "npp": run_npp_target,
    "open_meteo": run_open_meteo_target,
    "mosdac": run_mosdac_target,
}


# ------------------------------------------------------------------------------- aggregation

def summarise(runs: list[RunTriple], detail: dict[str, Any]) -> ExecutionOutcome:
    """Aggregate the runs of one job into an outcome (before the retry policy is applied)."""
    outcome = ExecutionOutcome(JobStatus.SUCCEEDED, detail=dict(detail))
    for _status, result, _error in runs:
        if result is None:
            continue
        outcome.ingestion_run_ids.append(result.ingestion_run_id)
        outcome.records_read += result.records_read
        outcome.records_inserted += result.records_inserted
        outcome.records_updated += result.records_updated
        outcome.records_unchanged += result.records_unchanged
        outcome.records_rejected += result.records_rejected
        outcome.data_quality_issues += result.data_quality_issues
    documents = sum(r.documents_processed for _s, r, _e in runs if r is not None)
    outcome.detail["documents_processed"] = documents
    outcome.detail["run_statuses"] = [s.value for s, _r, _e in runs]
    if not runs and detail.get("all_stored"):      # e.g. a MOSDAC retry whose plants are all stored by now
        return outcome
    if not runs:
        outcome.status, outcome.error, outcome.error_class = (
            JobStatus.FAILED, "the runner returned no ingestion run", ErrorClass.PERMANENT)
        return outcome
    failures = [(e or "unknown error") for s, _r, e in runs if s is RunStatus.FAILED]
    if failures:
        classes = {classify_error_text(e) for e in failures}
        outcome.error = " | ".join(failures)[:4000]
        if classes == {ErrorClass.NOT_PUBLISHED}:
            outcome.status, outcome.error_class = JobStatus.NOT_PUBLISHED, ErrorClass.NOT_PUBLISHED
        else:
            outcome.status = JobStatus.FAILED
            outcome.error_class = ErrorClass.TRANSIENT if ErrorClass.TRANSIENT in classes else ErrorClass.PERMANENT
        return outcome
    if detail.get("missing") and documents == 0:
        outcome.status, outcome.error_class = JobStatus.NOT_PUBLISHED, ErrorClass.NOT_PUBLISHED
        outcome.error = f"not published (yet): {detail['missing']}"
        return outcome
    if any(s is RunStatus.PARTIAL for s, _r, _e in runs):
        outcome.status = JobStatus.PARTIAL
    return outcome


class Executor:
    """Runs job targets. ``runners`` can be replaced in tests."""

    def __init__(self, settings: Settings, runners: dict[str, Runner] | None = None) -> None:
        self.settings = settings
        self.runners = dict(RUNNERS if runners is None else runners)

    def execute(self, target: dict[str, Any]) -> ExecutionOutcome:
        """Run a target. Exceptions from the runner propagate (the worker classifies them)."""
        name = target.get("runner")
        if name not in self.runners:
            raise ValueError(f"unknown job runner {name!r}; available: {sorted(self.runners)}")
        runs, detail = self.runners[name](target, self.settings)
        return summarise(runs, detail)
