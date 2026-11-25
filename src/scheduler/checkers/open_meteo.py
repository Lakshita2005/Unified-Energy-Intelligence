"""Open-Meteo availability checkers (forecast refresh cycle; historical archive, off by default).

Our forecast data carries no issue time (``forecast_issued_at`` is NULL, and the model-run metadata endpoint
was not verified), so the forecast is refreshed on a configurable cycle instead of probed: the latest
refresh slot (e.g. 01:30, 07:30, 13:30, 19:30 local) that has passed and has not been ingested yet is
*new*. No request is made by the check itself, which keeps API usage at one call per location per slot.
Missed slots during downtime collapse into the latest one (an old forecast is not worth fetching late).
The pipeline stores only changed values as new versions, so a refresh never duplicates rows.

The historical checker targets ``today - lag_days`` (ERA5 is about five days behind) and, with catch-up,
earlier days that have no ingested watermark. It is disabled in the default configuration.

Plan policy (:func:`open_meteo_refusal`, applied by the scheduler gate):

* ``OPEN_METEO_PLAN=free`` (default): the free endpoints ``api.open-meteo.com`` / ``archive-api.open-meteo.com``,
  **no API key**, non-commercial use only, fewer than ``OPEN_METEO_DAILY_CALL_LIMIT`` (10,000) calls a day,
  data under CC BY 4.0 (attribution required). A key must NOT be set: the pipeline would then switch to the
  paid ``customer-`` hosts, so the schedule is refused instead of changing endpoint silently.
* ``OPEN_METEO_PLAN=commercial``: requires ``OPEN_METEO_API_KEY`` (paid plan, ``customer-`` hosts).
* On the free plan the worst-case daily call count of all enabled Open-Meteo schedules (every slot or day,
  every location, every retry attempt, Open-Meteo's weighting of long or wide requests) must stay within
  the limit, otherwise they are refused.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any, ClassVar

from config.settings import Settings
from src.scheduler.checkers.base import AvailabilityChecker, CheckContext, date_span
from src.scheduler.config import ScheduleConfig, parse_clock_time
from src.scheduler.models import CheckResult, Detection, WatermarkStatus

DONE = {WatermarkStatus.INGESTED, WatermarkStatus.INGESTED_WITH_REJECTS, WatermarkStatus.QUEUED,
        WatermarkStatus.QUARANTINED}


class OpenMeteoForecastChecker(AvailabilityChecker):
    name: ClassVar[str] = "open_meteo_forecast"
    defaults: ClassVar[dict[str, Any]] = {
        "slots": ["01:30", "07:30", "13:30", "19:30"],
        "forecast_days": 7,
        "locations": None,     # None = every registered weather location
    }

    def validate(self) -> None:
        for slot in self.params["slots"]:
            if parse_clock_time(slot, f"{self.schedule.key} slot") is None:
                raise ValueError(f"{self.schedule.key}: slot 24:00 is not allowed")
        if not self.params["slots"]:
            raise ValueError(f"{self.schedule.key}: at least one slot is required")

    def latest_slot(self, ctx: CheckContext) -> datetime:
        local = ctx.local_now
        candidates = []
        for day in (local.date() - timedelta(days=1), local.date()):
            for slot in self.params["slots"]:
                moment = datetime.combine(day, parse_clock_time(slot, "slot"), ctx.tz)
                if moment <= local:
                    candidates.append(moment)
        return max(candidates)

    def check(self, ctx: CheckContext) -> CheckResult:
        slot = self.latest_slot(ctx)
        key = f"slot|{slot.isoformat()}"
        mark = ctx.watermark(key)
        if mark is not None and mark.status in DONE:
            return CheckResult.from_detections([], slot=slot.isoformat(), requests=0)
        target = {"runner": "open_meteo", "kind": "forecast", "forecast_days": int(self.params["forecast_days"]),
                  "locations": self.params["locations"]}
        return CheckResult.from_detections(
            [Detection(key, "refresh", target, {"slot": slot.isoformat(), "revision": False})], requests=0)


class OpenMeteoHistoricalChecker(AvailabilityChecker):
    name: ClassVar[str] = "open_meteo_historical"
    defaults: ClassVar[dict[str, Any]] = {"lag_days": 6, "locations": None}

    def check(self, ctx: CheckContext) -> CheckResult:
        newest = ctx.today - timedelta(days=int(self.params["lag_days"]))
        first = newest - timedelta(days=ctx.catchup_days() - 1)
        detections = []
        for day in date_span(first, newest):
            key = day.isoformat()
            mark = ctx.watermark(key)
            if mark is not None and mark.status in DONE:
                continue
            detections.append(Detection(key, "archive", {"runner": "open_meteo", "kind": "historical",
                                                         "start_date": key, "end_date": key,
                                                         "locations": self.params["locations"]},
                                        {"revision": False}))
        return CheckResult.from_detections(detections, requests=0, window=f"{first}..{newest}")


# ------------------------------------------------------------------------------- plan and call budget
SOURCE = "OPEN_METEO"
FREE_ENDPOINTS = ("https://api.open-meteo.com/v1/forecast", "https://archive-api.open-meteo.com/v1/archive")
# Open-Meteo counts a request with more than 10 variables, or more than 2 weeks of data, as several calls.
VARIABLES_PER_CALL = 10
DAYS_PER_CALL = 14


def call_weight(variables: int, days: int) -> int:
    """Calls one request counts as (rounded up: conservative)."""
    return math.ceil(max(variables, 1) / VARIABLES_PER_CALL) * math.ceil(max(days, 1) / DAYS_PER_CALL)


def location_count(schedule: ScheduleConfig, settings: Settings) -> int:
    """Locations a job requests: the schedule's ``locations`` or every non-test location in the file.

    Locations registered only in the database are not counted (the estimate reads no database).
    """
    if schedule.params.get("locations"):
        return len(schedule.params["locations"])
    from src.pipelines.open_meteo.locations import is_test_location, load_location_config

    return sum(1 for c in load_location_config(settings.weather_locations_file) if not is_test_location(c.code))


def estimated_daily_calls(schedule: ScheduleConfig, locations: int) -> int:
    """Worst-case API calls per day for one schedule, counting every retry attempt."""
    from src.pipelines.open_meteo.api import HOURLY_VARIABLES

    variables = len(HOURLY_VARIABLES)
    if schedule.checker == OpenMeteoForecastChecker.name:
        slots = len(schedule.params.get("slots") or OpenMeteoForecastChecker.defaults["slots"])
        days = int(schedule.params.get("forecast_days", OpenMeteoForecastChecker.defaults["forecast_days"]))
        requests_per_day, weight = slots, call_weight(variables, days)
    elif schedule.checker == OpenMeteoHistoricalChecker.name:
        requests_per_day = schedule.catchup_max_days if schedule.catchup_enabled else 1
        weight = call_weight(variables, 1)
    else:
        return 0
    return requests_per_day * locations * schedule.max_attempts * weight


def open_meteo_refusal(schedule: ScheduleConfig, enabled: Iterable[ScheduleConfig],
                       settings: Settings) -> str | None:
    """Why an Open-Meteo schedule must not run under the configured plan, or None.

    ``enabled`` = every schedule enabled in the file (the daily budget is shared by all of them).
    """
    key = settings.open_meteo_api_key
    has_key = key is not None and bool(key.get_secret_value())
    if settings.open_meteo_plan == "free":
        if has_key:
            return ("OPEN_METEO_PLAN=free but OPEN_METEO_API_KEY is set: the pipeline would switch to the paid "
                    "customer- endpoint. Clear the key to use the free (non-commercial) endpoint, or set "
                    "OPEN_METEO_PLAN=commercial for a paid plan")
        peers = {s.key: s for s in enabled if s.source == SOURCE}
        peers[schedule.key] = schedule
        try:
            total = sum(estimated_daily_calls(s, location_count(s, settings)) for s in peers.values())
        except Exception as exc:   # unreadable locations file: the budget cannot be shown to hold
            return f"cannot estimate Open-Meteo API usage ({type(exc).__name__}: {exc})"
        if total > settings.open_meteo_daily_call_limit:
            return (f"worst-case Open-Meteo usage of the enabled schedules is {total} calls/day, above the free "
                    f"limit of {settings.open_meteo_daily_call_limit} (OPEN_METEO_DAILY_CALL_LIMIT); reduce "
                    "slots, locations or max_attempts")
        return None
    if not has_key:
        return "OPEN_METEO_PLAN=commercial requires OPEN_METEO_API_KEY (paid plan)"
    return None
