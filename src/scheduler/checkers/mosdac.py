"""MOSDAC availability checker: ONE forecast kind (solar OR wind) per schedule.

Solar and wind are independent schedules (``MOSDAC_SOLAR`` and ``MOSDAC_WIND`` in ``config/scheduler.toml``)
with independent checks, jobs, retries and watermarks. A wind outage (the wind endpoint returned HTTP 500 for
weeks) therefore can neither fail nor hide a successful solar ingestion, and wind is re-checked and retried
on its own interval as soon as the endpoint recovers: it never waits for a new solar series.

MOSDAC publishes no forecast issue time. The pipeline identifies a forecast series by its earliest valid
time (``series_start_at``). The check requests the schedule's kind for ONE reference plant (one request
instead of one per plant) and compares its series start with the newest stored one for that plant and kind
(``mosdac_solar_forecast`` / ``mosdac_wind_forecast``):

* a different series start -> a *new forecast series* -> job ``run_mosdac(<kind>)`` for all plants;
* the same series start -> nothing new (a byte-identical re-fetch would be skipped anyway);
* HTTP 5xx / timeouts -> source error for THIS schedule only, re-checked at its next interval.

Item ``"series|<series start>"``. The job target uses the unchanged runner interface
``run_mosdac("solar" | "wind")`` plus the detected ``series_start``; the MOSDAC pipeline itself is not modified.
The job processes every plant independently (``executors.run_mosdac_target``): plants that already hold the
series are skipped, so a retry requests only the plants that failed, and a job that ends with some plants
stored and some failed is recorded as PARTIAL (see ``worker.decide``).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, ClassVar

from src.pipelines.mosdac.api import MosdacClient, decode
from src.pipelines.mosdac.parse import SPECS, Location, parse_series
from src.pipelines.mosdac.runner import load_locations
from src.scheduler.checkers.base import AvailabilityChecker, CheckContext
from src.scheduler.models import CheckResult, Detection

KINDS = ("solar", "wind")


class MosdacChecker(AvailabilityChecker):
    name: ClassVar[str] = "mosdac"
    defaults: ClassVar[dict[str, Any]] = {
        "kind": None,                 # required: solar or wind (one kind per schedule)
        "reference_location": None,   # plant code probed; default: the first in MOSDAC_LOCATIONS_FILE
    }

    def validate(self) -> None:
        if self.params["kind"] not in KINDS:
            raise ValueError(f"{self.schedule.key}: params.kind must be one of {KINDS} "
                             "(solar and wind are separate schedules)")

    @property
    def kind(self) -> str:
        return str(self.params["kind"])

    def check(self, ctx: CheckContext) -> CheckResult:
        locations = load_locations(ctx.settings)
        if not locations:
            raise ValueError("no MOSDAC locations configured (MOSDAC_LOCATIONS_FILE)")
        code = self.params["reference_location"] or next(iter(locations))
        if code not in locations:
            raise ValueError(f"reference location {code!r} is not in MOSDAC_LOCATIONS_FILE")
        config = locations[code]
        response = MosdacClient(ctx.http, ctx.settings).get(self.kind, str(config.latitude), str(config.longitude))
        parsed = parse_series(decode(response.content, f"MOSDAC {self.kind} {code}"), SPECS[self.kind],
                              Location(code, config.latitude, config.longitude), datetime.now(timezone.utc))
        series_start = parsed.series_start_at
        if series_start is None:
            return CheckResult.from_detections([], not_published=[f"{code}: empty {self.kind} series"], requests=1)
        stored = ctx.state.mosdac_latest_series(code, self.kind)
        if stored is not None and stored == series_start:
            return CheckResult.from_detections([], requests=1, kind=self.kind, reference_location=code,
                                               series_start=series_start.isoformat())
        detection = Detection(
            f"series|{series_start.isoformat()}", series_start.isoformat(),
            {"runner": "mosdac", "command": self.kind, "series_start": series_start.isoformat()},
            {"kind": self.kind, "reference_location": code, "series_start": series_start.isoformat(),
             "stored_series_start": stored.isoformat() if stored else None, "points": len(parsed.records),
             "revision": False})
        return CheckResult.from_detections([detection], requests=1)
