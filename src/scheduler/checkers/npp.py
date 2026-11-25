"""NPP availability checker (All-India demand met / generation by source, intraday snapshots).

NPP serves one IST day per request: snapshots roughly every 2-4 minutes, each with an ``updated_on`` epoch
in milliseconds. There is no cheaper "what's new" endpoint, so the check requests the day and compares the
newest ``updated_on`` with the newest stored ``source_updated_on_ms``:

* today: newer snapshots than stored -> *new* (the job loads only snapshots not seen before);
* yesterday: checked on the first check of a day, so the day is completed after midnight;
* earlier days in the catch-up window: only when nothing is stored for them at all (catch-up after downtime);
* an empty day -> *not published yet*.

Item ``"YYYY-MM-DD"``, version = newest ``updated_on``.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any, ClassVar

from src.pipelines.npp import api as npp_api
from src.scheduler.checkers.base import AvailabilityChecker, CheckContext, date_span
from src.scheduler.models import CheckResult, Detection

ENDPOINTS = {"demand": npp_api.DEMAND, "generation": npp_api.GENERATION}


def newest_updated_on(content: bytes) -> int | None:
    """Highest integer ``updated_on`` in an NPP response; None for an empty array.

    Raises:
        ValueError: The response is not a JSON array of objects.
    """
    payload = json.loads(content)
    if not isinstance(payload, list):
        raise ValueError(f"NPP response is {type(payload).__name__}, expected a JSON array")
    values = [item.get("updated_on") for item in payload if isinstance(item, dict)]
    values = [v for v in values if isinstance(v, int) and not isinstance(v, bool)]
    return max(values) if values else None


class NppChecker(AvailabilityChecker):
    name: ClassVar[str] = "npp"
    defaults: ClassVar[dict[str, Any]] = {"kind": "demand"}

    def validate(self) -> None:
        if self.params["kind"] not in ENDPOINTS:
            raise ValueError(f"{self.schedule.key}: kind must be one of {sorted(ENDPOINTS)}")

    def check(self, ctx: CheckContext) -> CheckResult:
        endpoint = ENDPOINTS[self.params["kind"]]
        first = max(ctx.today - timedelta(days=ctx.catchup_days() - 1), npp_api.HISTORY_START)
        days = date_span(first, ctx.today)
        stored = ctx.state.npp_max_updated_ms(endpoint.series, days)
        candidates = [d for d in days if d == ctx.today
                      or (d == ctx.today - timedelta(days=1) and ctx.first_check_of_day)
                      or d not in stored]
        url = npp_api.endpoint_url(ctx.settings.npp_base_url, endpoint)
        detections: list[Detection] = []
        not_published: list[str] = []
        for day in candidates:
            response = npp_api.call_api(ctx.http, url, npp_api.request_params(day))
            newest = newest_updated_on(response.content)
            if newest is None:
                if day not in stored:
                    not_published.append(day.isoformat())
                continue
            if newest <= stored.get(day, -1):
                continue
            detections.append(Detection(
                day.isoformat(), str(newest),
                {"runner": "npp", "kind": self.params["kind"], "start_date": day.isoformat(),
                 "end_date": day.isoformat()},
                {"newest_updated_on_ms": newest, "stored_updated_on_ms": stored.get(day),
                 "revision": False, "final_day": day < ctx.today}))
        return CheckResult.from_detections(detections, not_published=not_published, requests=len(candidates),
                                           days=[d.isoformat() for d in candidates])
