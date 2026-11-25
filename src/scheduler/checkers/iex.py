"""IEX availability checker (DAM, GDAM, RTM).

One ``market-snapshot`` request covers the whole window (the API accepts a date range), parsed with the
pipeline's own parser, and the system-wide MCP (area ``MKT``) per delivery date and time block is compared
with the latest stored MKT prices:

* blocks published but not stored -> *new* (RTM publishes blocks incrementally as sessions clear, so a
  partial day is normal: 40 of 96 blocks now, 52 later; each growth is a new version of the item);
* a stored block whose published price changed -> *revision* (the pipeline stores it as a new version);
* a day with no published blocks -> *not published yet* (``LATE`` after ``late_after_hours`` for DAM/GDAM).

DAM and GDAM look up to tomorrow (results for D are published on D-1 afternoon); RTM up to today.
Item ``"YYYY-MM-DD"``, version = hash of the published MKT blocks. The job re-runs the existing IEX
pipeline for that delivery date; unchanged rows are skipped by its VERSION policy, so nothing is
duplicated. Bid-area prices are fetched by the job (14 requests), not by the check (1 request).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, ClassVar

from src.pipelines.iex import api as iex_api
from src.pipelines.iex.errors import EmptyResponseError
from src.pipelines.iex.parser import parse_snapshot
from src.scheduler.checkers.base import AvailabilityChecker, CheckContext, date_span
from src.scheduler.models import CheckResult, Detection, version_token


class IexChecker(AvailabilityChecker):
    name: ClassVar[str] = "iex"
    defaults: ClassVar[dict[str, Any]] = {
        "market": "dam",            # dam, gdam, rtm
        "late_after_hours": None,   # LATE when a delivery date is still unpublished this long after D-1 00:00 local
    }

    def validate(self) -> None:
        if self.params["market"] not in iex_api.MARKETS:
            raise ValueError(f"{self.schedule.key}: market must be one of {sorted(iex_api.MARKETS)}")

    def check(self, ctx: CheckContext) -> CheckResult:
        market = iex_api.MARKETS[self.params["market"]]
        last = ctx.today + timedelta(days=market.days_ahead)
        first = max(ctx.today - timedelta(days=ctx.catchup_days() - 1), market.history_start)
        url = iex_api.snapshot_url(ctx.settings.iex_api_base_url, market)
        response = iex_api.call_api(ctx.http, url, iex_api.snapshot_params(first, last))
        parse_failures = 0
        try:
            parsed = parse_snapshot(response.content, market=market, start=first, end=last)
            records, parse_failures = parsed.records, len(parsed.failures)
        except EmptyResponseError:   # nothing published for any date in the window
            records = []
        live: dict[date, dict[int, Decimal]] = defaultdict(dict)
        for record in records:
            live[record.values["delivery_date"]][record.values["time_block"]] = record.values["price_inr_per_mwh"]
        stored = ctx.state.iex_mkt_prices(market.market_code, first, last)
        detections: list[Detection] = []
        not_published: list[str] = []
        late: list[str] = []
        for day in date_span(first, last):
            published = live.get(day, {})
            if not published:
                if not stored.get(day):
                    reference = day - timedelta(days=market.days_ahead)
                    hours = self.params["late_after_hours"]
                    (late if hours is not None and ctx.is_late(reference, float(hours)) else not_published).append(
                        day.isoformat())
                continue
            have = stored.get(day, {})
            new_blocks = sorted(set(published) - set(have))
            changed = sorted(b for b in set(published) & set(have) if published[b] != have[b])
            if not new_blocks and not changed:
                continue
            detections.append(Detection(
                day.isoformat(),
                version_token(sorted((b, str(p)) for b, p in published.items())),
                {"runner": "iex", "kind": self.params["market"], "start_date": day.isoformat(),
                 "end_date": day.isoformat()},
                {"blocks_published": len(published), "blocks_stored": len(have), "new_blocks": len(new_blocks),
                 "changed_blocks": len(changed), "partial": len(published) < iex_api.BLOCKS_PER_DAY,
                 "revision": bool(changed)}))
        return CheckResult.from_detections(detections, not_published=not_published, late=late, requests=1,
                                           window=f"{first}..{last}", parse_failures=parse_failures)
