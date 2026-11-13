"""IEX API access: markets, bid areas, request parameters, date guards and calls.

The endpoints are the undocumented JSON API used by the iexindia.com market
data pages (identified 2026-11-12; the same snapshot endpoint is used by
``IEX_Price_Forecasting/iex_data_feeds``). No authentication is required:

* ``GET {IEX_API_BASE_URL}/{dam|gdam|rtm}/market-snapshot?interval=ONE_FOURTH_HOUR&fromDate=DD-MM-YYYY&toDate=DD-MM-YYYY``
* ``GET {IEX_API_BASE_URL}/{dam|gdam}/area-price?interval=ONE_FOURTH_HOUR&fromDate=...&toDate=...&bidArea=<area>``

Both return ``{"statusCode": 200, "message": ..., "data": [...]}`` and accept a
date range in one request. ``rtm/area-price`` returned no rows for every bid
area (2026-11-10, a day with congested RTM blocks), so RTM area prices are not
requested. The base URL is configurable (``IEX_API_BASE_URL``).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import httpx

from src.common.exceptions import HttpStatusError
from src.common.http_client import HttpClient
from src.pipelines.iex.errors import IexApiError

IST = timezone(timedelta(hours=5, minutes=30))  # India has no DST
INTERVAL = "ONE_FOURTH_HOUR"
BLOCKS_PER_DAY = 96
MAX_DAYS_PER_COMMAND = 31
MKT_AREA = "MKT"  # the system-wide (unconstrained) market clearing price

# The 13 IEX bid areas; each returned 96 blocks for DAM and GDAM on 2026-11-10.
BID_AREAS: tuple[str, ...] = ("A1", "A2", "E1", "E2", "N1", "N2", "N3", "S1", "S2", "S3", "W1", "W2", "W3")


@dataclass(frozen=True)
class Market:
    """One IEX market.

    Attributes:
        dataset_code: Catalogue dataset.
        market_code: ``pi.market_master.market_code``.
        path: API path segment.
        price_field: Snapshot field holding the unconstrained MCP (Rs/MWh).
        snapshot_fields: Accepted snapshot field sets as ``(first delivery date, fields)``, latest last.
        area_prices: Whether bid-area prices are fetched.
        history_start: Earliest delivery date with 15-minute data.
        days_ahead: How far beyond today delivery dates may be requested
            (DAM/GDAM results for tomorrow are published on the afternoon of today).
    """

    dataset_code: str
    market_code: str
    path: str
    price_field: str
    snapshot_fields: tuple[tuple[date, frozenset[str]], ...]
    area_prices: bool
    history_start: date
    days_ahead: int

    def fields_for(self, delivery_date: date) -> frozenset[str]:
        """The snapshot fields expected for ``delivery_date``."""
        expected = self.snapshot_fields[0][1]
        for first_day, fields in self.snapshot_fields:
            if delivery_date >= first_day:
                expected = fields
        return expected


_DAM_RTM_FIELDS = frozenset({"date", "period", "purchase_bid", "sell_bid", "mcv", "final_scheduled_volume", "mcp",
                             "congestion"})


def _gdam_fields(sources: tuple[str, ...]) -> frozenset[str]:
    per_source = {f"{s}_{kind}" for s in sources for kind in ("sell_bid", "unconstrained_m_c_v", "cleared_volume")}
    return frozenset({"date", "period", "purchase_bid", "total_sell_bid", "total_unconstrained_m_c_v",
                      "total_cleared_volume", "unconstrained_m_c_p", "congestion", *per_source})


# GDAM's volume breakdown changed with delivery date 2026-07-08 (verified on
# 2026-07-07 and 2026-07-08; the IEX site has "gDamSummaryHeadersAfter8July2026").
GDAM_FIELDS_CHANGE = date(2026, 7, 8)

DAM = Market("IEX_DAM", "IEX_DAM", "dam", "mcp", ((date(2000, 1, 1), _DAM_RTM_FIELDS),), True,
             date(2015, 1, 1), 1)
GDAM = Market("IEX_GDAM", "IEX_GDAM", "gdam", "unconstrained_m_c_p",
              ((date(2000, 1, 1), _gdam_fields(("solar", "non_solar", "hydro"))),
               (GDAM_FIELDS_CHANGE, _gdam_fields(("hydro", "wind", "other_re", "dre")))),
              True, date(2021, 10, 27), 1)
RTM = Market("IEX_RTM", "IEX_RTM", "rtm", "mcp", ((date(2000, 1, 1), _DAM_RTM_FIELDS),), False,
             date(2020, 6, 1), 0)
MARKETS: dict[str, Market] = {"dam": DAM, "gdam": GDAM, "rtm": RTM}

# Price limits (Rs/MWh) by delivery date, as recorded (verified against CERC
# orders) in IEX_Price_Forecasting/scripts/prepare_datasets.py. Prices above
# the cap in force are flagged, not rejected; prices outside [0, ABSOLUTE_MAX]
# are rejected.
PRICE_CAPS: tuple[tuple[date, Decimal], ...] = (
    (date(2020, 1, 1), Decimal("20000")),
    (date(2022, 4, 3), Decimal("12000")),
    (date(2023, 4, 3), Decimal("10000")),
)
PRICE_FLOOR = Decimal("0")
ABSOLUTE_MAX_PRICE = Decimal("20000")


def price_cap(delivery_date: date) -> Decimal | None:
    """The price cap in force on ``delivery_date`` (None before the first known cap)."""
    cap = None
    for first_day, value in PRICE_CAPS:
        if delivery_date >= first_day:
            cap = value
    return cap


def today_ist() -> date:
    return datetime.now(IST).date()


def api_date(day: date) -> str:
    return day.strftime("%d-%m-%Y")


def snapshot_url(base_url: str, market: Market) -> str:
    return f"{base_url.rstrip('/')}/{market.path}/market-snapshot"


def area_price_url(base_url: str, market: Market) -> str:
    return f"{base_url.rstrip('/')}/{market.path}/area-price"


def snapshot_params(start: date, end: date) -> dict[str, str]:
    return {"interval": INTERVAL, "fromDate": api_date(start), "toDate": api_date(end)}


def area_price_params(start: date, end: date, area: str) -> dict[str, str]:
    return {**snapshot_params(start, end), "bidArea": area}


def validate_date_range(market: Market, start: date, end: date, *, today: date,
                        max_days: int = MAX_DAYS_PER_COMMAND) -> None:
    """Guard against invalid, unpublished, pre-history or oversized requests.

    Raises:
        ValueError: With a message suitable for the command line.
    """
    if end < start:
        raise ValueError(f"end date {end} is before start date {start}")
    latest = today + timedelta(days=market.days_ahead)
    if end > latest:
        raise ValueError(f"{end} is after {latest}, the latest {market.market_code} delivery date that can be "
                         f"published by today ({today} IST)")
    if start < market.history_start:
        raise ValueError(f"{start} is before {market.history_start}, the earliest {market.market_code} 15-minute data")
    days = (end - start).days + 1
    if days > max_days:
        raise ValueError(f"Requested {days} days; at most {max_days} days are allowed per command")


@dataclass(frozen=True)
class ApiResponse:
    """A response exactly as received."""

    content: bytes
    content_type: str | None
    retrieved_at: datetime
    url: str


def call_api(client: HttpClient, url: str, params: dict[str, str]) -> ApiResponse:
    """GET one IEX endpoint. Timeouts and transient failures are retried by ``client``.

    Raises:
        IexApiError: If IEX returns an error status.
        HttpRequestError: If the request could not be completed after retries.
    """
    try:
        response = client.get(url, params=params)
    except HttpStatusError as exc:
        raise IexApiError(f"IEX returned HTTP {exc.status_code} for {exc.url}: {exc.response_text[:200]}") from exc
    return ApiResponse(
        content=response.content,
        content_type=response.headers.get("content-type"),
        retrieved_at=datetime.now(timezone.utc),
        url=str(httpx.URL(url, params=params)),
    )
