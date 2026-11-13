"""Builders for mocked IEX API responses (test-only; no live API).

The SHAPES mirror the live responses captured 2026-11-12: the envelope,
field names, DAM/RTM numbers as strings, GDAM and area prices as JSON
numbers, RTM periods without spaces, and both GDAM field sets. All VALUES
are synthetic: no IEX content is stored in the repository (the same policy
as Resolven-Operation-Intelligence's ``tests/pi_iex_json.py``).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date
from decimal import Decimal
from typing import Any

import httpx

DAY = date(2026, 11, 10)


def api_date(day: date) -> str:
    return day.strftime("%d-%m-%Y")


def period(block: int, *, spaced: bool = True) -> str:
    start = (block - 1) * 15
    end = start + 15
    text = f"{start // 60:02d}:{start % 60:02d}", f"{end // 60:02d}:{end % 60:02d}"
    return f"{text[0]} - {text[1]}" if spaced else f"{text[0]}-{text[1]}"


def price_for(block: int, offset: Decimal = Decimal("0")) -> Decimal:
    """A synthetic, deterministic price with 2 decimals."""
    return Decimal(3000 + block * 10) + Decimal(block % 7) / 100 + offset


def dam_record(day: date, block: int, *, price: Any = None, spaced: bool = True, **override: Any) -> dict[str, Any]:
    value = price_for(block) if price is None else price
    record = {"date": api_date(day), "period": period(block, spaced=spaced), "purchase_bid": "10000.00",
              "sell_bid": "15000.00", "mcv": "4400.00", "final_scheduled_volume": "4400.00",
              "mcp": f"{value:.2f}" if isinstance(value, Decimal) else value, "congestion": "NO"}
    record.update(override)
    return record


def gdam_record(day: date, block: int, *, price: Any = None, era: str = "after") -> dict[str, Any]:
    sources = ("hydro", "wind", "other_re", "dre") if era == "after" else ("solar", "non_solar", "hydro")
    record: dict[str, Any] = {"date": api_date(day), "period": period(block), "purchase_bid": 2665.7,
                              "total_sell_bid": 410.9, "total_unconstrained_m_c_v": 385.19,
                              "total_cleared_volume": 385.19}
    for source in sources:
        record.update({f"{source}_sell_bid": 10, f"{source}_unconstrained_m_c_v": 9.5, f"{source}_cleared_volume": 9.5})
    record["unconstrained_m_c_p"] = float(price_for(block) + 1000) if price is None else price
    record["congestion"] = "NO"
    return record


def area_record(day: date, block: int, area: str, *, price: Any = None) -> dict[str, Any]:
    start = (block - 1) * 15
    return {"date": api_date(day), "timeperiod": f"{start // 60:02d}:{start % 60:02d}",
            area.lower(): float(price_for(block) - 50) if price is None else price}


def envelope(records: list[dict[str, Any]], *, status: int = 200, message: str = "fetched successfully") -> bytes:
    return json.dumps({"statusCode": status, "message": message, "data": records}).encode("utf-8")


def snapshot(market: str, days: list[date] | None = None, blocks: range = range(1, 97), **kwargs: Any) -> bytes:
    records = []
    for day in days or [DAY]:
        for block in blocks:
            if market == "gdam":
                records.append(gdam_record(day, block, **kwargs))
            else:
                records.append(dam_record(day, block, spaced=market != "rtm", **kwargs))
    return envelope(records)


def area_prices(area: str, days: list[date] | None = None, blocks: range = range(1, 97)) -> bytes:
    return envelope([area_record(day, block, area) for day in days or [DAY] for block in blocks])


Responder = Callable[[httpx.Request], httpx.Response]


def router(snapshot_body: bytes, area_body: Callable[[str], bytes] | None = None) -> Responder:
    """Serve ``snapshot_body`` for market-snapshot and ``area_body(area)`` for area-price requests."""

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/market-snapshot"):
            return httpx.Response(200, content=snapshot_body, headers={"content-type": "application/json"})
        area = request.url.params["bidArea"]
        body = area_body(area) if area_body else area_prices(area)
        return httpx.Response(200, content=body, headers={"content-type": "application/json"})

    return respond


class MockIex:
    """httpx transport handler that records requests."""

    def __init__(self, responder: Responder) -> None:
        self.responder = responder
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self.responder(request)
