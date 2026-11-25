"""Parse IEX market-snapshot and area-price responses into price records.

Pure: no network or database access.

* Snapshot records give the system-wide unconstrained MCP (``area_code = 'MKT'``):
  ``mcp`` for DAM and RTM, ``unconstrained_m_c_p`` for GDAM. The same records'
  volumes and congestion flag are parsed separately by
  :func:`parse_snapshot_volumes` for ``pi.iex_market_snapshot``.
* Area-price records give one bid area's clearing price, which differs from
  the MCP in congested blocks.

Delivery blocks: ``period`` (``"00:00 - 00:15"``, or ``"00:00-00:15"`` for RTM)
or ``timeperiod`` (block start, ``"00:00"``) is converted to a time block
1-96 as ``start_minutes / 15 + 1``; block 1 is 00:00-00:15 IST.

Values arrive as strings (DAM/RTM snapshot) or JSON numbers (GDAM snapshot,
area prices). Both are parsed exactly as ``Decimal``, never float.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from src.ingestion.models import ParsedRecord, ParseFailure, ParseResult
from src.pipelines.iex.api import GDAM, GDAM_SOURCES, MKT_AREA, Market
from src.pipelines.iex.errors import EmptyResponseError, IexApiError, InvalidResponseError

_PERIOD = re.compile(r"^(\d{2}):(\d{2})\s*-\s*(\d{2}):(\d{2})$")
_TIME = re.compile(r"^(\d{2}):(\d{2})$")
_DECIMAL_TEXT = re.compile(r"^-?\d+(\.\d+)?$")


def _envelope(content: bytes) -> list[Any]:
    try:
        payload = json.loads(content, parse_float=Decimal)
    except (UnicodeDecodeError, ValueError) as exc:
        raise InvalidResponseError(f"Response is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict) or "statusCode" not in payload or "data" not in payload:
        raise InvalidResponseError("Response is not an IEX envelope with 'statusCode' and 'data'")
    if payload["statusCode"] != 200:
        raise IexApiError(f"IEX statusCode {payload['statusCode']}: {payload.get('message')}")
    data = payload["data"]
    if not isinstance(data, list):
        raise InvalidResponseError(f"'data' is {type(data).__name__}, expected a list")
    if not data:
        raise EmptyResponseError(
            "IEX returned no records (data: []) - not yet published, or no data for the requested dates")
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            raise InvalidResponseError(f"Item {index} is {type(item).__name__}, expected an object")
    return data


def _price(value: Any) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"price {value!r} is not numeric")
    if isinstance(value, (int, Decimal)):
        return Decimal(value)
    if isinstance(value, str) and _DECIMAL_TEXT.fullmatch(value.strip()):
        return Decimal(value.strip())
    raise ValueError(f"price {value!r} is not numeric")


def _delivery_date(value: Any) -> date:
    if not isinstance(value, str):
        raise ValueError(f"date {value!r} is not a DD-MM-YYYY string")
    return datetime.strptime(value, "%d-%m-%Y").date()


def _block_from_period(value: Any) -> int:
    match = _PERIOD.fullmatch(value.strip()) if isinstance(value, str) else None
    if not match:
        raise ValueError(f"period {value!r} is not 'HH:MM - HH:MM'")
    h1, m1, h2, m2 = map(int, match.groups())
    start, end = h1 * 60 + m1, h2 * 60 + m2
    if start % 15 or not 0 <= start < 1440 or end != start + 15:
        raise ValueError(f"period {value!r} is not a 15-minute block")
    return start // 15 + 1


def _block_from_start(value: Any) -> int:
    match = _TIME.fullmatch(value.strip()) if isinstance(value, str) else None
    if not match:
        raise ValueError(f"timeperiod {value!r} is not 'HH:MM'")
    hour, minute = map(int, match.groups())
    start = hour * 60 + minute
    if start % 15 or not 0 <= start < 1440:
        raise ValueError(f"timeperiod {value!r} is not the start of a 15-minute block")
    return start // 15 + 1


def _parse_items(
    data: list[dict[str, Any]],
    *,
    market: Market,
    area: str,
    start: date,
    end: date,
    expected_fields: Callable[[date], frozenset[str]],
    block_field: str,
    to_block: Callable[[Any], int],
    measures: Callable[[dict[str, Any]], dict[str, Any]],
) -> ParseResult:
    result = ParseResult()
    for item in data:
        key = f"{market.market_code}|{area}|{item.get('date')}|{item.get(block_field)}"
        try:
            delivery_date = _delivery_date(item.get("date"))
        except ValueError as exc:
            result.failures.append(ParseFailure(key, item, str(exc)))
            continue
        fields = expected_fields(delivery_date)
        if set(item) != fields:
            raise InvalidResponseError(
                f"{market.market_code} {area} record for {delivery_date} has fields {sorted(item)}; expected "
                f"{sorted(fields)}. The API may have changed.")
        if not start <= delivery_date <= end:
            result.failures.append(ParseFailure(key, item, f"delivery date {delivery_date} is outside the requested "
                                                           f"range {start}..{end}"))
            continue
        try:
            block = to_block(item[block_field])
            values = measures(item)
        except ValueError as exc:
            result.failures.append(ParseFailure(key, item, str(exc)))
            continue
        result.records.append(ParsedRecord(key=f"{market.market_code}|{area}|{delivery_date}|{block}", raw=item,
                                           values={"market_code": market.market_code, "delivery_date": delivery_date,
                                                   "time_block": block, **values}))
    return result


def _price_values(area: str, price_field: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    # The API states no publication time.
    return lambda item: {"area_code": area, "price_inr_per_mwh": _price(item[price_field]),
                         "source_published_at": None}


def _congestion(value: Any) -> bool:
    if value == "YES":
        return True
    if value == "NO":
        return False
    raise ValueError(f"congestion {value!r} is not 'YES' or 'NO'")


def _volume(item: dict[str, Any], name: str) -> Decimal:
    try:
        return _price(item[name])
    except ValueError:
        raise ValueError(f"{name} {item[name]!r} is not numeric") from None


_GDAM_TOTALS = {"sell_bid_mw": "total_sell_bid", "mcv_mw": "total_unconstrained_m_c_v",
                "final_scheduled_volume_mw": "total_cleared_volume"}
_DAM_RTM_VOLUMES = {"sell_bid_mw": "sell_bid", "mcv_mw": "mcv", "final_scheduled_volume_mw": "final_scheduled_volume"}
_GDAM_BREAKDOWN = {"sell_bid": "sell_bid_mw", "unconstrained_m_c_v": "mcv_mw", "cleared_volume": "cleared_volume_mw"}

SNAPSHOT_BREAKDOWN_COLUMNS: tuple[str, ...] = tuple(
    f"{source}_{column}" for source in GDAM_SOURCES for column in _GDAM_BREAKDOWN.values())


def _snapshot_values(market: Market) -> Callable[[dict[str, Any]], dict[str, Any]]:
    gdam = market.market_code == GDAM.market_code

    def values(item: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {"purchase_bid_mw": _volume(item, "purchase_bid")}
        result |= {column: _volume(item, name) for column, name in (_GDAM_TOTALS if gdam else _DAM_RTM_VOLUMES).items()}
        result["congestion"] = _congestion(item["congestion"])
        result |= dict.fromkeys(SNAPSHOT_BREAKDOWN_COLUMNS)
        if gdam:
            for source in GDAM_SOURCES:
                for kind, column in _GDAM_BREAKDOWN.items():
                    if f"{source}_{kind}" in item:
                        result[f"{source}_{column}"] = _volume(item, f"{source}_{kind}")
        return result

    return values


def parse_snapshot(content: bytes, *, market: Market, start: date, end: date) -> ParseResult:
    """Parse a ``market-snapshot`` response into MCP records (``area_code = 'MKT'``).

    Raises:
        IexApiError: ``statusCode`` other than 200.
        InvalidResponseError: Not an envelope, or records without exactly the
            fields expected for their delivery date (two GDAM field sets).
        EmptyResponseError: ``data`` is empty.
    """
    return _parse_items(_envelope(content), market=market, area=MKT_AREA, start=start, end=end,
                        expected_fields=market.fields_for, block_field="period", to_block=_block_from_period,
                        measures=_price_values(MKT_AREA, market.price_field))


def parse_snapshot_volumes(content: bytes, *, market: Market, start: date, end: date) -> ParseResult:
    """Parse a ``market-snapshot`` response into volume and congestion records (``pi.iex_market_snapshot``).

    DAM/RTM ``sell_bid``, ``mcv``, ``final_scheduled_volume`` and GDAM ``total_sell_bid``,
    ``total_unconstrained_m_c_v``, ``total_cleared_volume`` share the columns ``sell_bid_mw``, ``mcv_mw``,
    ``final_scheduled_volume_mw``. GDAM per-source fields go to ``<source>_sell_bid_mw``, ``<source>_mcv_mw``
    and ``<source>_cleared_volume_mw``; sources a delivery date does not publish are NULL. The price is not
    repeated here (it is in ``pi.iex_market_price``, area ``MKT``).

    Raises:
        The same errors as :func:`parse_snapshot`.
    """
    return _parse_items(_envelope(content), market=market, area=MKT_AREA, start=start, end=end,
                        expected_fields=market.fields_for, block_field="period", to_block=_block_from_period,
                        measures=_snapshot_values(market))


def parse_area_prices(content: bytes, *, market: Market, area: str, start: date, end: date) -> ParseResult:
    """Parse an ``area-price`` response for one bid area.

    Records hold ``date``, ``timeperiod`` (block start) and the price under the
    lower-cased area code, e.g. ``{"date": "10-11-2026", "timeperiod": "00:00", "n1": 3114.65}``.
    """
    fields = frozenset({"date", "timeperiod", area.lower()})
    return _parse_items(_envelope(content), market=market, area=area, start=start, end=end,
                        expected_fields=lambda _: fields, block_field="timeperiod", to_block=_block_from_start,
                        measures=_price_values(area, area.lower()))
