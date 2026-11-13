"""Parse IEX market-snapshot and area-price responses into price records.

Pure: no network or database access.

* Snapshot records give the system-wide unconstrained MCP (``area_code = 'MKT'``):
  ``mcp`` for DAM and RTM, ``unconstrained_m_c_p`` for GDAM. Volumes and the
  congestion flag have no column in ``pi.iex_market_price``; they stay in the
  raw file.
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
from src.pipelines.iex.api import MKT_AREA, Market
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
    price_field: str,
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
            price = _price(item[price_field])
        except ValueError as exc:
            result.failures.append(ParseFailure(key, item, str(exc)))
            continue
        result.records.append(ParsedRecord(key=f"{market.market_code}|{area}|{delivery_date}|{block}", raw=item,
                                           values={
                                               "market_code": market.market_code,
                                               "area_code": area,
                                               "delivery_date": delivery_date,
                                               "time_block": block,
                                               "price_inr_per_mwh": price,
                                               # The API states no publication time.
                                               "source_published_at": None,
                                           }))
    return result


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
                        price_field=market.price_field)


def parse_area_prices(content: bytes, *, market: Market, area: str, start: date, end: date) -> ParseResult:
    """Parse an ``area-price`` response for one bid area.

    Records hold ``date``, ``timeperiod`` (block start) and the price under the
    lower-cased area code, e.g. ``{"date": "10-11-2026", "timeperiod": "00:00", "n1": 3114.65}``.
    """
    fields = frozenset({"date", "timeperiod", area.lower()})
    return _parse_items(_envelope(content), market=market, area=area, start=start, end=end,
                        expected_fields=lambda _: fields, block_field="timeperiod", to_block=_block_from_start,
                        price_field=area.lower())
