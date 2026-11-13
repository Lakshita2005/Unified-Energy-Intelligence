"""Parse NPP demand/generation responses into records. Pure: no network or database access.

Timestamps: ``updated_on`` (epoch milliseconds) is the only time the source
provides - the MERIT India snapshot time. It is stored twice: unchanged in
``source_updated_on_ms`` and as the instant ``observed_at`` (timestamptz).
Snapshots are irregular (about every 2-4 minutes) and are never rounded or
resampled to time blocks.

Values are kept exactly as published (``Decimal``, never float). Series names
are stored verbatim. The data is All-India, so ``state_code`` and
``region_code`` are NULL.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from src.ingestion.models import ParsedRecord, ParseFailure, ParseResult
from src.pipelines.npp.api import IST, UNIT
from src.pipelines.npp.errors import EmptyResponseError, InvalidResponseError

FIELDS = frozenset({"updated_on", "name_of_data", "value_of_data"})
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_NAME_MAX_LENGTH = 80  # npp_demand_generation.name_of_data varchar(80)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def parse_response(content: bytes, *, requested_date: date, expected_series: tuple[str, ...]) -> ParseResult:
    """Parse one day's response from ``demandmet1chartdata`` or ``demandmet2chartdata``.

    Returns:
        Records keyed ``"<name_of_data>|<updated_on>"``. Items with a bad
        timestamp, name or value, or a timestamp outside ``requested_date``
        (IST), become :class:`ParseFailure` entries.

    Raises:
        InvalidResponseError: Not JSON, not a list, items without exactly the
            expected fields, or none of ``expected_series`` present - i.e. the
            endpoint has changed and nothing should be loaded.
        EmptyResponseError: The response is ``[]``.
    """
    try:
        data = json.loads(content, parse_float=Decimal)
    except (UnicodeDecodeError, ValueError) as exc:
        raise InvalidResponseError(f"Response is not valid JSON: {exc}") from exc
    if not isinstance(data, list):
        raise InvalidResponseError(f"Expected a JSON array, got {type(data).__name__}")
    if not data:
        raise EmptyResponseError(
            f"NPP returned no data for {requested_date} (NPP answers [] for days without data, future dates "
            "and invalid dates alike)")
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            raise InvalidResponseError(f"Item {index} is {type(item).__name__}, expected an object")
        if set(item) != FIELDS:
            raise InvalidResponseError(
                f"Item {index} has fields {sorted(item)}, expected {sorted(FIELDS)}; the endpoint may have changed")
    names = {item["name_of_data"] for item in data if isinstance(item["name_of_data"], str)}
    if not names & set(expected_series):
        raise InvalidResponseError(
            f"None of the expected series {list(expected_series)} is present (got {sorted(names)}); "
            "wrong endpoint or the endpoint has changed")

    day_start = datetime.combine(requested_date, datetime.min.time(), IST)
    day_end = day_start + timedelta(days=1)
    result = ParseResult()
    for item in data:
        updated_on, name, value = item["updated_on"], item["name_of_data"], item["value_of_data"]
        key = f"{name}|{updated_on}"
        if not _is_int(updated_on):
            result.failures.append(ParseFailure(key, item, f"updated_on {updated_on!r} is not integer epoch ms"))
            continue
        if not isinstance(name, str) or not name.strip() or len(name) > _NAME_MAX_LENGTH:
            result.failures.append(ParseFailure(
                key, item, f"name_of_data {name!r} is not a non-empty string of at most {_NAME_MAX_LENGTH} chars"))
            continue
        if not (_is_int(value) or isinstance(value, Decimal)):
            result.failures.append(ParseFailure(key, item, f"value_of_data {value!r} is not numeric"))
            continue
        observed_at = _EPOCH + timedelta(milliseconds=updated_on)
        if not day_start <= observed_at < day_end:
            result.failures.append(ParseFailure(
                key, item, f"updated_on {observed_at.astimezone(IST).isoformat()} is outside the requested IST "
                           f"day {requested_date}"))
            continue
        result.records.append(ParsedRecord(key=key, raw=item, values={
            "state_code": None,  # All-India
            "region_code": None,
            "observed_at": observed_at,
            "name_of_data": name,
            "value_of_data": Decimal(value),
            "unit_code": UNIT,
            "source_updated_on_ms": updated_on,
        }))
    return result
