"""Parse Open-Meteo hourly responses into records. Pure: no network or database access.

Timestamp convention: requests use ``timezone=Asia/Kolkata``, so ``hourly.time``
holds IST wall-clock times without an offset. The response's
``utc_offset_seconds`` (fixed for the whole response) is attached to make
each ``valid_time`` timezone-aware; PostgreSQL stores it as an absolute instant
(``timestamptz``). ``valid_time`` is the API timestamp unchanged; for
preceding-hour variables (radiation, precipitation) it is the *end* of the hour.

Values are parsed as ``Decimal`` (never float) so they are kept exactly as
published. ``null`` measurements stay ``None`` and are stored as SQL NULL.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from src.ingestion.models import ParsedRecord, ParseFailure, ParseResult
from src.pipelines.open_meteo.api import HOURLY_VARIABLES, REQUEST_TIMEZONE, HourlyVariable
from src.pipelines.open_meteo.errors import EmptyResponseError, InvalidResponseError, OpenMeteoApiError


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, Decimal)) and not isinstance(value, bool)


def parse_hourly_response(
    content: bytes,
    *,
    weather_location_id: int,
    location_code: str,
    is_forecast: bool,
    expected_timezone: str = REQUEST_TIMEZONE,
    variables: tuple[HourlyVariable, ...] = HOURLY_VARIABLES,
) -> ParseResult:
    """Parse one single-location hourly response.

    Returns:
        Records keyed ``"<location_code>|<API time>"``. Hours with an unparseable
        timestamp or non-numeric value become :class:`ParseFailure` entries.

    Raises:
        OpenMeteoApiError: The body is an Open-Meteo error object.
        InvalidResponseError: The response structure, units or timezone are wrong.
        EmptyResponseError: The response is valid but contains no hours.
    """
    try:
        data = json.loads(content, parse_float=Decimal)
    except (UnicodeDecodeError, ValueError) as exc:
        raise InvalidResponseError(f"Response is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise InvalidResponseError(f"Expected a JSON object for one location, got {type(data).__name__}")
    if data.get("error"):
        raise OpenMeteoApiError(f"Open-Meteo error: {data.get('reason', 'no reason given')}")

    if data.get("timezone") != expected_timezone:
        raise InvalidResponseError(f"Response timezone {data.get('timezone')!r}, expected {expected_timezone!r}")
    offset = data.get("utc_offset_seconds")
    if not isinstance(offset, int) or isinstance(offset, bool):
        raise InvalidResponseError(f"Missing or invalid utc_offset_seconds: {offset!r}")

    hourly = data.get("hourly")
    units = data.get("hourly_units")
    if not isinstance(hourly, dict) or not isinstance(units, dict):
        raise InvalidResponseError("Response has no 'hourly' and 'hourly_units' objects")
    times = hourly.get("time")
    if not isinstance(times, list):
        raise InvalidResponseError("Response 'hourly.time' is missing or not a list")
    if units.get("time") != "iso8601":
        raise InvalidResponseError(f"Unexpected time format {units.get('time')!r}, expected 'iso8601'")
    for variable in variables:
        series = hourly.get(variable.api_name)
        if not isinstance(series, list):
            raise InvalidResponseError(f"Variable {variable.api_name!r} is missing from the response")
        if len(series) != len(times):
            raise InvalidResponseError(
                f"Variable {variable.api_name!r} has {len(series)} values for {len(times)} timestamps"
            )
        if units.get(variable.api_name) != variable.unit:
            raise InvalidResponseError(
                f"Variable {variable.api_name!r} is in {units.get(variable.api_name)!r}, expected {variable.unit!r}"
            )
    if not times:
        raise EmptyResponseError("Response contains no hourly timestamps")

    tzinfo = timezone(timedelta(seconds=offset))
    result = ParseResult()
    for index, time_text in enumerate(times):
        raw = {"time": time_text, **{v.api_name: hourly[v.api_name][index] for v in variables}}
        key = f"{location_code}|{time_text}"
        try:
            local_time = datetime.fromisoformat(time_text)
            if local_time.tzinfo is not None:
                raise ValueError("timestamp unexpectedly carries its own offset")
        except (TypeError, ValueError) as exc:
            result.failures.append(ParseFailure(key, raw, f"Invalid timestamp {time_text!r}: {exc}"))
            continue

        non_numeric = [v.api_name for v in variables if raw[v.api_name] is not None and not _is_number(raw[v.api_name])]
        if non_numeric:
            result.failures.append(ParseFailure(key, raw, f"Non-numeric value(s) for {non_numeric}"))
            continue

        values: dict[str, Any] = {
            "weather_location_id": weather_location_id,
            "valid_time": local_time.replace(tzinfo=tzinfo),
            "is_forecast": is_forecast,
            # The API does not expose the model initialisation time, so the
            # forecast issue time is unknown. It is never derived from valid or
            # retrieval time; forecast vintages are identified by retrieved_at.
            "forecast_issued_at": None,
        }
        values.update({v.column: raw[v.api_name] for v in variables})
        result.records.append(ParsedRecord(key=key, values=values, raw=raw))
    return result
