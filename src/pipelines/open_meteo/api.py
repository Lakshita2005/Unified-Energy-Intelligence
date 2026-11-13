"""Open-Meteo API access: endpoints, variables, request parameters and calls.

Endpoints and parameters follow the official documentation
(https://open-meteo.com/en/docs and /en/docs/historical-weather-api),
verified 2026-11-12:

* Forecast:   https://api.open-meteo.com/v1/forecast        (``forecast_days`` 0-16)
* Historical: https://archive-api.open-meteo.com/v1/archive (``start_date``/``end_date``)

Commercial use requires an API key and the ``customer-`` host prefix; when
``OPEN_METEO_API_KEY`` is set, both are applied automatically. The key is sent
as a query parameter and is never logged or stored (``public_url`` omits it).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import httpx

from src.common.exceptions import HttpStatusError
from src.common.http_client import HttpClient
from src.pipelines.open_meteo.errors import OpenMeteoApiError

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Requests use IST so date ranges are Indian calendar days. India has no DST,
# so the response's utc_offset_seconds is always +19800 (+05:30).
REQUEST_TIMEZONE = "Asia/Kolkata"
MAX_FORECAST_DAYS = 16
HISTORICAL_CHUNK_DAYS = 31


@dataclass(frozen=True)
class HourlyVariable:
    """An Open-Meteo hourly variable and the column that stores it.

    Attributes:
        api_name: Variable name in the API.
        column: Destination column in ``pi.open_meteo_weather``.
        unit: Unit the API must report in ``hourly_units`` (validated).
        aggregation: How the API defines the hourly value.
        minimum, maximum: Plausible physical range; values outside are rejected.
    """

    api_name: str
    column: str
    unit: str
    aggregation: str
    minimum: Decimal
    maximum: Decimal


HOURLY_VARIABLES: tuple[HourlyVariable, ...] = (
    HourlyVariable("temperature_2m", "temperature_2m_c", "°C", "instantaneous", Decimal("-90"), Decimal("60")),
    HourlyVariable("shortwave_radiation", "shortwave_radiation_wm2", "W/m²", "preceding-hour mean (GHI)",
                   Decimal("0"), Decimal("1500")),
    HourlyVariable("direct_radiation", "direct_radiation_wm2", "W/m²", "preceding-hour mean",
                   Decimal("0"), Decimal("1500")),
    HourlyVariable("diffuse_radiation", "diffuse_radiation_wm2", "W/m²", "preceding-hour mean",
                   Decimal("0"), Decimal("1500")),
    HourlyVariable("wind_speed_10m", "wind_speed_10m_ms", "m/s", "instantaneous", Decimal("0"), Decimal("120")),
    HourlyVariable("cloud_cover", "cloud_cover_pct", "%", "instantaneous", Decimal("0"), Decimal("100")),
    HourlyVariable("relative_humidity_2m", "relative_humidity_2m_pct", "%", "instantaneous",
                   Decimal("0"), Decimal("100")),
    HourlyVariable("precipitation", "precipitation_mm", "mm", "preceding-hour sum", Decimal("0"), Decimal("500")),
)

# Units are requested explicitly rather than relying on API defaults.
_UNIT_PARAMS = {"temperature_unit": "celsius", "wind_speed_unit": "ms", "precipitation_unit": "mm"}


def endpoint(base_url: str, api_key: str | None) -> str:
    """Return the endpoint, switched to the ``customer-`` host when an API key is used."""
    if not api_key:
        return base_url
    url = httpx.URL(base_url)
    return str(url.copy_with(host=f"customer-{url.host}"))


def _base_params(latitude: Decimal, longitude: Decimal) -> dict[str, Any]:
    return {
        "latitude": str(latitude),
        "longitude": str(longitude),
        "hourly": ",".join(variable.api_name for variable in HOURLY_VARIABLES),
        "timezone": REQUEST_TIMEZONE,
        "timeformat": "iso8601",
        **_UNIT_PARAMS,
    }


def validate_forecast_days(forecast_days: int) -> None:
    """Raise ValueError unless 1 <= ``forecast_days`` <= 16."""
    if not 1 <= forecast_days <= MAX_FORECAST_DAYS:
        raise ValueError(f"forecast_days must be between 1 and {MAX_FORECAST_DAYS}, got {forecast_days}")


def forecast_params(latitude: Decimal, longitude: Decimal, forecast_days: int) -> dict[str, Any]:
    """Parameters for one forecast request (starting today, IST).

    Raises:
        ValueError: If ``forecast_days`` is outside 1-16.
    """
    validate_forecast_days(forecast_days)
    return {**_base_params(latitude, longitude), "forecast_days": forecast_days, "past_days": 0}


def historical_params(latitude: Decimal, longitude: Decimal, start: date, end: date) -> dict[str, Any]:
    """Parameters for one historical request covering ``start``..``end`` (inclusive, IST days)."""
    return {**_base_params(latitude, longitude), "start_date": start.isoformat(), "end_date": end.isoformat()}


def validate_historical_range(start: date, end: date, *, today: date, max_days: int) -> None:
    """Guard against invalid or unrestricted historical requests.

    Raises:
        ValueError: If the range is reversed, in the future, or longer than ``max_days``.
    """
    if end < start:
        raise ValueError(f"end_date {end} is before start_date {start}")
    if end > today:
        raise ValueError(f"end_date {end} is in the future (today is {today} IST)")
    days = (end - start).days + 1
    if days > max_days:
        raise ValueError(f"Requested {days} days, more than the limit of {max_days}. "
                         "Pass a larger --max-days deliberately for a backfill.")


def split_date_range(start: date, end: date, chunk_days: int = HISTORICAL_CHUNK_DAYS) -> list[tuple[date, date]]:
    """Split an inclusive date range into consecutive chunks of at most ``chunk_days``."""
    chunks = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=chunk_days - 1))
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


@dataclass(frozen=True)
class ApiResponse:
    """An API response exactly as received.

    ``public_url`` is the request URL without the API key, safe to store.
    """

    content: bytes
    content_type: str | None
    retrieved_at: datetime
    public_url: str


def _error_reason(body: str) -> str:
    try:
        payload = json.loads(body)
    except ValueError:
        return body[:200] or "no response body"
    if isinstance(payload, dict) and payload.get("reason"):
        return str(payload["reason"])
    return body[:200]


def call_api(client: HttpClient, url: str, params: dict[str, Any], api_key: str | None = None) -> ApiResponse:
    """GET an Open-Meteo endpoint. Transient failures are retried by ``client``.

    Raises:
        OpenMeteoApiError: If the API returns an error status (e.g. 400 with a reason).
        HttpRequestError: If the request could not be completed after retries.
    """
    request_params = dict(params)
    if api_key:
        request_params["apikey"] = api_key
    try:
        response = client.get(url, params=request_params)
    except HttpStatusError as exc:
        raise OpenMeteoApiError(
            f"Open-Meteo returned HTTP {exc.status_code} for {exc.url}: {_error_reason(exc.response_text)}"
        ) from exc
    return ApiResponse(
        content=response.content,
        content_type=response.headers.get("content-type"),
        retrieved_at=datetime.now(timezone.utc),
        public_url=str(httpx.URL(url, params=params)),
    )
