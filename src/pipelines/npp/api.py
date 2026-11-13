"""NPP endpoint access: datasets, request parameters, date guards and calls.

The two endpoints are undocumented JSON behind the NPP dashboard "Real Time
Generation and Demand Data (Source-MERIT INDIA)"
(https://npp.gov.in/dashBoard/gc-map-dashboard-meritchart). Verified live on
2026-11-12:

* ``GET {NPP_BASE_URL}/demandmet1chartdata?date=YYYY-MM-DD`` - All-India demand met
* ``GET {NPP_BASE_URL}/demandmet2chartdata?date=YYYY-MM-DD`` - All-India generation by source

Each returns a JSON array of ``{"updated_on": <epoch ms>, "name_of_data":
<series>, "value_of_data": <MW>}`` for one IST calendar day: irregular
snapshots roughly every 2-4 minutes. No authentication is required. The base
URL is configurable (``NPP_BASE_URL``) in case NPP moves the endpoints.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import httpx

from src.common.exceptions import HttpStatusError
from src.common.http_client import HttpClient
from src.pipelines.npp.errors import NppApiError

IST = timezone(timedelta(hours=5, minutes=30))  # India has no DST
UNIT = "MW"  # labelled "(MW)" on the dashboard; the response carries no unit
MAX_DAYS_PER_COMMAND = 31
# Earliest day with data found on 2026-11-12 (2025-09-01 returned [], 2025-10-01 had data).
HISTORY_START = date(2025, 10, 1)


@dataclass(frozen=True)
class NppEndpoint:
    """One NPP JSON endpoint and the series it is expected to return.

    ``series`` are the ``name_of_data`` values observed on 2026-11-12. They are
    stored verbatim; any other name is rejected as ``UNKNOWN_SERIES`` so a new
    or renamed series is noticed rather than silently loaded.
    """

    dataset_code: str
    path: str
    series: tuple[str, ...]


DEMAND = NppEndpoint("NPP_DEMAND", "demandmet1chartdata", ("DEMAND MET",))
# The composition of THERMAL GENERATION (coal/lignite only, or more) is not
# documented by NPP or MERIT India; GAS and NUCLEAR are reported separately.
GENERATION = NppEndpoint(
    "NPP_GENERATION", "demandmet2chartdata",
    ("THERMAL GENERATION", "HYDRO GENERATION", "GAS GENERATION", "NUCLEAR GENERATION", "SOLAR GENERATION",
     "WIND GENERATION"),
)


def endpoint_url(base_url: str, endpoint: NppEndpoint) -> str:
    return f"{base_url.rstrip('/')}/{endpoint.path}"


def request_params(day: date) -> dict[str, str]:
    """Always send an explicit date: without one NPP silently returns today."""
    return {"date": day.isoformat()}


def today_ist() -> date:
    return datetime.now(IST).date()


def validate_date_range(start: date, end: date, *, today: date, max_days: int = MAX_DAYS_PER_COMMAND) -> None:
    """Guard against invalid, future, pre-history or oversized requests.

    Raises:
        ValueError: With a message suitable for the command line.
    """
    if end < start:
        raise ValueError(f"end date {end} is before start date {start}")
    if end > today:
        raise ValueError(f"{end} is in the future (today is {today} IST); NPP returns no data for future dates")
    if start < HISTORY_START:
        raise ValueError(f"{start} is before {HISTORY_START}, the earliest date NPP was found to serve")
    days = (end - start).days + 1
    if days > max_days:
        raise ValueError(f"Requested {days} days; at most {max_days} days are allowed per command")


def date_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


@dataclass(frozen=True)
class ApiResponse:
    """A response exactly as received."""

    content: bytes
    content_type: str | None
    retrieved_at: datetime
    url: str


def call_api(client: HttpClient, url: str, params: dict[str, str]) -> ApiResponse:
    """GET one NPP endpoint. Transient failures are retried by ``client``.

    Raises:
        NppApiError: If NPP returns an error status.
        HttpRequestError: If the request could not be completed after retries.
    """
    try:
        response = client.get(url, params=params)
    except HttpStatusError as exc:
        raise NppApiError(f"NPP returned HTTP {exc.status_code} for {exc.url}: {exc.response_text[:200]}") from exc
    return ApiResponse(
        content=response.content,
        content_type=response.headers.get("content-type"),
        retrieved_at=datetime.now(timezone.utc),
        url=str(httpx.URL(url, params=params)),
    )
