"""Builders for mocked Open-Meteo responses (test-only; no live API).

Coordinates used here are synthetic test values, not real sites.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import httpx

from src.pipelines.open_meteo.api import HOURLY_VARIABLES

TEST_LATITUDE = Decimal("10.0000000")
TEST_LONGITUDE = Decimal("20.0000000")

DEFAULT_TIMES = ["2026-11-12T00:00", "2026-11-12T01:00", "2026-11-12T02:00"]
DEFAULT_VALUES: dict[str, list[Any]] = {
    "temperature_2m": [21.4, 20.9, 20.5],
    "shortwave_radiation": [0.0, 0.0, 0.0],
    "direct_radiation": [0.0, 0.0, 0.0],
    "diffuse_radiation": [0.0, 0.0, 0.0],
    "wind_speed_10m": [2.35, 2.1, 1.8],
    "cloud_cover": [12, 40, 100],
    "relative_humidity_2m": [61, 64, 66],
    "precipitation": [0.0, 0.0, 0.2],
}


def response_dict(
    times: list[str] | None = None,
    values: dict[str, list[Any]] | None = None,
    *,
    timezone: str = "Asia/Kolkata",
    offset: int = 19800,
    generation_ms: float = 0.05,
) -> dict[str, Any]:
    """An Open-Meteo-shaped single-location hourly response."""
    times = list(DEFAULT_TIMES if times is None else times)
    series = {name: list(vals) for name, vals in DEFAULT_VALUES.items()}
    if values:
        series.update(values)
    return {
        "latitude": float(TEST_LATITUDE),
        "longitude": float(TEST_LONGITUDE),
        "generationtime_ms": generation_ms,
        "utc_offset_seconds": offset,
        "timezone": timezone,
        "timezone_abbreviation": "GMT+5:30",
        "elevation": 100.0,
        "hourly_units": {"time": "iso8601", **{v.api_name: v.unit for v in HOURLY_VARIABLES}},
        "hourly": {"time": times, **{name: vals[: len(times)] for name, vals in series.items()}},
    }


def response_bytes(*args: Any, **kwargs: Any) -> bytes:
    return json.dumps(response_dict(*args, **kwargs), ensure_ascii=False).encode("utf-8")


class MockOpenMeteo:
    """httpx transport handler serving queued responses and recording requests."""

    def __init__(self, *responses: httpx.Response | bytes | Exception | Callable[[httpx.Request], httpx.Response]):
        self.queue = list(responses)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        item = self.queue.pop(0) if len(self.queue) > 1 else self.queue[0]
        if isinstance(item, Exception):
            raise item
        if callable(item) and not isinstance(item, httpx.Response):
            return item(request)
        if isinstance(item, bytes):
            return httpx.Response(200, content=item, headers={"content-type": "application/json; charset=utf-8"})
        return item
