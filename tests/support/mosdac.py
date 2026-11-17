"""Synthetic MOSDAC responses and a mocked MOSDAC API (no real MOSDAC data, no network).

Structure follows the live solar response observed 2026-11-17: a JSON list of objects with string
values, ``time`` as ``DD-MM-YY HHMM`` (IST), 289 points at 15-minute spacing. Values are artificial
(three decimals, non-zero) so they never coincide with real records. Series start in 2030 so they
are not "stale" relative to the test clock unless a test says so.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import httpx

API = "https://mosdac-api.test/apienergy"
START = datetime(2030, 1, 10, 5, 30)        # naive IST


def times(n: int = 289, start: datetime = START) -> list[str]:
    return [(start + timedelta(minutes=15 * i)).strftime("%d-%m-%y %H%M") for i in range(n)]


def solar_records(n: int = 289, start: datetime = START, offset: float = 0.0) -> list[dict[str, Any]]:
    return [{"time": t, "temp": f"{18.117 + 0.013 * i + offset:.3f}", "flux": f"{(i % 96) * 7.137 + 0.111:.3f}"}
            for i, t in enumerate(times(n, start))]


def wind_records(n: int = 289, start: datetime = START, offset: float = 0.0) -> list[dict[str, Any]]:
    return [{"time": t, "speed": f"{2.317 + 0.011 * i + offset:.3f}", "direction": f"{(i * 7.311) % 359 + 0.113:.3f}"}
            for i, t in enumerate(times(n, start))]


def body(payload: Any) -> bytes:
    return json.dumps(payload).encode()


@dataclass
class MockMosdac:
    """httpx handler for /solar and /wind. Queue items: payload, bytes, httpx.Response or an exception."""

    responses: dict[str, list[Any]] = field(default_factory=dict)   # "solar" / "wind" -> queue
    calls: list[str] = field(default_factory=list)                  # "solar?lat=..&lon=.."

    def set(self, kind: str, *items: Any) -> None:
        self.responses[kind] = list(items)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        kind = request.url.path.rsplit("/", 1)[-1]
        self.calls.append(f"{kind}?{request.url.query.decode()}")
        queue = self.responses.get(kind)
        if not queue:
            return httpx.Response(404, json={"error": "not mocked"})
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if callable(item) and not isinstance(item, (httpx.Response, BaseException)):
            item = item(request)
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, httpx.Response):
            return item
        content = item if isinstance(item, bytes) else body(item)
        return httpx.Response(200, content=content, headers={"content-type": "application/json"})


def server_error(request: httpx.Request | None = None) -> httpx.Response:
    """A fresh HTTP 500 like the live wind response (queue the function, not a shared Response)."""
    return httpx.Response(500, json={"status": "Internal Server Error",
                                     "message": "The server encountered an internal error"})
