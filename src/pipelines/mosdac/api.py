"""MOSDAC energy API client.

Endpoints (supplied endpoint information; no key or login expected)::

    GET https://mosdac.gov.in/apienergy/solar?lat=<latitude>&lon=<longitude>
    GET https://mosdac.gov.in/apienergy/wind?lat=<latitude>&lon=<longitude>

Locations are passed in by the caller (``config/mosdac_locations.toml``); none is built in here.

Reliability comes from :class:`~src.common.http_client.HttpClient`: connect/read timeouts from
``HTTP_*`` settings, and a limited number of retries (``HTTP_MAX_RETRIES``) with exponential backoff
for transport failures (connection errors including SSL EOF, timeouts) and retryable statuses
(408, 425, 429, 500, 502, 503, 504). After the last attempt the error is raised; nothing retries
forever. A 4xx other than those is not retried.

Observed live (2026-11-17, ESEPL): solar returned HTTP 200 ``application/json``, a JSON list of
objects ``{"time": "13-11-26 0530", "temp": "16.91", "flux": "0.0"}`` with string values; wind
returned HTTP 500 ``{"status": "Internal Server Error", ...}``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal

from config.settings import Settings
from src.common.http_client import HttpClient
from src.pipelines.mosdac.errors import MosdacResponseError

Kind = Literal["solar", "wind"]
KINDS: tuple[Kind, ...] = ("solar", "wind")


@dataclass(frozen=True)
class MosdacResponse:
    content: bytes            # body exactly as received
    url: str                  # public URL including lat/lon (no credentials exist)
    status_code: int
    content_type: str | None
    retrieved_at: datetime


def decode(content: bytes, what: str) -> Any:
    """Decode a response body as JSON with exact numbers. Raises MosdacResponseError if it is not JSON."""
    try:
        return json.loads(content, parse_float=Decimal)
    except ValueError:
        snippet = content[:80].decode("utf-8", "replace")
        raise MosdacResponseError(f"{what}: response is not valid JSON (starts {snippet!r})") from None


class MosdacClient:
    def __init__(self, http: HttpClient, settings: Settings) -> None:
        self.http = http
        self.base_url = settings.mosdac_api_base_url.rstrip("/")

    def url(self, kind: Kind) -> str:
        return f"{self.base_url}/{kind}"

    def get(self, kind: Kind, latitude: str, longitude: str) -> MosdacResponse:
        """One GET (with HttpClient's bounded retries). Raises HttpStatusError / HttpRequestError on failure."""
        if kind not in KINDS:
            raise ValueError(f"unknown MOSDAC endpoint {kind!r}")
        params = {"lat": latitude, "lon": longitude}
        response = self.http.get(self.url(kind), params=params, headers={"Accept": "application/json"})
        return MosdacResponse(response.content, str(response.url), response.status_code,
                              response.headers.get("content-type"), datetime.now(timezone.utc))
