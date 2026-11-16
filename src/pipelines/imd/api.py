"""IMD API client (documented endpoints only).

Endpoints (https://api.imd.gov.in/public/api_reference.html):

* ``GET /aws_data`` (optional ``?id=<call sign>`` or ``?sid=<state id 1-36>``): AWS/ARG observations;
* ``GET /aws_data_mapping``: AWS station mapping (fields not documented);
* ``GET /cityforecast`` (optional ``?id=<station code>``): 7-day city forecast.

Access: IMD approval and IP whitelisting. A key is sent only if both ``IMD_API_KEY`` and
``IMD_API_KEY_HEADER`` are configured (no auth mechanism is assumed). HTTP 401/403 raise
:class:`ImdAccessDenied` and are never retried or worked around. Transient errors (5xx, 429,
timeouts) are retried by :class:`HttpClient` with backoff.

Response container: the reference shows record fields but not the top-level structure, so only a
JSON **list of objects** is accepted; anything else raises :class:`ImdResponseError` (the raw body
has already been stored). JSON numbers are decoded exactly (``Decimal``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from config.settings import Settings
from src.common.exceptions import HttpStatusError
from src.common.http_client import HttpClient
from src.pipelines.imd.errors import ImdAccessDenied, ImdResponseError

AWS_DATA_PATH = "/aws_data"
AWS_MAPPING_PATH = "/aws_data_mapping"
CITY_FORECAST_PATH = "/cityforecast"


@dataclass(frozen=True)
class ImdResponse:
    content: bytes
    url: str                 # public URL (query parameters only: id/sid; never credentials)
    content_type: str | None
    retrieved_at: datetime


def decode_records(content: bytes, what: str) -> list[dict[str, Any]]:
    """The records of a response: a JSON list of objects (exact numbers). Raises ImdResponseError otherwise."""
    try:
        payload = json.loads(content, parse_float=Decimal)
    except ValueError:
        raise ImdResponseError(f"{what}: response is not JSON") from None
    if not isinstance(payload, list):
        raise ImdResponseError(f"{what}: expected a JSON list of records, got {type(payload).__name__} "
                               "(the top-level structure is not documented; not guessed)")
    if not all(isinstance(item, dict) for item in payload):
        raise ImdResponseError(f"{what}: the list contains non-object items")
    return payload


class ImdClient:
    def __init__(self, http: HttpClient, settings: Settings) -> None:
        self.http = http
        self.base_url = settings.imd_api_base_url.rstrip("/")
        self.headers: dict[str, str] = {"Accept": "application/json"}
        if settings.imd_api_key is not None and settings.imd_api_key_header:
            self.headers[settings.imd_api_key_header] = settings.imd_api_key.get_secret_value()

    def get(self, path: str, params: dict[str, Any] | None = None) -> ImdResponse:
        url = self.base_url + path
        try:
            response = self.http.get(url, params=params or None, headers=self.headers)
        except HttpStatusError as exc:
            if exc.status_code in (401, 403):
                raise ImdAccessDenied(f"IMD refused GET {path} with HTTP {exc.status_code}: access not approved or "
                                      "this IP is not whitelisted (requires IMD approval; not worked around)") from None
            raise
        query = "&".join(f"{k}={v}" for k, v in (params or {}).items())
        return ImdResponse(response.content, url + (f"?{query}" if query else ""),
                           response.headers.get("content-type"), datetime.now(timezone.utc))
