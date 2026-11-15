"""WBES API client: bearer-token requests with safe token refresh.

Behaviour (assessment, section 5):

* the access token (JWT) is short-lived; before each call it is refreshed
  when less than ``WBES_REFRESH_MARGIN_SECONDS`` remain (or its expiry is
  unreadable);
* refresh: ``POST /auth/api/Auth/refresh-token`` with the current access
  token as bearer and ``{"Token": <refresh token>, "UtilityId": <id>}``; the
  response carries a new ``Token`` and a new ``RefreshToken``. The refresh
  token rotates, so the new pair is saved atomically *before* it is used;
* a 401/403 on a data call triggers one refresh and one retry;
* a refused refresh (non-200, ``IsExpired``, or missing tokens) raises
  :class:`LoginNeeded`. No browser is ever opened here: a person must run the
  interactive login command.

Network failures of the refresh call are raised as-is (they are transient and
do not prove the session is dead). The refresh response body is never logged
or included in an error message, because it contains tokens.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import urlsplit

from config.settings import Settings
from src.common.http_client import HttpClient
from src.common.logging_config import get_logger
from src.pipelines.wbes.errors import LoginNeeded, WbesResponseError
from src.pipelines.wbes.session import SessionTokens, TokenStore

logger = get_logger(__name__)

REFRESH_PATH = "/auth/api/Auth/refresh-token"


@dataclass(frozen=True)
class WbesResponse:
    """A data response exactly as received, plus its decoded JSON (floats kept as Decimal)."""

    content: bytes
    payload: Any
    url: str
    content_type: str | None
    retrieved_at: datetime
    status_code: int


def decode_json(content: bytes) -> Any:
    """Decode JSON keeping every number exact (floats as ``Decimal``)."""
    return json.loads(content, parse_float=Decimal)


class WbesClient:
    def __init__(self, store: TokenStore, *, settings: Settings, http_client: HttpClient | None = None,
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> None:
        self.store = store
        self.settings = settings
        self.base_url = settings.wbes_api_base_url.rstrip("/")
        portal = urlsplit(settings.wbes_portal_url)
        self._origin = f"{portal.scheme}://{portal.netloc}"
        self._owns_http = http_client is None
        self.http = http_client or HttpClient(settings=settings)
        self._clock = clock
        self._tokens: SessionTokens | None = None

    # ------------------------------------------------------------- session
    @property
    def tokens(self) -> SessionTokens:
        if self._tokens is None:
            loaded = self.store.load()
            if loaded is None:
                raise LoginNeeded("no saved WBES session")
            self._tokens = loaded
        return self._tokens

    @property
    def utility_id(self) -> str:
        return self.tokens.utility_id

    def _headers(self, access_token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {access_token}", "Accept": "application/json",
                "Content-Type": "application/json", "Origin": self._origin, "Referer": self._origin + "/"}

    def ensure_fresh(self) -> None:
        expires = self.tokens.access_expires_at
        margin = timedelta(seconds=self.settings.wbes_refresh_margin_seconds)
        if expires is not None and expires - self._clock() > margin:
            return
        self.refresh()

    def refresh(self) -> None:
        """Rotate the token pair and persist it atomically. Raises LoginNeeded if WBES refuses."""
        current = self.tokens
        response = self.http.post(self.base_url + REFRESH_PATH, headers=self._headers(current.access_token),
                                  json={"Token": current.refresh_token, "UtilityId": current.utility_id},
                                  raise_for_status=False)
        if response.status_code != 200:
            raise LoginNeeded(f"session refresh refused with HTTP {response.status_code}")
        try:
            body = response.json()
        except ValueError:
            raise LoginNeeded("session refresh returned a non-JSON response") from None
        if not isinstance(body, dict) or body.get("IsExpired") or not body.get("Token") or not body.get("RefreshToken"):
            raise LoginNeeded("WBES reports the session as expired")
        rotated = current.rotated(str(body["Token"]), str(body["RefreshToken"]), self._clock())
        self.store.save(rotated)          # persist first: the previous refresh token is now invalid
        self._tokens = rotated
        logger.info("WBES session refreshed (access token valid until %s)",
                    rotated.access_expires_at.isoformat() if rotated.access_expires_at else "unknown")

    # ---------------------------------------------------------------- data
    def request(self, method: str, path: str, *, body: dict[str, Any] | None = None) -> WbesResponse:
        """Authenticated data call. Read-only POSTs are retried on transient errors like GETs.

        Raises:
            LoginNeeded: No session, or WBES refused the session after one refresh.
            WbesResponseError: A non-auth HTTP error status after retries, or a non-JSON body.
        """
        url = self.base_url + path
        self.ensure_fresh()
        for attempt in (1, 2):
            response = self.http.request(method, url, headers=self._headers(self.tokens.access_token), json=body,
                                         retry=True, raise_for_status=False)
            if response.status_code in (401, 403):
                if attempt == 1:
                    self.refresh()
                    continue
                raise LoginNeeded(f"HTTP {response.status_code} on {path} after a token refresh")
            if response.is_error:
                raise WbesResponseError(f"{method} {path} returned HTTP {response.status_code}")
            retrieved_at = self._clock()
            try:
                payload = decode_json(response.content)
            except ValueError:
                raise WbesResponseError(f"{method} {path} returned a non-JSON body") from None
            return WbesResponse(response.content, payload, url, response.headers.get("content-type"), retrieved_at,
                                response.status_code)
        raise AssertionError("unreachable")  # pragma: no cover

    def close(self) -> None:
        if self._owns_http:
            self.http.close()
