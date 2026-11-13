"""Reusable synchronous HTTP client with timeouts, retries and error handling.

:class:`HttpClient` wraps :class:`httpx.Client` and adds:

* connect/read timeouts from settings;
* retries with exponential backoff and jitter on transport errors and
  retryable status codes (408, 425, 429, 5xx), honouring ``Retry-After``;
* retries only for idempotent methods by default (override per call);
* typed exceptions (:mod:`src.common.exceptions`) instead of raw httpx errors;
* logging that never includes query strings or headers, which often carry
  API keys.

Usage::

    with HttpClient(base_url="https://api.example.com", headers={"Authorization": ...}) as client:
        data = client.get_json("/prices", params={"date": "2026-01-01"})
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Collection, Mapping
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from types import TracebackType
from typing import Any

import httpx

from config.settings import Settings, get_settings
from src.common.exceptions import (
    HttpRequestError,
    HttpResponseDecodeError,
    HttpStatusError,
)
from src.common.logging_config import get_logger

logger = get_logger(__name__)

DEFAULT_RETRY_STATUS_CODES: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})
IDEMPOTENT_METHODS: frozenset[str] = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})
_MAX_ERROR_BODY_CHARS = 500


def parse_retry_after(value: str | None) -> float | None:
    """Parse a ``Retry-After`` header (seconds or HTTP-date) into seconds.

    Returns:
        Non-negative seconds to wait, or None if the header is absent/invalid.
    """
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        seconds = (retry_at - datetime.now(timezone.utc)).total_seconds()
    return max(0.0, seconds)


class HttpClient:
    """HTTP client with retry, backoff and consistent error handling.

    Args:
        base_url: Prefix for relative request URLs.
        headers: Default headers sent with every request.
        timeout_seconds: Overall read/write/pool timeout (default ``HTTP_TIMEOUT_SECONDS``).
        connect_timeout_seconds: Connect timeout (default ``HTTP_CONNECT_TIMEOUT_SECONDS``).
        max_retries: Retries after the first attempt (default ``HTTP_MAX_RETRIES``).
        backoff_factor_seconds: Base delay; attempt *n* waits ``factor * 2**(n-1)`` plus jitter.
        backoff_max_seconds: Upper bound on any single wait, including ``Retry-After``.
        retry_status_codes: Status codes that trigger a retry.
        retry_methods: Methods retried by default.
        verify_ssl: Verify TLS certificates (default ``HTTP_VERIFY_SSL``).
        settings: Settings instance; defaults to the global settings.
        transport: Custom httpx transport (e.g. ``httpx.MockTransport`` in tests).
        sleep: Sleep function, injectable for tests.
    """

    def __init__(
        self,
        base_url: str = "",
        *,
        headers: Mapping[str, str] | None = None,
        timeout_seconds: float | None = None,
        connect_timeout_seconds: float | None = None,
        max_retries: int | None = None,
        backoff_factor_seconds: float | None = None,
        backoff_max_seconds: float | None = None,
        retry_status_codes: Collection[int] = DEFAULT_RETRY_STATUS_CODES,
        retry_methods: Collection[str] = IDEMPOTENT_METHODS,
        verify_ssl: bool | None = None,
        settings: Settings | None = None,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        settings = settings or get_settings()

        self.max_retries = settings.http_max_retries if max_retries is None else max_retries
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        self.backoff_factor_seconds = (
            settings.http_backoff_factor_seconds if backoff_factor_seconds is None else backoff_factor_seconds
        )
        self.backoff_max_seconds = (
            settings.http_backoff_max_seconds if backoff_max_seconds is None else backoff_max_seconds
        )
        self.retry_status_codes = frozenset(retry_status_codes)
        self.retry_methods = frozenset(method.upper() for method in retry_methods)
        self._sleep = sleep

        timeout = httpx.Timeout(
            timeout_seconds if timeout_seconds is not None else settings.http_timeout_seconds,
            connect=(
                connect_timeout_seconds
                if connect_timeout_seconds is not None
                else settings.http_connect_timeout_seconds
            ),
        )
        default_headers = {"User-Agent": settings.http_user_agent}
        default_headers.update(headers or {})

        self._client = httpx.Client(
            base_url=base_url,
            headers=default_headers,
            timeout=timeout,
            verify=settings.http_verify_ssl if verify_ssl is None else verify_ssl,
            follow_redirects=True,
            transport=transport,
        )

    # ------------------------------------------------------------------ public

    def request(
        self,
        method: str,
        url: str,
        *,
        retry: bool | None = None,
        raise_for_status: bool = True,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send a request, retrying transient failures.

        Args:
            method: HTTP method.
            url: Absolute URL, or path relative to ``base_url``.
            retry: Force retries on/off. Defaults to on for idempotent methods only.
            raise_for_status: Raise :class:`HttpStatusError` for 4xx/5xx responses.
            **kwargs: Passed to :meth:`httpx.Client.request` (``params``, ``json``,
                ``data``, ``headers``, ``timeout`` ...).

        Raises:
            HttpRequestError: Transport failure after all attempts.
            HttpStatusError: Error status after all attempts (if ``raise_for_status``).
        """
        method = method.upper()
        should_retry = (method in self.retry_methods) if retry is None else retry
        max_attempts = self.max_retries + 1 if should_retry else 1
        safe_url = self._safe_url(url)

        for attempt in range(1, max_attempts + 1):
            started = time.perf_counter()
            try:
                response = self._client.request(method, url, **kwargs)
            except httpx.TransportError as exc:
                reason = f"{type(exc).__name__}: {exc}"
                if attempt < max_attempts:
                    delay = self._backoff_delay(attempt)
                    logger.warning(
                        "HTTP %s %s failed (%s); retrying in %.2fs [attempt %d/%d]",
                        method, safe_url, reason, delay, attempt, max_attempts,
                    )
                    self._sleep(delay)
                    continue
                logger.error(
                    "HTTP %s %s failed after %d attempt(s): %s", method, safe_url, attempt, reason
                )
                raise HttpRequestError(
                    f"{method} {safe_url} failed after {attempt} attempt(s): {reason}",
                    method=method,
                    url=safe_url,
                    attempts=attempt,
                ) from exc

            elapsed_ms = (time.perf_counter() - started) * 1000
            logger.debug(
                "HTTP %s %s -> %d (%.0f ms)", method, safe_url, response.status_code, elapsed_ms
            )

            if response.status_code in self.retry_status_codes and attempt < max_attempts:
                retry_after = parse_retry_after(response.headers.get("Retry-After"))
                delay = (
                    min(retry_after, self.backoff_max_seconds)
                    if retry_after is not None
                    else self._backoff_delay(attempt)
                )
                logger.warning(
                    "HTTP %s %s returned %d; retrying in %.2fs [attempt %d/%d]",
                    method, safe_url, response.status_code, delay, attempt, max_attempts,
                )
                response.close()
                self._sleep(delay)
                continue

            if raise_for_status and response.is_error:
                body = response.text[:_MAX_ERROR_BODY_CHARS]
                logger.error(
                    "HTTP %s %s returned %d after %d attempt(s)",
                    method, safe_url, response.status_code, attempt,
                )
                raise HttpStatusError(
                    f"{method} {safe_url} returned HTTP {response.status_code}",
                    status_code=response.status_code,
                    response_text=body,
                    method=method,
                    url=safe_url,
                    attempts=attempt,
                )
            return response

        raise AssertionError("unreachable")  # pragma: no cover

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        """Send a GET request. See :meth:`request`."""
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        """Send a POST request (not retried unless ``retry=True``). See :meth:`request`."""
        return self.request("POST", url, **kwargs)

    def get_json(self, url: str, **kwargs: Any) -> Any:
        """Send a GET request and decode the JSON body.

        Raises:
            HttpResponseDecodeError: If the body is not valid JSON.
        """
        response = self.get(url, **kwargs)
        try:
            return response.json()
        except ValueError as exc:
            safe_url = self._safe_url(url)
            raise HttpResponseDecodeError(
                f"GET {safe_url} returned a non-JSON body", method="GET", url=safe_url
            ) from exc

    def close(self) -> None:
        """Close the underlying connection pool."""
        self._client.close()

    def __enter__(self) -> HttpClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # ----------------------------------------------------------------- helpers

    def _backoff_delay(self, attempt: int) -> float:
        """Exponential backoff with up to 25% jitter, capped at ``backoff_max_seconds``."""
        base = self.backoff_factor_seconds * (2 ** (attempt - 1))
        jitter = random.uniform(0, base * 0.25)
        return min(self.backoff_max_seconds, base + jitter)

    def _safe_url(self, url: str) -> str:
        """Return the URL without query string or fragment, for logging."""
        target = httpx.URL(url)
        if target.is_relative_url:
            base = str(self._client.base_url.copy_with(query=None, fragment=None)).rstrip("/")
            path = target.copy_with(query=None, fragment=None)
            return f"{base}/{str(path).lstrip('/')}" if base else str(path)
        return str(target.copy_with(query=None, fragment=None))
