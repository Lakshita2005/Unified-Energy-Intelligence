"""Failure classification and exponential backoff.

Classification decides what happens after a failed check or job attempt:

* ``TRANSIENT`` (network errors, timeouts, HTTP 408/425/429/5xx): retried with exponential backoff, at most
  ``max_attempts`` times per job. The shared HTTP client already retries inside each attempt.
* ``NOT_PUBLISHED`` (HTTP 404 for a dated file, an empty IEX response, no listing entry): not an error;
  the item is simply detected again on a later check.
* ``PERMANENT`` (unexpected format, parser/validation errors, invalid arguments, configuration): not
  retried; the item is quarantined until someone requeues it (e.g. after a parser fix).
"""

from __future__ import annotations

import re

import httpx

from src.common.exceptions import ConfigurationError, HttpRequestError, HttpStatusError
from src.scheduler.models import ErrorClass

TRANSIENT_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})

# Exception class names raised by the pipelines that mean "not published (yet)".
_NOT_PUBLISHED_NAMES = frozenset({"EmptyResponseError"})
# Exception class names that mean "the source changed or the input is invalid": no retry.
_PERMANENT_NAMES = frozenset({
    "InvalidResponseError", "InvalidWorkbookError", "MosdacResponseError", "MosdacLocationError",
    "LocationError", "ConfigurationError", "ValueError", "TypeError", "KeyError",
})
_TRANSIENT_NAMES = frozenset({
    "HttpRequestError", "TimeoutException", "ConnectError", "ReadTimeout", "ConnectTimeout", "RemoteProtocolError",
    "OperationalError", "TimeoutError", "ConnectionError",
})
_HTTP_STATUS = re.compile(r"\bHTTP (\d{3})\b")


def classify_exception(exc: BaseException) -> ErrorClass:
    """Classify an exception raised by a checker or runner."""
    if isinstance(exc, HttpStatusError):
        return _classify_status(exc.status_code)
    if isinstance(exc, (HttpRequestError, httpx.TransportError, TimeoutError, ConnectionError)):
        return ErrorClass.TRANSIENT
    if isinstance(exc, ConfigurationError):
        return ErrorClass.PERMANENT
    return classify_error_text(f"{type(exc).__name__}: {exc}")


def classify_error_text(text: str | None) -> ErrorClass:
    """Classify an error recorded as text (``IngestionResult.error`` is ``"<ExceptionName>: <message>"``)."""
    if not text:
        return ErrorClass.TRANSIENT
    name = text.split(":", 1)[0].strip()
    if name in _NOT_PUBLISHED_NAMES or "not published" in text.lower():
        return ErrorClass.NOT_PUBLISHED
    match = _HTTP_STATUS.search(text)
    if match:
        return _classify_status(int(match.group(1)))
    if name in _TRANSIENT_NAMES or any(word in text.lower() for word in ("timed out", "timeout", "connection reset",
                                                                        "temporarily unavailable")):
        return ErrorClass.TRANSIENT
    if name in _PERMANENT_NAMES:
        return ErrorClass.PERMANENT
    # Unknown errors are treated as permanent: retrying an unknown bug would only repeat it.
    return ErrorClass.PERMANENT


def _classify_status(status: int) -> ErrorClass:
    if status in TRANSIENT_STATUS or status >= 500:
        return ErrorClass.TRANSIENT
    if status == 404:
        return ErrorClass.NOT_PUBLISHED
    return ErrorClass.PERMANENT


def backoff_seconds(attempt: int, *, base: float, maximum: float) -> float:
    """Delay before retry number ``attempt`` (1 = first retry): ``base * 2 ** (attempt - 1)``, capped."""
    if attempt < 1:
        return 0.0
    return float(min(maximum, base * (2 ** (attempt - 1))))
