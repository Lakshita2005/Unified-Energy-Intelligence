"""Project-wide exception hierarchy.

Catch :class:`PriceIntelligenceError` to handle any error raised deliberately by
this codebase; catch the narrower subclasses to handle specific failure modes.
"""

from __future__ import annotations


class PriceIntelligenceError(Exception):
    """Base class for all errors raised by this project."""


class ConfigurationError(PriceIntelligenceError):
    """Required configuration is missing or invalid."""


class DatabaseError(PriceIntelligenceError):
    """A database operation or connectivity check failed."""


class StorageError(PriceIntelligenceError):
    """Reading from or writing to raw storage failed."""


class IngestionError(PriceIntelligenceError):
    """An ingestion run cannot proceed (e.g. inactive dataset, invalid run state)."""


class HttpClientError(PriceIntelligenceError):
    """Base class for HTTP client failures.

    Attributes:
        method: HTTP method of the failed request.
        url: Request URL with the query string removed (safe to log).
        attempts: Number of attempts made before giving up.
    """

    def __init__(
        self,
        message: str,
        *,
        method: str | None = None,
        url: str | None = None,
        attempts: int = 0,
    ) -> None:
        super().__init__(message)
        self.method = method
        self.url = url
        self.attempts = attempts


class HttpRequestError(HttpClientError):
    """The request could not be completed (connection error, timeout, etc.)."""


class HttpStatusError(HttpClientError):
    """The server returned an error status code (4xx/5xx).

    Attributes:
        status_code: HTTP status code returned by the server.
        response_text: Truncated response body, useful for diagnostics.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        response_text: str = "",
        method: str | None = None,
        url: str | None = None,
        attempts: int = 0,
    ) -> None:
        super().__init__(message, method=method, url=url, attempts=attempts)
        self.status_code = status_code
        self.response_text = response_text


class HttpResponseDecodeError(HttpClientError):
    """The response body could not be decoded (e.g. invalid JSON)."""
