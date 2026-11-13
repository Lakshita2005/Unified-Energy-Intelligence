"""Generic retry helper for transient failures.

:class:`~src.common.http_client.HttpClient` already retries individual HTTP
requests. Use :func:`retry_call` for coarser operations, such as a pipeline's
whole ``fetch`` step that spans several requests, or for non-HTTP transports.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

from src.common.exceptions import HttpRequestError
from src.common.logging_config import get_logger

logger = get_logger(__name__)

T = TypeVar("T")

# Failures worth retrying: the network may recover. HTTP status errors are not
# included; HttpClient has already retried retryable statuses.
TRANSIENT_NETWORK_ERRORS: tuple[type[BaseException], ...] = (HttpRequestError, ConnectionError, TimeoutError)


def retry_call(
    func: Callable[[], T],
    *,
    attempts: int = 3,
    backoff_factor_seconds: float = 1.0,
    backoff_max_seconds: float = 60.0,
    retry_on: tuple[type[BaseException], ...] = TRANSIENT_NETWORK_ERRORS,
    sleep: Callable[[float], None] = time.sleep,
    description: str = "operation",
) -> T:
    """Call ``func``, retrying on ``retry_on`` exceptions with exponential backoff.

    Args:
        func: Zero-argument callable to run.
        attempts: Total attempts including the first (>= 1).
        backoff_factor_seconds: Attempt *n* waits ``factor * 2**(n-1)`` seconds.
        backoff_max_seconds: Upper bound on any single wait.
        retry_on: Exception types considered transient.
        sleep: Sleep function, injectable for tests.
        description: Used in log messages.

    Raises:
        The last exception if every attempt fails, or immediately for
        exceptions not listed in ``retry_on``.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except retry_on as exc:
            if attempt == attempts:
                logger.error("%s failed after %d attempt(s): %s", description, attempt, exc)
                raise
            delay = min(backoff_max_seconds, backoff_factor_seconds * (2 ** (attempt - 1)))
            logger.warning(
                "%s failed (%s: %s); retrying in %.2fs [attempt %d/%d]",
                description, type(exc).__name__, exc, delay, attempt, attempts,
            )
            sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover
