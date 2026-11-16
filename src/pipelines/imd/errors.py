"""IMD errors."""

from __future__ import annotations


class ImdError(Exception):
    """Base class for IMD errors."""


class ImdAccessDenied(ImdError):
    """IMD refused the request (HTTP 401/403): access not approved, or this IP is not whitelisted."""


class ImdResponseError(ImdError):
    """An IMD response is not in a recognised structure (never guessed)."""
