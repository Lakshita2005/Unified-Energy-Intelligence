"""WBES errors. Messages never contain tokens or response bodies of the auth endpoint."""

from __future__ import annotations

LOGIN_COMMAND = "python -m scripts.ingest_wbes login"


class WbesError(Exception):
    """Base class for WBES errors."""


class LoginNeeded(WbesError):
    """No usable WBES session: a person must log in interactively (OTP + CAPTCHA)."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"WBES login required ({reason}). Run: {LOGIN_COMMAND}")
        self.reason = reason


class WbesSessionError(WbesError):
    """The session file is unusable (malformed, or in a forbidden location)."""


class WbesSessionLocked(WbesError):
    """Another process holds the WBES session lock for this utility."""


class WbesResponseError(WbesError):
    """A WBES response was unsuccessful (ResponseStatus) or structurally invalid."""
