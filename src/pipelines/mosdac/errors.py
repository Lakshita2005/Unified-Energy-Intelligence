"""MOSDAC errors."""

from __future__ import annotations

from src.common.exceptions import ConfigurationError


class MosdacError(Exception):
    """Base class for MOSDAC errors."""


class MosdacResponseError(MosdacError):
    """A MOSDAC response is not in the observed structure (never guessed). The raw body is already stored."""


class MosdacLocationError(ConfigurationError):
    """A MOSDAC location is missing, invalid, or conflicts with rows already stored."""
