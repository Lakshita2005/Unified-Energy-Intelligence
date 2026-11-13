"""CEA pipeline errors. A document-level error fails the run (nothing of that document is loaded)."""

from __future__ import annotations

from src.common.exceptions import IngestionError


class CeaError(IngestionError):
    """Base class for CEA source errors."""


class CeaHttpError(CeaError):
    """The publisher returned an error status (other than 404 = not published)."""


class InvalidResponseError(CeaError):
    """The file is not the expected report (HTML page, wrong title, missing headers, unsupported layout)."""
