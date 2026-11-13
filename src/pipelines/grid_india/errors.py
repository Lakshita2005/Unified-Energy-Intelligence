"""Grid-India pipeline errors. A document-level error fails the section's run (nothing of that document loads)."""

from __future__ import annotations

from src.common.exceptions import IngestionError


class GridIndiaError(IngestionError):
    """Base class for Grid-India source errors."""


class GridIndiaApiError(GridIndiaError):
    """The listing API or the file CDN returned an error or an unexpected response."""


class InvalidWorkbookError(GridIndiaError):
    """The workbook is unreadable, or a required sheet, section, header or label is missing."""
