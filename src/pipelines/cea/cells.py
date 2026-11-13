"""Reading CEA .xls reports into a cell grid, and converting cell values.

Values are converted to :class:`~decimal.Decimal` and quantized to the scale
of the destination column, so a re-published, unchanged report compares equal
to what is stored (xlrd returns binary floats such as ``10.283639999999998``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

import xlrd

from src.pipelines.cea.api import IST
from src.pipelines.cea.errors import InvalidResponseError


@dataclass(frozen=True)
class Grid:
    """First worksheet of a workbook: ``rows[r][c]`` are xlrd values (str or float)."""

    rows: list[list[Any]]
    datemode: int = 0

    def cell(self, row: int, col: int) -> Any:
        values = self.rows[row]
        return values[col] if 0 <= col < len(values) else ""

    def text(self, row: int, col: int) -> str:
        return clean(self.cell(row, col))


def read_grid(content: bytes) -> Grid:
    """Parse BIFF .xls bytes. Raises InvalidResponseError if it is not a readable workbook."""
    try:
        book = xlrd.open_workbook(file_contents=content, on_demand=True)
        sheet = book.sheet_by_index(0)
        rows = [sheet.row_values(r) for r in range(sheet.nrows)]
        return Grid(rows, book.datemode)
    except Exception as exc:  # xlrd raises several unrelated types
        raise InvalidResponseError(f"not a readable .xls workbook: {type(exc).__name__}: {exc}") from exc


def clean(value: Any) -> str:
    """Cell text with whitespace (including newlines) collapsed."""
    if isinstance(value, float):
        return format_number_label(value)
    return re.sub(r"\s+", " ", str(value)).strip()


def norm(value: Any) -> str:
    """Normalised name used for matching (upper case, single spaces)."""
    return clean(value).upper()


def format_number_label(value: float) -> str:
    """Unit numbers stored as floats (6.0) become "6"."""
    return str(int(value)) if value == int(value) else repr(value)


def is_blank(value: Any) -> bool:
    return clean(value) == ""


def is_number(value: Any) -> bool:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return True
    return re.fullmatch(r"-?[\d,]+(\.\d+)?%?", clean(value)) is not None


def to_decimal(value: Any, scale: int) -> Decimal | None:
    """Number cell -> Decimal rounded half-up to ``scale`` places; blank -> None.

    Accepts floats, and strings with thousands separators (Indian grouping) or
    a trailing ``%``. Raises ValueError for anything else.
    """
    if isinstance(value, float):
        number = Decimal(repr(value))
    else:
        text = clean(value).replace(",", "")
        if text == "":
            return None
        text = text[:-1] if text.endswith("%") else text
        try:
            number = Decimal(text)
        except InvalidOperation as exc:
            raise ValueError(f"not a number: {value!r}") from exc
    if not number.is_finite():
        raise ValueError(f"not a finite number: {value!r}")
    return number.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)


_DATETIME_FORMATS = ("%d/%m/%Y %H:%M", "%d/%m/%Y %H.%M.%S", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H.%M", "%d/%m/%Y")


def to_ist_datetime(value: Any, datemode: int = 0) -> datetime | None:
    """IST timestamp from a ``DD/MM/YYYY[ HH24:MI]`` string or an Excel date serial; blank -> None.

    Raises ValueError if the value cannot be read.
    """
    if isinstance(value, float):
        return xlrd.xldate_as_datetime(value, datemode).replace(tzinfo=IST)
    text = clean(value)
    if not text:
        return None
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=IST)
        except ValueError:
            continue
    raise ValueError(f"unrecognised date/time {text!r}")


def find_report_date(grid: Grid, pattern: str, *, rows: int = 8, fmt: str = "%d-%m-%Y") -> date:
    """First match of ``pattern`` (one group with the date) in the first ``rows`` rows."""
    for r in range(min(rows, len(grid.rows))):
        for value in grid.rows[r]:
            match = re.search(pattern, clean(value), flags=re.IGNORECASE)
            if match:
                try:
                    return datetime.strptime(match.group(1), fmt).date()
                except ValueError as exc:
                    raise InvalidResponseError(f"invalid report date {match.group(1)!r}") from exc
    raise InvalidResponseError(f"report date not found (pattern {pattern!r})")
