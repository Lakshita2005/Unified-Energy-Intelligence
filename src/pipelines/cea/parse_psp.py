"""Parser for CEA monthly Power Supply Position reports (energy and peak) and their listing pages.

Report layout (verified 2020-06 to 2026-08): a one-page bilingual PDF with a
text layer. Title "Energy|Peak Power Supply Position Report" (often
"(Revised)", sometimes "Corrigendum"), "Figures in MU net|MW net", the month,
then one line per state/system, region and All India with four numbers for the
month and, except in April, four more for April-to-month::

    requirement  supplied  not-supplied  not-supplied-%

Numbers use Indian digit grouping (``2,57,783``). Long names wrap: the name
then sits on its own line just after (or, for the last region, just before)
the line of numbers. Reports in any other layout (pre-2020 tables, or files
mislabelled in the CEA listing) are rejected by the title check.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from datetime import date
from html.parser import HTMLParser
from typing import Any

from src.ingestion.models import ParseFailure, ParsedRecord
from src.pipelines.cea.cells import to_decimal
from src.pipelines.cea.errors import InvalidResponseError
from src.pipelines.cea.geography import PSP_REGION_NAMES, STATE_REGION, normalise_name, state_code

MONTHS = ("January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
          "November", "December")
_NUMBER = re.compile(r"^-?[\d,]+(\.\d+)?$")
_MARKER = re.compile(r"\((#{1,2}|\*{1,2})\)|(?<=\s)(#{1,2}|\*{1,2})(?=\s|$)")  # "(#)" or a bare " #"
_TITLES = {"ENERGY": "ENERGY POWER SUPPLY POSITION REPORT", "PEAK": "PEAK POWER SUPPLY POSITION REPORT"}
_UNITS = {"ENERGY": ("MU", "FIGURES IN MU NET"), "PEAK": ("MW", "FIGURES IN MW NET")}
FORMER_UTS = {"DAMAN & DIU": "DD", "DADRA & NAGAR HAVELI": "DNH"}
_COLUMN_HEADER = {"ENERGY": "ENERGY REQUIREMENT", "PEAK": "PEAK DEMAND"}


# --------------------------------------------------------------------- listing

@dataclass(frozen=True)
class ListingEntry:
    report_name: str
    month: date
    uuid: str


class _RowParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[tuple[list[str], list[str]]] = []
        self._cells: list[str] | None = None
        self._cell: str | None = None
        self._links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self._cells, self._links = [], []
        elif tag in ("td", "th") and self._cells is not None:
            self._cell = ""
        elif tag == "a" and self._cells is not None:
            href = dict(attrs).get("href")
            if href:
                self._links.append(href)

    def handle_endtag(self, tag: str) -> None:
        if tag in ("td", "th") and self._cells is not None and self._cell is not None:
            self._cells.append(re.sub(r"\s+", " ", self._cell).strip())
            self._cell = None
        elif tag == "tr" and self._cells is not None:
            self.rows.append((self._cells, self._links))
            self._cells = None

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell += data


def parse_listing(content: bytes, *, listing_title: str) -> list[ListingEntry]:
    """Rows of a cea.nic.in report listing: (report name, YYYY-MM, PDF uuid).

    Raises:
        InvalidResponseError: The page has no rows for ``listing_title``.
    """
    parser = _RowParser()
    parser.feed(content.decode("utf-8", errors="replace"))
    entries = []
    for cells, links in parser.rows:
        if len(cells) < 3 or cells[1] != listing_title:
            continue
        match = re.fullmatch(r"(\d{4})-(\d{2})", cells[2])
        uuid = next((m.group(1) for link in links
                     if (m := re.search(r"/download-pdf/en/([0-9a-fA-F-]{36})", link))), None)
        if match and uuid:
            entries.append(ListingEntry(cells[1], date(int(match[1]), int(match[2]), 1), uuid.lower()))
    if not entries:
        raise InvalidResponseError(f"no '{listing_title}' rows found on the listing page")
    return entries


# ---------------------------------------------------------------------- report

def pdf_text(content: bytes) -> str:
    """Text layer of all pages. Raises InvalidResponseError if the bytes are not a readable PDF."""
    import pdfplumber  # imported lazily: only the monthly reports need it

    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as exc:  # pdfminer raises many unrelated types
        raise InvalidResponseError(f"not a readable PDF: {type(exc).__name__}: {exc}") from exc


def _clean_line(line: str) -> str:
    line = re.sub(r"\(cid:\d+\)", " ", line)
    line = re.sub(r"[ऀ-ॿ‌‍]+", " ", line)  # Devanagari (Hindi labels)
    return re.sub(r"\s+", " ", line).strip()


def _split_numbers(line: str) -> tuple[str, list[str]]:
    tokens = line.split(" ")
    numbers: list[str] = []
    while tokens and _NUMBER.match(tokens[-1]):
        numbers.insert(0, tokens.pop())
    return " ".join(tokens), numbers


def _area_name(text: str) -> tuple[str, str | None]:
    """English name and footnote markers from the label part of a line ("- / Andaman- Nicobar (#) (**)")."""
    name = text.rsplit("/", 1)[-1]
    flags = " ".join(f"({a or b})" for a, b in _MARKER.findall(name)) or None
    name = _MARKER.sub(" ", name)
    name = re.sub(r"\s+", " ", name).strip(" .-")
    return name, flags


def _has_letters(text: str) -> bool:
    return re.search(r"[A-Za-z]{2,}", text.rsplit("/", 1)[-1]) is not None


@dataclass
class PspReport:
    measure: str
    month: date
    status: str | None
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)


def classify_area(name: str, region: str | None) -> dict[str, Any] | None:
    """(area_code, area_type, state_code, region_code) for a report row name, or None if unknown."""
    key = normalise_name(name)
    if key == "ALL INDIA":
        return {"area_code": "ALL_INDIA", "area_type": "COUNTRY", "state_code": None, "region_code": None}
    if key in PSP_REGION_NAMES:
        code = PSP_REGION_NAMES[key]
        return {"area_code": code, "area_type": "REGION", "state_code": None, "region_code": code}
    if key == "DVC":
        return {"area_code": "DVC", "area_type": "SYSTEM", "state_code": None, "region_code": region}
    if key == "UT OF J&K AND LADAKH":
        return {"area_code": "JK_LA", "area_type": "SYSTEM", "state_code": None, "region_code": region}
    if key in FORMER_UTS:  # reported separately before the 2020 merger into DH
        return {"area_code": FORMER_UTS[key], "area_type": "SYSTEM", "state_code": None, "region_code": region}
    if key == "OTHERS":
        if region is None:
            return None
        return {"area_code": f"OTHERS_{region}", "area_type": "OTHER", "state_code": None, "region_code": region}
    code = state_code(name)
    if code is None:
        return None
    return {"area_code": code, "area_type": "STATE", "state_code": code, "region_code": region or STATE_REGION[code]}


def parse_psp_report(content: bytes, *, measure: str, expected_month: date) -> PspReport:
    """Monthly report -> ``cea_power_supply_position`` records.

    Raises:
        InvalidResponseError: Not a current-layout report of ``measure``, or a different month.
    """
    lines = [_clean_line(line) for line in pdf_text(content).splitlines()]
    lines = [line for line in lines if line]
    upper = [line.upper() for line in lines]
    title_line = next((line for line in upper if _TITLES[measure] in line), None)
    if title_line is None:
        other = next((m for m, t in _TITLES.items() if any(t in line for line in upper)), None)
        found = f"a {other.lower()} report" if other else f"title {lines[0][:80]!r}" if lines else "an empty page"
        raise InvalidResponseError(f"not a {measure.lower()} Power Supply Position report in the supported "
                                   f"layout (found {found})")
    unit, _ = _UNITS[measure]
    other_unit_label = next(label for m, (_, label) in _UNITS.items() if m != measure)
    if any(other_unit_label in line for line in upper) or not any(f"( {unit} )" in line for line in upper):
        raise InvalidResponseError(f"report figures are not in {unit}")
    month_match = re.search(r"(" + "|".join(MONTHS) + r")\s*,\s*(\d{4})", "\n".join(lines))
    if not month_match:
        raise InvalidResponseError("report month not found")
    month = date(int(month_match[2]), MONTHS.index(month_match[1]) + 1, 1)
    if month != expected_month:
        raise InvalidResponseError(f"report is for {month:%Y-%m}, expected {expected_month:%Y-%m}")
    status_parts = [s for s, present in (("Revised", "(REVISED)" in title_line),
                                         ("Corrigendum", any("CORRIGENDUM" in line for line in upper))) if present]
    report = PspReport(measure, month, "; ".join(status_parts) or None)

    header_index = next((i for i, line in enumerate(upper) if _COLUMN_HEADER[measure] in line), None)
    if header_index is None:
        raise InvalidResponseError(f"column header '{_COLUMN_HEADER[measure].title()}' not found")
    periods = ("MONTH", "FYTD") if upper[header_index].count(_COLUMN_HEADER[measure]) >= 2 else ("MONTH",)
    expected_numbers = 4 * len(periods)
    start = next((i for i in range(header_index, len(upper)) if f"( {unit} )" in upper[i]), header_index) + 1
    end = next((i for i in range(start, len(lines)) if lines[i].startswith(("(#)", "(*)", "Note"))), len(lines))

    parsed: list[tuple[str, list[str]]] = []
    split = [_split_numbers(line) for line in lines[start:end]]
    used: set[int] = set()
    for i, (label, numbers) in enumerate(split):
        if not numbers:
            continue
        if len(numbers) != expected_numbers:
            report.failures.append(ParseFailure(f"{month:%Y-%m}|line{start + i + 1}", lines[start + i],
                                                f"expected {expected_numbers} numbers, found {len(numbers)}"))
            continue
        if not _has_letters(label):
            # Wrapped name: on its own line after the numbers, or else before them.
            neighbours = [j for j in (i + 1, i - 1) if 0 <= j < len(split) and j not in used
                          and not split[j][1] and _has_letters(split[j][0])]
            if not neighbours:
                report.failures.append(ParseFailure(f"{month:%Y-%m}|line{start + i + 1}", lines[start + i],
                                                    "row name not found"))
                continue
            used.add(neighbours[0])
            label = split[neighbours[0]][0]
        parsed.append((label, numbers))

    # Rows belong to the region whose total row follows them.
    pending: list[tuple[str, str | None, list[str]]] = []
    for label, numbers in parsed:
        name, flags = _area_name(label)
        region = PSP_REGION_NAMES.get(normalise_name(name))
        pending.append((name, flags, numbers))
        if region or normalise_name(name) == "ALL INDIA":
            for row_name, row_flags, row_numbers in pending:
                _add_rows(report, row_name, row_flags, row_numbers, region, periods, unit)
            pending = []
    for row_name, row_flags, row_numbers in pending:
        _add_rows(report, row_name, row_flags, row_numbers, None, periods, unit)
    if not report.records and not report.failures:
        raise InvalidResponseError("no data rows found in the report")
    return report


def _add_rows(report: PspReport, name: str, flags: str | None, numbers: list[str], region: str | None,
              periods: tuple[str, ...], unit: str) -> None:
    area = classify_area(name, region)
    for index, scope in enumerate(periods):
        chunk = numbers[4 * index:4 * index + 4]
        key = f"{report.month:%Y-%m}|{report.measure}|{scope}|{normalise_name(name)}"
        raw = {"name": name, "flags": flags, "period": scope, "values": chunk}
        try:
            values = {
                "report_month": report.month,
                "period_scope": scope,
                "measure": report.measure,
                "area_code": area["area_code"] if area else None,
                "area_name": name,
                "area_type": area["area_type"] if area else None,
                "state_code": area["state_code"] if area else None,
                "region_code": area["region_code"] if area else None,
                "requirement": to_decimal(chunk[0], 3),
                "supplied": to_decimal(chunk[1], 3),
                "not_supplied": to_decimal(chunk[2], 3),
                "not_supplied_pct": to_decimal(chunk[3], 2),
                "unit": unit,
                "report_status": report.status,
                "footnote_flags": flags,
            }
        except ValueError as exc:
            report.failures.append(ParseFailure(key, raw, str(exc)))
            continue
        report.records.append(ParsedRecord(key, values, raw))
