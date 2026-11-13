"""Parsers for the NLDC Daily PSP workbook (sections A, C and J).

The workbook layout changes over time (sheets added, rows shifted, header
wording edited), so everything is located by labels:

* a section starts at the row whose first cell begins with ``"<letter>. "``
  and ends at the next section heading;
* section A columns are found from the region header row (NR WR SR ER NER TOTAL),
  rows from their labels;
* section C/J columns are found from the two header rows' combined text.

A missing sheet, section, header or required label raises
:class:`InvalidWorkbookError` (the section's document fails; other sections
are parsed by their own runs). Unknown extra labels are reported, not guessed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

import xlrd

from src.ingestion.models import ParseFailure, ParsedRecord
from src.pipelines.cea.cells import Grid, clean, norm, to_decimal
from src.pipelines.grid_india.entities import REGION_HEADINGS, resolve_area
from src.pipelines.grid_india.errors import InvalidWorkbookError

_SECTION = re.compile(r"^([A-Z])\.\s")
REGION_COLUMNS = ("NR", "WR", "SR", "ER", "NER")


@dataclass(frozen=True)
class Workbook:
    sheets: dict[str, Grid]
    report_date: date            # operating day D ("Date of Reporting" - 1 day)
    reporting_date: date

    def sheet(self, name: str) -> Grid:
        if name not in self.sheets:
            raise InvalidWorkbookError(f"sheet {name!r} not found (sheets: {sorted(self.sheets)})")
        return self.sheets[name]


def read_workbook(content: bytes, *, expected_date: date) -> Workbook:
    """All sheets of the workbook and its report date, checked against the date in the listing title."""
    try:
        book = xlrd.open_workbook(file_contents=content, on_demand=True)
        sheets = {}
        for name in book.sheet_names():
            sheet = book.sheet_by_name(name)
            sheets[name] = Grid([sheet.row_values(r) for r in range(sheet.nrows)], book.datemode)
    except Exception as exc:  # xlrd raises several unrelated types
        raise InvalidWorkbookError(f"not a readable .xls workbook: {type(exc).__name__}: {exc}") from exc
    if "MOP_E" not in sheets:
        raise InvalidWorkbookError(f"sheet 'MOP_E' not found (sheets: {sorted(sheets)})")
    reporting = _reporting_date(sheets["MOP_E"])
    report_date = reporting - timedelta(days=1)
    if report_date != expected_date:
        raise InvalidWorkbookError(f"workbook reports {report_date} (Date of Reporting {reporting}); "
                                   f"the listing title says {expected_date}")
    return Workbook(sheets, report_date, reporting)


def _reporting_date(grid: Grid) -> date:
    for r in range(min(10, len(grid.rows))):
        row = grid.rows[r]
        for c, value in enumerate(row):
            if norm(value).startswith("DATE OF REPORTING"):
                for candidate in [value, *row[c + 1:]]:
                    parsed = _as_date(candidate, grid.datemode)
                    if parsed:
                        return parsed
    raise InvalidWorkbookError("'Date of Reporting' not found in sheet MOP_E")


def _as_date(value: Any, datemode: int) -> date | None:
    if isinstance(value, float) and value > 30000:
        return xlrd.xldate_as_datetime(value, datemode).date()
    text = clean(value)
    match = re.search(r"(\d{1,2}-[A-Za-z]{3}-\d{4})", text)
    if match:
        return datetime.strptime(match.group(1), "%d-%b-%Y").date()
    return None


def section_rows(grid: Grid, letter: str) -> range:
    """Row range of a section (heading row excluded), located by its letter."""
    start = None
    for r, row in enumerate(grid.rows):
        match = _SECTION.match(clean(row[0]) if row else "")
        if start is None and match and match[1] == letter:
            start = r
        elif start is not None and match:
            return range(start + 1, r)
    if start is None:
        raise InvalidWorkbookError(f"section {letter} not found")
    return range(start + 1, len(grid.rows))


def section_title(grid: Grid, letter: str) -> str:
    for row in grid.rows:
        text = clean(row[0]) if row else ""
        if text.startswith(f"{letter}. "):
            return text
    return ""


def _number(value: Any, scale: int) -> Decimal | None:
    """Cell -> Decimal; '-' and blanks -> None. Raises ValueError otherwise."""
    if clean(value) in ("", "-"):
        return None
    return to_decimal(value, scale)


def _clock(value: Any) -> time | None:
    """'18:20' or an Excel day fraction -> time; '-'/blank -> None."""
    if isinstance(value, float):
        if not 0 <= value < 1:
            raise ValueError(f"not a time of day: {value!r}")
        minutes = round(value * 24 * 60)
        return time(minutes // 60 % 24, minutes % 60)
    text = clean(value)
    if text in ("", "-"):
        return None
    match = re.fullmatch(r"(\d{1,2}):(\d{2})(?::\d{2})?", text)
    if not match:
        raise ValueError(f"not a time of day: {text!r}")
    return time(int(match[1]), int(match[2]))


# ------------------------------------------------------------------ section A

# (label pattern on the normalised first cell, column, kind)
SECTION_A_FIELDS: tuple[tuple[str, str, str], ...] = (
    # "Demand Met during Evening Peak hrs(MW) (at 20:00 hrs ...)" / "Demand Met (MW) during Evening Peak hr (19:00hrs)"
    # (the evening-peak hour itself varies between reports: 19:00 or 20:00; it is kept in the raw record)
    (r"^DEMAND MET .*EVENING PEAK", "evening_peak_demand_met_mw", "mw"),
    # "Peak Shortage (MW)" (2023) / "Shoratge (MW) during Evening Peak hr" (2026, misspelt in the source)
    (r"^(PEAK SHORTAGE|SHO[A-Z]* .*EVENING PEAK)", "evening_peak_shortage_mw", "mw"),
    (r"^ENERGY MET", "energy_met_mu", "mu"),
    (r"^HYDRO GEN", "hydro_gen_mu", "mu"),
    (r"^WIND GEN", "wind_gen_mu", "mu"),
    (r"^SOLAR GEN", "solar_gen_mu", "mu"),
    (r"^ENERGY SHORTAGE", "energy_shortage_mu", "mu"),
    (r"^MAXIMUM DEMAND MET DURING THE DAY", "max_demand_met_mw", "mw"),
    (r"^TIME OF MAXIMUM DEMAND MET", "max_demand_met_time", "time"),
)
REQUIRED_A = frozenset({"energy_met_mu", "max_demand_met_mw", "evening_peak_demand_met_mw"})
# Regional values that add up to the All-India value (maximum demand and its time do not).
ADDITIVE_A = ("evening_peak_demand_met_mw", "evening_peak_shortage_mw", "energy_met_mu", "hydro_gen_mu",
              "wind_gen_mu", "solar_gen_mu", "energy_shortage_mu")


@dataclass
class SectionAResult:
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    unknown_labels: list[str] = field(default_factory=list)
    # field -> (sum of regions, published All-India value), for additive fields with all values present
    totals: dict[str, tuple[Decimal, Decimal]] = field(default_factory=dict)


def parse_section_a(workbook: Workbook) -> SectionAResult:
    """Section A -> one ``grid_daily_power_supply`` record per region and one for All India (region NULL)."""
    grid = workbook.sheet("MOP_E")
    rows = section_rows(grid, "A")
    header = next((r for r in rows if {norm(v) for v in grid.rows[r]} >= {*REGION_COLUMNS, "TOTAL"}), None)
    if header is None:
        raise InvalidWorkbookError("section A region header (NR WR SR ER NER TOTAL) not found")
    columns = {norm(v): c for c, v in enumerate(grid.rows[header]) if norm(v) in (*REGION_COLUMNS, "TOTAL")}
    found: dict[str, int] = {}
    result = SectionAResult()
    for r in rows:
        if r <= header:
            continue
        label = norm(grid.cell(r, 0))
        if not label:
            continue
        match = next(((f, kind) for pattern, f, kind in SECTION_A_FIELDS if re.search(pattern, label)), None)
        if match is None:
            result.unknown_labels.append(clean(grid.cell(r, 0)))
            continue
        if match[0] in found:
            raise InvalidWorkbookError(f"section A label for {match[0]} appears twice (rows {found[match[0]] + 1}, {r + 1})")
        found[match[0]] = r
    missing = REQUIRED_A - set(found)
    if missing:
        raise InvalidWorkbookError(f"section A rows not found: {sorted(missing)}")

    for region in (*REGION_COLUMNS, "TOTAL"):
        col = columns[region]
        region_code = None if region == "TOTAL" else REGION_HEADINGS[region]
        key = f"{workbook.report_date}|{region}"
        raw: dict[str, Any] = {"column": region}
        values: dict[str, Any] = {"region_code": region_code, "report_date": workbook.report_date}
        try:
            for pattern, name, kind in SECTION_A_FIELDS:
                if name not in found:
                    values[name] = None
                    continue
                cell = grid.cell(found[name], col)
                raw[name] = clean(cell)
                values[name] = _clock(cell) if kind == "time" else _number(cell, 3 if kind == "mw" else 4)
        except ValueError as exc:
            result.failures.append(ParseFailure(key, raw, f"Section A, {region}: {exc}"))
            continue
        result.records.append(ParsedRecord(key, values, raw))

    by_region = {r.values["region_code"]: r.values for r in result.records}
    if None in by_region and all(code in by_region for code in REGION_COLUMNS):
        for name in ADDITIVE_A:
            published = by_region[None].get(name)
            if published is None:
                continue
            parts = [by_region[code].get(name) for code in REGION_COLUMNS]
            # Wind is published as '-' (no wind) for ER and NER: treated as 0 in the sum.
            result.totals[name] = (sum((p or Decimal(0) for p in parts), Decimal(0)), published)
    return result


# -------------------------------------------------------------- sections C, J

# Column keys from the combined text of the two header rows.
def _state_column(text: str) -> str | None:
    t = text.replace(" ", "")
    if "MAX.DEMAND" in t or t.startswith("MAXDEMAND"):
        return "max_demand_met_mw"
    if t.startswith("SHORTAGE"):
        return "shortage_at_max_demand_mw"
    if t.startswith("ENERGYMET"):
        return "energy_met_mu"
    if t.startswith("DRAWAL"):
        return "drawal_schedule_mu"
    if t.startswith("MAXOD") or (t.startswith("MAX") and "OD" in t):
        return "max_od_ud"
    if t.startswith("OD(+)/UD(-)"):
        return "od_ud_mu"
    if t.startswith("ENERGYSHORTAGE"):
        return "energy_shortage_mu"
    return None


REQUIRED_STATE_COLUMNS = frozenset({"max_demand_met_mw", "energy_met_mu", "drawal_schedule_mu", "od_ud_mu",
                                    "shortage_at_max_demand_mw", "energy_shortage_mu", "max_od_ud"})


@dataclass
class StatesResult:
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    region_label_mismatches: list[str] = field(default_factory=list)


def _split_max_od_ud(value: Any) -> tuple[Decimal | None, Decimal | None]:
    """'631/ -686' -> (631, -686); a single number (older layout, 'Max OD (MW)') -> (value, None)."""
    if isinstance(value, float):
        return to_decimal(value, 3), None
    text = clean(value)
    if text in ("", "-"):
        return None, None
    if "/" in text:
        left, right = (part.strip() for part in text.split("/", 1))
        return _number(left, 3), _number(right, 3)
    return _number(text, 3), None


def parse_states_table(grid: Grid, rows: range, *, section: str, report_date: date) -> StatesResult:
    """Rows of section C (or J): one record per state / system / bulk consumer / entity."""
    header = next((r for r in rows if norm(grid.cell(r, 0)) == "REGION" and norm(grid.cell(r, 1)) == "STATES"), None)
    if header is None:
        raise InvalidWorkbookError(f"section {section} header row ('Region', 'States') not found")
    columns: dict[str, int] = {}
    for c in range(2, len(grid.rows[header])):
        text = f"{norm(grid.cell(header - 1, c))} {norm(grid.cell(header, c))}".strip()
        key = _state_column(text)
        if key and key not in columns:
            columns[key] = c
    missing = REQUIRED_STATE_COLUMNS - set(columns)
    if missing:
        raise InvalidWorkbookError(f"section {section} columns not found: {sorted(missing)}")

    result = StatesResult()
    for r in rows:
        if r <= header:
            continue
        name = clean(grid.cell(r, 1))
        if not name:
            continue
        region_label = norm(grid.cell(r, 0))
        area = resolve_area(name)
        key = f"{report_date}|{norm(name)}"
        raw = {"section": section, "row": r + 1, "name": name, "region_label": region_label or None,
               **{k: clean(grid.cell(r, c)) for k, c in columns.items()}}
        if area and region_label in REGION_HEADINGS and REGION_HEADINGS[region_label] != area.region_code:
            result.region_label_mismatches.append(f"{name}: listed under {region_label}, reference region "
                                                  f"{area.region_code}")
        try:
            max_od, max_ud = _split_max_od_ud(grid.cell(r, columns["max_od_ud"]))
            values = {
                "area_code": area.area_code if area else None,
                "area_name": name,
                "area_type": area.area_type if area else None,
                "state_code": area.state_code if area else None,
                "region_code": area.region_code if area else None,
                "report_date": report_date,
                "max_demand_met_mw": _number(grid.cell(r, columns["max_demand_met_mw"]), 3),
                "shortage_at_max_demand_mw": _number(grid.cell(r, columns["shortage_at_max_demand_mw"]), 3),
                "energy_met_mu": _number(grid.cell(r, columns["energy_met_mu"]), 4),
                "drawal_schedule_mu": _number(grid.cell(r, columns["drawal_schedule_mu"]), 4),
                "od_ud_mu": _number(grid.cell(r, columns["od_ud_mu"]), 4),
                "max_od_mw": max_od,
                "max_ud_mw": max_ud,
                "energy_shortage_mu": _number(grid.cell(r, columns["energy_shortage_mu"]), 4),
            }
        except ValueError as exc:
            result.failures.append(ParseFailure(key, raw, f"Section {section} row {r + 1} ({name}): {exc}"))
            continue
        result.records.append(ParsedRecord(key, values, raw))
    if not result.records and not result.failures:
        raise InvalidWorkbookError(f"section {section} contains no rows")
    return result


def parse_states(workbook: Workbook) -> tuple[StatesResult, bool]:
    """Section C (sheet MOP_E) plus section J (sheet MOP_E1, when present). Returns (result, has_section_j)."""
    grid = workbook.sheet("MOP_E")
    result = parse_states_table(grid, section_rows(grid, "C"), section="C", report_date=workbook.report_date)
    has_j = "MOP_E1" in workbook.sheets
    if has_j:
        sheet = workbook.sheets["MOP_E1"]
        j = parse_states_table(sheet, section_rows(sheet, "J"), section="J", report_date=workbook.report_date)
        result.records += j.records
        result.failures += j.failures
        result.region_label_mismatches += j.region_label_mismatches
    return result, has_j


# ------------------------------------------------------------------ section G

# Published fuel row label (normalised) -> fuel_type code. The same six labels appear in every workbook
# checked from 2023-01 to 2026-11 (68 workbooks). Anything else is not guessed: it becomes a record with
# fuel_type None, which validation rejects (UNMAPPED_FUEL) and records.
FUEL_TYPES: dict[str, str] = {
    "COAL": "COAL",
    "LIGNITE": "LIGNITE",
    "HYDRO": "HYDRO",
    "NUCLEAR": "NUCLEAR",
    "GAS, NAPTHA & DIESEL": "GAS_NAPHTHA_DIESEL",
    "GAS, NAPHTHA & DIESEL": "GAS_NAPHTHA_DIESEL",
    "RES (WIND, SOLAR, BIOMASS & OTHERS)": "RES",
}
# Rows that are derived from the fuel rows: used for validation (Total) or ignored (shares), never stored.
SECTION_G_DERIVED = ("TOTAL", "SHARE OF RES", "SHARE OF NON-FOSSIL", "SHARE OF NON FOSSIL")


@dataclass
class SectionGResult:
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    missing_fuels: list[str] = field(default_factory=list)
    # (what, sum of parts, published value): fuel -> regions vs All India; region -> fuels vs Total
    totals: list[tuple[str, Decimal, Decimal, int]] = field(default_factory=list)


def parse_section_g(workbook: Workbook) -> SectionGResult:
    """Section G -> one ``grid_sourcewise_generation`` record per region and fuel (All India / Total validated only)."""
    grid = workbook.sheet("MOP_E")
    rows = section_rows(grid, "G")
    header = next((r for r in rows if {norm(v) for v in grid.rows[r]} >= {*REGION_COLUMNS, "ALL INDIA"}), None)
    if header is None:
        raise InvalidWorkbookError("section G header (NR WR SR ER NER All India) not found")
    columns = {norm(v): c for c, v in enumerate(grid.rows[header]) if norm(v) in (*REGION_COLUMNS, "ALL INDIA")}
    result = SectionGResult()
    day = workbook.report_date
    published_all_india: dict[str, Decimal | None] = {}
    total_row: dict[str, Decimal | None] | None = None

    for r in rows:
        if r <= header:
            continue
        label = clean(grid.cell(r, 0))
        key_label = norm(label)
        if not key_label:
            continue
        if key_label.startswith(SECTION_G_DERIVED):
            if key_label == "TOTAL":
                try:
                    total_row = {c: _number(grid.cell(r, columns[c]), 4) for c in (*REGION_COLUMNS, "ALL INDIA")}
                except ValueError:
                    total_row = None
            continue
        fuel_type = FUEL_TYPES.get(key_label)
        try:
            published_all_india[fuel_type or label] = _number(grid.cell(r, columns["ALL INDIA"]), 4)
        except ValueError:
            published_all_india[fuel_type or label] = None
        for region in REGION_COLUMNS:
            cell = grid.cell(r, columns[region])
            key = f"{day}|{region}|{fuel_type or key_label}"
            raw = {"row": r + 1, "fuel_label": label, "region_column": region, "value": clean(cell),
                   "all_india": clean(grid.cell(r, columns["ALL INDIA"]))}
            try:
                value = _number(cell, 4)
            except ValueError as exc:
                result.failures.append(ParseFailure(key, raw, f"Section G {label!r} {region}: {exc}"))
                continue
            result.records.append(ParsedRecord(key, {
                "region_code": REGION_HEADINGS[region], "report_date": day, "fuel_type": fuel_type,
                "fuel_label": label, "gross_generation_mu": value}, raw))

    present = {r.values["fuel_type"] for r in result.records}
    result.missing_fuels = sorted(set(FUEL_TYPES.values()) - present)
    if not result.records and not result.failures:
        raise InvalidWorkbookError("section G contains no fuel rows")

    known = [r.values for r in result.records if r.values["fuel_type"]]
    # Each fuel: sum of regions vs published All India.
    for fuel in sorted({v["fuel_type"] for v in known}):
        parts = [v["gross_generation_mu"] for v in known if v["fuel_type"] == fuel]
        published = published_all_india.get(fuel)
        if published is not None and len(parts) == len(REGION_COLUMNS):
            result.totals.append((f"{fuel} (sum of regions vs All India)",
                                  sum((p or Decimal(0) for p in parts), Decimal(0)), published, len(parts)))
    # Each region (and All India): sum of fuels vs published Total, only if every fuel row was mapped.
    if total_row is not None and not [r for r in result.records if r.values["fuel_type"] is None]:
        for region in REGION_COLUMNS:
            parts = [v["gross_generation_mu"] for v in known if v["region_code"] == REGION_HEADINGS[region]]
            if total_row.get(region) is not None and parts:
                result.totals.append((f"{region} (sum of fuels vs Total)",
                                      sum((p or Decimal(0) for p in parts), Decimal(0)), total_row[region], len(parts)))
        all_india_parts = [published_all_india.get(f) for f in sorted({v["fuel_type"] for v in known})]
        if total_row.get("ALL INDIA") is not None and all(p is not None for p in all_india_parts):
            result.totals.append(("All India (sum of fuels vs Total)", sum(all_india_parts, Decimal(0)),
                                  total_row["ALL INDIA"], len(all_india_parts)))
    return result


# ------------------------------------------------------------------ section F

# Published sector row label (normalised) -> sector. The same two rows (plus "Total") appear in every workbook
# checked from 2023-01 to 2026-11 (102 workbooks). Anything else becomes a record with sector None, which
# validation rejects (UNMAPPED_SECTOR).
OUTAGE_SECTORS: dict[str, str] = {"CENTRAL SECTOR": "CENTRAL", "STATE SECTOR": "STATE"}


@dataclass
class SectionFResult:
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    missing_sectors: list[str] = field(default_factory=list)
    # (what, sum of parts, published value)
    totals: list[tuple[str, Decimal, Decimal]] = field(default_factory=list)


def parse_section_f(workbook: Workbook) -> SectionFResult:
    """Section F -> one ``grid_generation_outage`` record per region and sector (TOTAL / Total validated only)."""
    grid = workbook.sheet("MOP_E")
    rows = section_rows(grid, "F")
    header = next((r for r in rows if {norm(v) for v in grid.rows[r]} >= {*REGION_COLUMNS, "TOTAL"}), None)
    if header is None:
        raise InvalidWorkbookError("section F header (NR WR SR ER NER TOTAL) not found")
    columns = {norm(v): c for c, v in enumerate(grid.rows[header]) if norm(v) in (*REGION_COLUMNS, "TOTAL")}
    result = SectionFResult()
    day = workbook.report_date
    published_total: dict[str, Decimal | None] = {}
    total_row: dict[str, Decimal | None] | None = None

    for r in rows:
        if r <= header:
            continue
        label = clean(grid.cell(r, 0))
        key_label = norm(label)
        if not key_label:
            continue
        if key_label == "TOTAL":
            try:
                total_row = {c: _number(grid.cell(r, columns[c]), 3) for c in (*REGION_COLUMNS, "TOTAL")}
            except ValueError:
                total_row = None
            continue
        sector = OUTAGE_SECTORS.get(key_label)
        try:
            published_total[sector or label] = _number(grid.cell(r, columns["TOTAL"]), 3)
        except ValueError:
            published_total[sector or label] = None
        for region in REGION_COLUMNS:
            cell = grid.cell(r, columns[region])
            key = f"{day}|{region}|{sector or key_label}"
            raw = {"row": r + 1, "sector_label": label, "region_column": region, "value": clean(cell),
                   "total": clean(grid.cell(r, columns["TOTAL"]))}
            try:
                value = _number(cell, 3)
            except ValueError as exc:
                result.failures.append(ParseFailure(key, raw, f"Section F {label!r} {region}: {exc}"))
                continue
            result.records.append(ParsedRecord(key, {
                "region_code": REGION_HEADINGS[region], "report_date": day, "sector": sector,
                "outage_capacity_mw": value}, raw))

    present = {r.values["sector"] for r in result.records}
    result.missing_sectors = sorted(set(OUTAGE_SECTORS.values()) - present)
    if not result.records and not result.failures:
        raise InvalidWorkbookError("section F contains no sector rows")

    known = [r.values for r in result.records if r.values["sector"]]
    # Each sector: sum of regions vs published TOTAL column.
    for sector in sorted({v["sector"] for v in known}):
        parts = [v["outage_capacity_mw"] for v in known if v["sector"] == sector]
        published = published_total.get(sector)
        if published is not None and len(parts) == len(REGION_COLUMNS):
            result.totals.append((f"{sector} (sum of regions vs TOTAL)",
                                  sum((p or Decimal(0) for p in parts), Decimal(0)), published))
    # Each region and the grand total: sum of sectors vs the Total row, only if every sector row was mapped.
    if total_row is not None and not [r for r in result.records if r.values["sector"] is None]:
        for region in REGION_COLUMNS:
            parts = [v["outage_capacity_mw"] for v in known if v["region_code"] == REGION_HEADINGS[region]]
            if total_row.get(region) is not None and parts:
                result.totals.append((f"{region} (sum of sectors vs Total)",
                                      sum((p or Decimal(0) for p in parts), Decimal(0)), total_row[region]))
        sector_totals = [published_total.get(s) for s in sorted({v["sector"] for v in known})]
        if total_row.get("TOTAL") is not None and sector_totals and all(p is not None for p in sector_totals):
            result.totals.append(("All India (sum of sectors vs Total)", sum(sector_totals, Decimal(0)),
                                  total_row["TOTAL"]))
    return result
