"""Parsers for the NLDC Daily PSP workbook (sections A, C, J, G, F, E, IR-Line, CrossBorder and TimeSeries).

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
from collections import defaultdict
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
    if t.startswith("ACE") and "MAX" in t:
        # From report date 2026-11-18: "ACE²(Max) +ve/ -ve (MW)", footnote "²Area Control Error".
        return "max_ace"
    if t.startswith("MAXOD") or (t.startswith("MAX") and "OD" in t):
        return "max_od_ud"
    if t.startswith("OD(+)/UD(-)"):
        return "od_ud_mu"
    if t.startswith("ENERGYSHORTAGE"):
        return "energy_shortage_mu"
    return None


REQUIRED_STATE_COLUMNS = frozenset({"max_demand_met_mw", "energy_met_mu", "drawal_schedule_mu", "od_ud_mu",
                                    "shortage_at_max_demand_mw", "energy_shortage_mu"})

# The eighth column of sections C and J. Exactly one of the two must be present:
#   OD_UD - "Max OD(+)/ UD(-) (MW)" (older: "Max OD (MW)"), up to report date 2026-11-17;
#   ACE   - "ACE²(Max) +ve/ -ve (MW)", Area Control Error, from report date 2026-11-18.
# ACE is a different measure from maximum over/under-drawal, so it is never written to max_od_mw / max_ud_mw:
# those stay NULL and the ACE pair is kept in the record's raw data (and in the raw workbook). Columns for it
# are prepared in migration 0010_grid_state_ace (not applied).
LAYOUT_OD_UD = "OD_UD"
LAYOUT_ACE = "ACE"
PEAK_COLUMNS = {"max_od_ud": LAYOUT_OD_UD, "max_ace": LAYOUT_ACE}


@dataclass
class StatesResult:
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    region_label_mismatches: list[str] = field(default_factory=list)
    layouts: dict[str, str] = field(default_factory=dict)   # section ("C", "J") -> LAYOUT_OD_UD / LAYOUT_ACE


def _split_max_od_ud(value: Any) -> tuple[Decimal | None, Decimal | None]:
    """'631/ -686' -> (631, -686); a single number (older layout, 'Max OD (MW)') -> (value, None).

    Also used for the ACE column, which has the same 'positive/ negative' format ('1772/ -433').
    """
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
    peak = sorted(k for k in PEAK_COLUMNS if k in columns)
    if len(peak) != 1:
        raise InvalidWorkbookError(
            f"section {section}: expected exactly one of the 'Max OD(+)/UD(-) (MW)' (max_od_ud) or "
            f"'ACE (Max) +ve/-ve (MW)' (max_ace) columns, found {peak or 'neither'}")
    peak_column = peak[0]
    layout = PEAK_COLUMNS[peak_column]

    result = StatesResult(layouts={section: layout})
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
            if layout == LAYOUT_OD_UD:
                max_od, max_ud = _split_max_od_ud(grid.cell(r, columns[peak_column]))
            else:
                max_od = max_ud = None   # not published in this layout; ACE is never mapped here
                ace_positive, ace_negative = _split_max_od_ud(grid.cell(r, columns[peak_column]))
                raw["ace_max_positive_mw"] = None if ace_positive is None else str(ace_positive)
                raw["ace_max_negative_mw"] = None if ace_negative is None else str(ace_negative)
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
        result.layouts.update(j.layouts)
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


# ------------------------------------------------------------------ section E

# Row label (normalised, spaces removed) -> column. Identical in every workbook checked from 2023-01 to 2026-11
# (136 workbooks, 54 distinct). Other row labels are reported (UNKNOWN_LABEL) and not loaded.
EXCHANGE_FIELDS: dict[str, str] = {
    "SCHEDULE(MU)": "scheduled_exchange_mu",
    "ACTUAL(MU)": "actual_exchange_mu",
    "O/D/U/D(MU)": "od_ud_mu",
}
REQUIRED_EXCHANGE_FIELDS = ("scheduled_exchange_mu", "actual_exchange_mu")
# The sign convention must be stated in the heading; it is stored with every row, never assumed.
_SIGN_CONVENTION = re.compile(r"IMPORT\s*\(\+VE\)\s*/\s*EXPORT\s*\(-VE\)")


@dataclass
class SectionEResult:
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    sign_convention: str = ""
    unknown_labels: list[str] = field(default_factory=list)
    unknown_columns: list[str] = field(default_factory=list)
    missing_fields: list[str] = field(default_factory=list)
    # (what, compared value, published value)
    totals: list[tuple[str, Decimal, Decimal]] = field(default_factory=list)
    od_checks: list[tuple[str, Decimal, Decimal]] = field(default_factory=list)


def parse_section_e(workbook: Workbook) -> SectionEResult:
    """Section E -> one ``grid_regional_exchange`` record per region (TOTAL column validated only)."""
    grid = workbook.sheet("MOP_E")
    rows = section_rows(grid, "E")  # raises "section E not found" first
    title = section_title(grid, "E")
    convention = title.split(" - ", 1)[1].strip() if " - " in title else ""
    if not _SIGN_CONVENTION.search(convention.upper()):
        raise InvalidWorkbookError(f"section E heading does not state the sign convention: {title!r}")
    header = next((r for r in rows if {norm(v) for v in grid.rows[r]} >= {*REGION_COLUMNS, "TOTAL"}), None)
    if header is None:
        raise InvalidWorkbookError("section E header (NR WR SR ER NER TOTAL) not found")
    columns = {norm(v): c for c, v in enumerate(grid.rows[header]) if norm(v) in (*REGION_COLUMNS, "TOTAL")}
    result = SectionEResult(sign_convention=convention)
    result.unknown_columns = [clean(v) for c, v in enumerate(grid.rows[header])
                              if c > 0 and clean(v) and norm(v) not in (*REGION_COLUMNS, "TOTAL")]
    day = workbook.report_date

    found: dict[str, int] = {}
    for r in rows:
        if r <= header:
            continue
        label = clean(grid.cell(r, 0))
        if not label:
            continue
        name = EXCHANGE_FIELDS.get(norm(label).replace(" ", ""))
        if name is None:
            result.unknown_labels.append(label)
        elif name in found:
            raise InvalidWorkbookError(f"section E row {label!r} appears twice")
        else:
            found[name] = r
    missing = [f for f in REQUIRED_EXCHANGE_FIELDS if f not in found]
    if missing:
        raise InvalidWorkbookError(f"section E rows not found: {missing}")
    result.missing_fields = [f for f in EXCHANGE_FIELDS.values() if f not in found]

    totals: dict[str, Decimal | None] = {}
    for name, r in found.items():
        try:
            totals[name] = _number(grid.cell(r, columns["TOTAL"]), 4)
        except ValueError:
            totals[name] = None
    for region in REGION_COLUMNS:
        key = f"{day}|{region}"
        raw = {"region_column": region, **{n: clean(grid.cell(r, columns[region])) for n, r in found.items()},
               **{f"total_{n}": clean(grid.cell(r, columns["TOTAL"])) for n, r in found.items()}}
        try:
            values: dict[str, Any] = {name: _number(grid.cell(r, columns[region]), 4) for name, r in found.items()}
        except ValueError as exc:
            result.failures.append(ParseFailure(key, raw, f"Section E {region}: {exc}"))
            continue
        result.records.append(ParsedRecord(key, {
            "region_code": REGION_HEADINGS[region], "report_date": day, "sign_convention": convention,
            **{name: values.get(name) for name in EXCHANGE_FIELDS.values()}}, raw))

    by_region = [r.values for r in result.records]
    if len(by_region) == len(REGION_COLUMNS):
        for name in found:
            parts = [v[name] for v in by_region]
            if totals.get(name) is not None and all(p is not None for p in parts):
                result.totals.append((f"{name} (sum of regions vs TOTAL)", sum(parts, Decimal(0)), totals[name]))
    if "od_ud_mu" in found:
        for v in by_region:
            if None not in (v["od_ud_mu"], v["actual_exchange_mu"], v["scheduled_exchange_mu"]):
                result.od_checks.append((v["region_code"], v["actual_exchange_mu"] - v["scheduled_exchange_mu"],
                                         v["od_ud_mu"]))
        if None not in (totals.get("od_ud_mu"), totals.get("actual_exchange_mu"), totals.get("scheduled_exchange_mu")):
            result.od_checks.append(("TOTAL", totals["actual_exchange_mu"] - totals["scheduled_exchange_mu"],
                                     totals["od_ud_mu"]))
    return result


# ------------------------------------------------------------- IR-Line sheet

IR_SHEET = "IR-Line"
# Header text (normalised) -> key, inter-regional part.
IR_COLUMNS = {
    "VOLTAGE LEVEL": "voltage_level", "LINE DETAILS": "line_name", "NO. OF CIRCUIT": "circuits",
    "MAX IMPORT (MW)": "max_import_mw", "MAX EXPORT (MW)": "max_export_mw", "IMPORT (MU)": "import_mu",
    "EXPORT (MU)": "export_mu", "NET (MU)": "net_exchange_mu",
}
# Header text (normalised) -> key, international part.
INTL_COLUMNS = {
    "STATE": "country_label", "REGION": "region_label", "LINE NAME": "line_name", "MAX (MW)": "max_mw",
    "MIN (MW)": "min_mw", "AVG (MW)": "avg_mw", "ENERGY EXCHANGE (MU)": "energy_exchange_mu",
}
_BLOCK = re.compile(r"^IMPORT/EXPORT OF (\S+) \(WITH (\S+)\)$")
_SUBTOTAL = re.compile(r"^(\S+)-(\S+)$")
_IR_CONVENTION = re.compile(r"IMPORT\s*=?\s*\(\+VE\)\s*/\s*EXPORT\s*=?\s*\(-VE\)")
_REGION_CELL = re.compile(r"^(NR|WR|SR|ER|NER)\b")


def _ir_rows(workbook: Workbook) -> tuple[Grid, int, int]:
    """IR-Line grid, inter-regional header row and international heading row."""
    grid = workbook.sheet(IR_SHEET)
    header = next((r for r, row in enumerate(grid.rows) if row and norm(row[0]).startswith("SL")
                   and any(norm(v) == "LINE DETAILS" for v in row)), None)
    if header is None:
        raise InvalidWorkbookError("IR-Line inter-regional header row ('Sl No', 'Line Details') not found")
    intl = next((r for r, row in enumerate(grid.rows) if row and norm(row[0]).startswith("INTERNATIONAL")), None)
    if intl is None:
        raise InvalidWorkbookError("IR-Line 'INTERNATIONAL EXCHANGES' heading not found")
    return grid, header, intl


def _header_columns(grid: Grid, row: int, wanted: dict[str, str], what: str) -> dict[str, int]:
    columns = {wanted[norm(v)]: c for c, v in enumerate(grid.rows[row]) if norm(v) in wanted}
    missing = sorted(set(wanted.values()) - set(columns))
    if missing:
        raise InvalidWorkbookError(f"{what} columns not found: {missing}")
    return columns


@dataclass
class IrLinesResult:
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    direction_convention: str = ""
    unknown_rows: list[str] = field(default_factory=list)
    # (pair label, sum of lines (import, export, net), published subtotal, excluded line or None)
    subtotals: list[tuple[str, tuple[Decimal, Decimal, Decimal], tuple[Decimal, Decimal, Decimal], str | None]] = \
        field(default_factory=list)


def parse_ir_lines(workbook: Workbook) -> IrLinesResult:
    """Inter-regional part of 'IR-Line' -> one ``grid_interregional_line_exchange`` record per line."""
    grid, header, intl = _ir_rows(workbook)
    convention = next((clean(row[0]) for row in grid.rows[:header] if row and _IR_CONVENTION.search(norm(row[0]))), "")
    if not convention:
        raise InvalidWorkbookError("IR-Line sheet does not state the NET sign convention")
    columns = _header_columns(grid, header, IR_COLUMNS, "IR-Line inter-regional")
    result = IrLinesResult(direction_convention=convention)
    day = workbook.report_date
    pair: tuple[str, str] | None = None
    block_lines: list[ParsedRecord] = []

    def value(r: int, key: str, scale: int) -> Decimal | None:
        return _number(grid.cell(r, columns[key]), scale)

    for r in range(header + 1, intl):
        first = norm(grid.cell(r, 0))
        if not first and not any(clean(v) for v in grid.rows[r]):
            continue
        if match := _BLOCK.match(first):
            pair, block_lines = (match[1], match[2]), []
            continue
        if re.fullmatch(r"\d+(\.0+)?", first):
            line = clean(grid.cell(r, columns["line_name"]))
            from_region = REGION_HEADINGS.get(pair[0]) if pair else None
            to_region = REGION_HEADINGS.get(pair[1]) if pair else None
            key = f"{day}|{pair[0] if pair else '?'}-{pair[1] if pair else '?'}|{norm(line)}"
            raw = {"row": r + 1, "block": f"{pair[0]}-{pair[1]}" if pair else None,
                   **{k: clean(grid.cell(r, c)) for k, c in columns.items()}}
            try:
                circuits = clean(grid.cell(r, columns["circuits"]))
                values = {
                    "from_region_code": from_region, "to_region_code": to_region, "report_date": day,
                    "line_name": line, "voltage_level": clean(grid.cell(r, columns["voltage_level"])) or None,
                    "circuits": None if circuits in ("", "-") else circuits,
                    "max_import_mw": value(r, "max_import_mw", 3), "max_export_mw": value(r, "max_export_mw", 3),
                    "import_mu": value(r, "import_mu", 4), "export_mu": value(r, "export_mu", 4),
                    "net_exchange_mu": value(r, "net_exchange_mu", 4), "direction_convention": convention,
                }
            except ValueError as exc:
                result.failures.append(ParseFailure(key, raw, f"IR-Line row {r + 1} ({line}): {exc}"))
                continue
            record = ParsedRecord(key, values, raw)
            result.records.append(record)
            block_lines.append(record)
            continue
        if (match := _SUBTOTAL.match(first)) and pair and (match[1], match[2]) == pair:
            try:
                published = tuple(value(r, k, 4) or Decimal(0) for k in ("import_mu", "export_mu", "net_exchange_mu"))
            except ValueError:
                continue
            parts = [tuple(b.values[k] or Decimal(0) for k in ("import_mu", "export_mu", "net_exchange_mu"))
                     for b in block_lines]
            total = tuple(sum((p[i] for p in parts), Decimal(0)) for i in range(3))
            gap = tuple(t - p for t, p in zip(total, published))
            excluded = None
            if any(abs(g) > Decimal("0.05") for g in gap):
                candidates = [b.values["line_name"] for b, p in zip(block_lines, parts)
                              if all(abs(p[i] - gap[i]) <= Decimal("0.005") for i in range(3))]
                excluded = candidates[0] if len(candidates) == 1 else None
            result.subtotals.append((f"{pair[0]}-{pair[1]}", total, published, excluded))
            continue
        result.unknown_rows.append(" | ".join(clean(v) for v in grid.rows[r] if clean(v))[:120])
    if not result.records and not result.failures:
        raise InvalidWorkbookError("IR-Line contains no inter-regional line rows")
    return result


@dataclass
class IntlLinesResult:
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    sign_convention: str = ""
    # (bucket, sum of line energies, Section D Actual) for comparable buckets
    section_d: list[tuple[str, Decimal, Decimal]] = field(default_factory=list)
    mw_not_reported: list[str] = field(default_factory=list)


def _section_d_actual(workbook: Workbook) -> dict[str, Decimal]:
    """Section D 'Actual (MU)' by column label (normalised); empty if the section or row is absent."""
    grid = workbook.sheet("MOP_E")
    try:
        rows = section_rows(grid, "D")
    except InvalidWorkbookError:
        return {}
    header = next((r for r in rows if sum(1 for v in grid.rows[r][1:] if clean(v)) >= 2), None)
    actual = next((r for r in rows if norm(grid.cell(r, 0)).startswith("ACTUAL")), None)
    if header is None or actual is None:
        return {}
    out = {}
    for c, label in enumerate(grid.rows[header]):
        if clean(label) and c > 0:
            try:
                v = _number(grid.cell(actual, c), 4)
            except ValueError:
                continue
            if v is not None:
                out[norm(label)] = v
    return out


def parse_intl_lines(workbook: Workbook, *, line_country: Any) -> IntlLinesResult:
    """International part of 'IR-Line' -> one ``grid_international_line_exchange`` record per line.

    ``line_country(name) -> code | None`` is the exact-name reference; ``country_code`` is None for an
    unknown name (rejected by validation). The published country label, printed on one row per group,
    is kept in the raw record and checked against the reference by validation.
    """
    from src.pipelines.grid_india.reference import COUNTRIES, COUNTRY_LABELS

    grid, _, intl = _ir_rows(workbook)
    convention = next((clean(v) for v in grid.rows[intl][1:] if _SIGN_CONVENTION.search(norm(v))), "")
    if not convention:
        raise InvalidWorkbookError("IR-Line international heading does not state the sign convention")
    header = next((r for r in range(intl + 1, len(grid.rows)) if any(clean(v) for v in grid.rows[r])), None)
    if header is None:
        raise InvalidWorkbookError("IR-Line international header row not found")
    columns = _header_columns(grid, header, INTL_COLUMNS, "IR-Line international")
    result = IntlLinesResult(sign_convention=convention)
    day = workbook.report_date
    sums: dict[str, Decimal] = {}
    for r in range(header + 1, len(grid.rows)):
        line = clean(grid.cell(r, columns["line_name"]))
        if not line:
            if any(clean(v) for v in grid.rows[r]):
                result.failures.append(ParseFailure(f"{day}|row{r + 1}", {"row": r + 1, "cells": [
                    clean(v) for v in grid.rows[r] if clean(v)]}, f"IR-Line international row {r + 1} has no line name"))
            continue
        code = line_country(line)
        label = clean(grid.cell(r, columns["country_label"]))
        region_label = clean(grid.cell(r, columns["region_label"]))
        region_match = _REGION_CELL.match(norm(region_label))
        key = f"{day}|{norm(line)}"
        raw = {"row": r + 1, "line_name": line, "country_label": label or None, "region_cell": str(
            grid.cell(r, columns["region_label"])), **{k: clean(grid.cell(r, c)) for k, c in columns.items()
                                                       if k not in ("line_name", "country_label")}}
        if label:
            raw["country_label_code"] = COUNTRY_LABELS.get(norm(label))
        try:
            values = {
                "country_code": code, "report_date": day, "line_name": line,
                "region_code": REGION_HEADINGS.get(region_match[1]) if region_match else None,
                "region_label": region_label or None,
                "max_mw": _number(grid.cell(r, columns["max_mw"]), 3),
                "min_mw": _number(grid.cell(r, columns["min_mw"]), 3),
                "avg_mw": _number(grid.cell(r, columns["avg_mw"]), 3),
                "energy_exchange_mu": _number(grid.cell(r, columns["energy_exchange_mu"]), 4),
                "sign_convention": convention,
            }
        except ValueError as exc:
            result.failures.append(ParseFailure(key, raw, f"IR-Line international row {r + 1} ({line}): {exc}"))
            continue
        result.records.append(ParsedRecord(key, values, raw))
        mw = (values["max_mw"], values["min_mw"], values["avg_mw"])
        if all(v == 0 for v in mw) and values["energy_exchange_mu"] not in (None, Decimal(0)):
            result.mw_not_reported.append(line)
        if code:
            isolated = "ISOLATED" in norm(region_label)
            bucket = f"-> {COUNTRIES[code].upper()}" if isolated else COUNTRIES[code].upper()
            sums[bucket] = sums.get(bucket, Decimal(0)) + (values["energy_exchange_mu"] or Decimal(0))
    if not result.records and not result.failures:
        raise InvalidWorkbookError("IR-Line contains no international line rows")

    section_d = _section_d_actual(workbook)
    if section_d and not [r for r in result.records if r.values["country_code"] is None]:
        for label, published in section_d.items():
            if "->" in label:   # e.g. "GODDA -> BANGLADESH": the isolated line(s) of that country
                bucket = "-> " + label.split("->", 1)[1].strip()
            else:
                bucket = label
            if bucket in sums:
                result.section_d.append((label, sums[bucket], published))
    return result


# --------------------------------------------------------- CrossBorder sheet

CB_SHEET = "CrossBorder"
# First report date with the sheet (CrossBorder audit, 2026-11-14): absent before it, required from it (C5).
CROSSBORDER_FIRST_PUBLISHED = date(2023, 7, 6)
_CB_TITLE = "CROSS BORDER EXCHANGE SCHEDULE"
_CB_BLOCKS = (("EXPORT FROM INDIA", "EXPORT"), ("IMPORT BY INDIA", "IMPORT"), ("NET FROM INDIA", "NET"))
_CB_NET_CONVENTION = re.compile(r"-\s*VE\s*:\s*EXPORT\s*/\s*\+\s*VE\s*:\s*IMPORT")
CB_EXCHANGES = ("IEX", "PXIL", "HPX")
# Column header label sets (normalised). The contract categories changed on 2023-10-01; the regimes are kept
# apart (decision C3): STOA is not T-GNA, and ISGS/LTA/MTOA PPA is not GNA.
# (label above the column, label in the 'Country' row) -> contract type; the Import block prints "(ISGA/PPA)".
_CB_SINGLE = {("GNA", "(ISGS/PPA)"): "GNA", ("GNA", "(ISGA/PPA)"): "GNA",
              ("(ISGS/LTA/MTOA)", "PPA"): "ISGS_LTA_MTOA_PPA"}
_CB_BILATERAL = {"T-GNA": "T_GNA_BILATERAL", "STOA": "STOA_BILATERAL"}   # label above 'BILATERAL' / 'TOTAL'
_CB_COLLECTIVE = {"IDAM": "COLLECTIVE_IDAM", "RTM": "COLLECTIVE_RTM"}     # under 'COLLECTIVE', one column per exchange
_COLLECTIVE_COLUMNS = frozenset((c, e) for c in _CB_COLLECTIVE.values() for e in CB_EXCHANGES)
CB_LAYOUTS: dict[str, frozenset[tuple[str, str | None]]] = {
    "GNA": frozenset({("GNA", None), ("T_GNA_BILATERAL", None)}) | _COLLECTIVE_COLUMNS,          # from 2023-10-01
    "PRE_GNA": frozenset({("ISGS_LTA_MTOA_PPA", None), ("STOA_BILATERAL", None)}) | _COLLECTIVE_COLUMNS,
}


@dataclass
class CrossBorderResult:
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    not_published: bool = False            # report date before the sheet existed: nothing expected
    layout: str | None = None
    missing_values: list[str] = field(default_factory=list)
    missing_countries: list[str] = field(default_factory=list)
    unknown_rows: list[str] = field(default_factory=list)
    # (what, sum of parts, published total)
    totals: list[tuple[str, Decimal, Decimal]] = field(default_factory=list)
    # (what, Import - Export, published Net)
    net_checks: list[tuple[str, Decimal, Decimal]] = field(default_factory=list)


def _cb_column_name(column: tuple[str, str | None]) -> str:
    return column[0] if column[1] is None else f"{column[0]}/{column[1]}"


def _cb_header(grid: Grid, heading: int, end: int, block: str) -> tuple[int, dict[int, tuple[str, str | None]], int]:
    """Header of one block -> (exchange-label row, {column: (contract_type, exchange)}, TOTAL column)."""
    country_row = next((r for r in range(heading + 1, min(end, heading + 6)) if norm(grid.cell(r, 0)) == "COUNTRY"), None)
    if country_row is None:
        raise InvalidWorkbookError(f"CrossBorder {block} block: 'Country' header row not found")
    sub_row = country_row + 1
    upper: dict[int, str] = {}
    for r in range(heading + 1, country_row):
        for c, v in enumerate(grid.rows[r]):
            if clean(v):
                if c in upper:
                    raise InvalidWorkbookError(f"CrossBorder {block} block: two header labels above column {c}")
                upper[c] = norm(v)
    anchors = {c: norm(v) for c, v in enumerate(grid.rows[country_row]) if c > 0 and clean(v)}
    sub = {c: norm(v) for c, v in enumerate(grid.rows[sub_row]) if c > 0 and clean(v)} if sub_row < end else {}
    used_upper: set[int] = set()
    used_sub: set[int] = set()
    columns: dict[int, tuple[str, str | None]] = {}
    total_column = None
    order = sorted(anchors)
    width = max(len(grid.rows[r]) for r in range(heading, min(end, sub_row + 1)))
    for i, c in enumerate(order):
        label, nxt = anchors[c], (order[i + 1] if i + 1 < len(order) else width)
        if label == "TOTAL" and c not in upper and c not in sub:
            total_column = c
        elif label == "BILATERAL" and upper.get(c) in _CB_BILATERAL and sub.get(c) == "TOTAL":
            columns[c] = (_CB_BILATERAL[upper[c]], None)
            used_upper.add(c)
            used_sub.add(c)
        elif label in _CB_COLLECTIVE and "COLLECTIVE" in upper.values():
            used_upper.add(next(k for k, v in upper.items() if v == "COLLECTIVE"))
            for col in range(c, nxt):
                if col in sub:
                    if sub[col] not in CB_EXCHANGES:
                        raise InvalidWorkbookError(f"CrossBorder {block} block: unknown exchange {sub[col]!r} under {label}")
                    columns[col] = (_CB_COLLECTIVE[label], sub[col])
                    used_sub.add(col)
        elif (upper.get(c), label) in _CB_SINGLE and not any(col in sub for col in range(c, nxt)):
            columns[c] = (_CB_SINGLE[(upper[c], label)], None)
            used_upper.add(c)
        else:
            raise InvalidWorkbookError(f"CrossBorder {block} block: unknown column header {upper.get(c)!r} / {label!r} "
                                       f"at column {c}")
    leftover = [upper[c] for c in upper if c not in used_upper] + [sub[c] for c in sub if c not in used_sub]
    if leftover:
        raise InvalidWorkbookError(f"CrossBorder {block} block: unrecognised header labels {leftover}")
    if total_column is None:
        raise InvalidWorkbookError(f"CrossBorder {block} block: TOTAL column not found")
    return sub_row, columns, total_column


def parse_crossborder(workbook: Workbook) -> CrossBorderResult:
    """'CrossBorder' sheet -> one ``grid_crossborder_market_schedule`` record per block, country and column.

    Blocks Export / Import (non-negative) and Net (signed, as published) are all kept; the row TOTAL
    column and the Total rows are additive and only validated. Before 2023-07-06 the sheet did not
    exist and nothing is expected; from that date a missing sheet fails the section.
    """
    from src.pipelines.grid_india.reference import CROSSBORDER_COUNTRIES

    day = workbook.report_date
    if CB_SHEET not in workbook.sheets:
        if day < CROSSBORDER_FIRST_PUBLISHED:
            return CrossBorderResult(not_published=True)
        raise InvalidWorkbookError(f"sheet {CB_SHEET!r} not found (published from {CROSSBORDER_FIRST_PUBLISHED}; "
                                   f"sheets: {sorted(workbook.sheets)})")
    grid = workbook.sheet(CB_SHEET)
    top = [norm(v) for row in grid.rows[:3] for v in row]
    if not any(_CB_TITLE in v for v in top):
        raise InvalidWorkbookError(f"CrossBorder title {_CB_TITLE!r} not found; the measure cannot be confirmed")
    reporting = None
    for row in grid.rows[:3]:
        for c, v in enumerate(row):
            if norm(v).startswith("DATE OF REPORTING"):
                reporting = next((d for d in (_as_date(x, grid.datemode) for x in row[c:]) if d), None)
    if reporting != workbook.reporting_date:
        raise InvalidWorkbookError(f"CrossBorder Date of Reporting {reporting} differs from MOP_E "
                                   f"{workbook.reporting_date}")

    headings: list[tuple[int, str, str]] = []
    for r, row in enumerate(grid.rows):
        first = norm(row[0]) if row else ""
        for prefix, direction in _CB_BLOCKS:
            if first.startswith(prefix):
                headings.append((r, direction, clean(row[0])))
    found = [d for _, d, _ in headings]
    if found != [d for _, d in _CB_BLOCKS]:
        raise InvalidWorkbookError(f"CrossBorder blocks found {found}; expected Export, Import and Net once each")
    if not _CB_NET_CONVENTION.search(norm(headings[2][2])):
        raise InvalidWorkbookError("CrossBorder Net block does not state the sign convention (-ve : Export / +ve : Import)")

    result = CrossBorderResult()
    matrix: dict[str, dict[str, dict[Any, Decimal | None]]] = {}
    reference_columns = None
    for i, (heading, direction, heading_text) in enumerate(headings):
        end = headings[i + 1][0] if i + 1 < len(headings) else len(grid.rows)
        sub_row, columns, total_column = _cb_header(grid, heading, end, direction)
        if reference_columns is None:
            reference_columns = (columns, total_column)
            layout = next((name for name, cols in CB_LAYOUTS.items() if frozenset(columns.values()) == cols), None)
            if layout is None:
                raise InvalidWorkbookError(f"CrossBorder columns {sorted(map(_cb_column_name, columns.values()))} do "
                                           "not match a known layout")
            result.layout = layout
        elif (columns, total_column) != reference_columns:
            raise InvalidWorkbookError(f"CrossBorder {direction} block columns differ from the Export block")
        block: dict[str, dict[Any, Decimal | None]] = {}
        matrix[direction] = block
        block_records: dict[str, list[ParsedRecord]] = defaultdict(list)
        seen_total = False
        for r in range(sub_row + 1, end):
            row = grid.rows[r]
            label = clean(row[0]) if row else ""
            if not any(clean(v) for v in row):
                continue
            if seen_total or not label:
                result.unknown_rows.append(f"{direction} row {r + 1}: " + " | ".join(clean(v) for v in row if clean(v))[:100])
                continue
            is_total = norm(label).startswith("TOTAL")
            name = "TOTAL" if is_total else norm(label)
            duplicate = not is_total and name in block
            cells: dict[Any, Decimal | None] = {}
            failed: set[int] = set()
            for col in [*columns, total_column]:
                what = f"{day}|{direction}|{name}|{'TOTAL' if col == total_column else _cb_column_name(columns[col])}"
                try:
                    cells[col] = _number(grid.cell(r, col), 4)
                except ValueError as exc:
                    cells[col] = None
                    failed.add(col)
                    result.failures.append(ParseFailure(what, {"row": r + 1, "column": col, "block": direction,
                                                               "country_label": label, "cell": str(grid.cell(r, col))},
                                                        f"CrossBorder {direction} {label} column {col}: {exc}"))
                    continue
                if cells[col] is None:
                    result.missing_values.append(what)
            if is_total:
                seen_total = True
                block["TOTAL"] = cells
                continue
            if duplicate:
                for record in block_records[name]:
                    record.raw["duplicate"] = True
            else:
                block[name] = cells
            for col, column in columns.items():
                if col in failed:
                    continue
                record = ParsedRecord(
                    f"{day}|{direction}|{name}|{_cb_column_name(column)}",
                    {"country_code": CROSSBORDER_COUNTRIES.get(name), "report_date": day, "contract_type": column[0],
                     "power_exchange": column[1], "direction": direction, "scheduled_energy_mu": cells[col],
                     "sign_convention": heading_text},
                    {"row": r + 1, "column": col, "block": direction, "country_label": label,
                     "header_layout": result.layout, "cell": str(grid.cell(r, col)), "duplicate": duplicate})
                block_records[name].append(record)
                result.records.append(record)
        if not seen_total:
            result.unknown_rows.append(f"{direction}: Total row not found")
        for code_label in CROSSBORDER_COUNTRIES:
            if code_label not in block:
                result.missing_countries.append(f"{direction}|{code_label}")
        # Row totals and column totals (additive; validated, not stored).
        countries = [n for n in block if n != "TOTAL"]
        for name, cells in block.items():
            parts = [cells.get(col) for col in columns]
            if None not in parts and cells.get(total_column) is not None:
                result.totals.append((f"{direction} {name} row TOTAL", sum(parts, Decimal(0)), cells[total_column]))
        if "TOTAL" in block:
            for col in [*columns, total_column]:
                parts = [block[n].get(col) for n in countries]
                published = block["TOTAL"].get(col)
                if None not in parts and published is not None:
                    what = "TOTAL" if col == total_column else _cb_column_name(columns[col])
                    result.totals.append((f"{direction} Total row {what}", sum(parts, Decimal(0)), published))

    # Net = Import - Export, per country (and Total row) and column, as published.
    columns, total_column = reference_columns
    for name, net_cells in matrix["NET"].items():
        exported, imported = matrix["EXPORT"].get(name), matrix["IMPORT"].get(name)
        if exported is None or imported is None:
            continue
        for col in [*columns, total_column]:
            values = (imported.get(col), exported.get(col), net_cells.get(col))
            if None not in values:
                what = "TOTAL" if col == total_column else _cb_column_name(columns[col])
                result.net_checks.append((f"{name} {what}", values[0] - values[1], values[2]))
    return result


# ---------------------------------------------------------- TimeSeries sheet

TS_SHEET = "TimeSeries"
# First report date with the sheet (TimeSeries audit, 2026-11-14): absent before it, required from it (T6).
TIMESERIES_FIRST_PUBLISHED = date(2024, 11, 4)
_TS_TITLE = "15 MIN (INSTANTANEOUS) ALL INDIA"
# Header label (footnote markers and units removed) -> column.
TS_COLUMNS = {
    "FREQUENCY": "grid_frequency_hz", "DEMAND MET": "demand_met_mw",
    "STORAGE DEMAND (PSP&BESS)": "storage_demand_mw", "NUCLEAR": "nuclear_generation_mw",
    "WIND": "wind_generation_mw", "SOLAR": "solar_generation_mw", "HYDRO": "hydro_generation_mw",
    "GAS": "gas_generation_mw", "THERMAL": "thermal_generation_mw", "STORAGE (PSP&BESS)": "storage_generation_mw",
    "OTHERS": "other_generation_mw", "NET DEMAND MET": "net_demand_met_mw", "TOTAL GENERATION": "total_generation_mw",
}
TS_EXCHANGE = "net_transnational_exchange_mw"
_TS_IMPORT_POSITIVE = re.compile(r"IMPORT\s*:?\s*'?\+\s*VE|\+\s*VE'?\)?\s*:?\s*IMP")
TS_SOURCES = ("nuclear_generation_mw", "wind_generation_mw", "solar_generation_mw", "hydro_generation_mw",
              "gas_generation_mw", "thermal_generation_mw", "storage_generation_mw", "other_generation_mw")
_TS_BASE = frozenset(TS_COLUMNS.values()) - {"storage_demand_mw", "storage_generation_mw"} | {TS_EXCHANGE}
# Known layouts (audit): 13 columns to 2026-05-05; + storage generation 2026-05-06..05-31; + storage demand from 06-01.
TS_LAYOUTS: dict[str, frozenset[str]] = {
    "13_COLUMNS": _TS_BASE,
    "15_COLUMNS": _TS_BASE | {"storage_generation_mw"},
    "16_COLUMNS": _TS_BASE | {"storage_generation_mw", "storage_demand_mw"},
}
TS_MARKS = tuple(time(h, m) for h in range(24) for m in (0, 15, 30, 45))
MW_CHECK_TOLERANCE = Decimal("1")
FREQUENCY_RANGE = (Decimal("49"), Decimal("51"))


@dataclass
class TimeSeriesResult:
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    not_published: bool = False
    layout: str | None = None
    exchange_heading: str = ""
    missing_marks: list[str] = field(default_factory=list)
    missing_values: list[str] = field(default_factory=list)
    unknown_rows: list[str] = field(default_factory=list)
    net_demand_mismatches: list[tuple[str, Decimal, Decimal]] = field(default_factory=list)   # (mark, published, A-wind-solar)
    total_mismatches: list[tuple[str, Decimal, Decimal]] = field(default_factory=list)        # (mark, published, sum of sources)
    total_excludes_storage: list[str] = field(default_factory=list)                           # marks
    telemetry_suspect: list[tuple[str, str]] = field(default_factory=list)                    # (mark, reason)
    max_demand_mw: Decimal | None = None


def _ts_label(value: Any) -> str:
    text = re.sub(r"[*¹²³�]+", "", norm(value))
    text = re.sub(r"\((MW|HZ)\)", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _ts_mark(value: Any) -> time | None:
    """'0:15', '0:15:00', an Excel day fraction, or a day fraction written as text -> time; None if not a time."""
    fraction = value if isinstance(value, float) else None
    text = clean(value)
    if fraction is None and re.fullmatch(r"0?\.\d+|0(\.0+)?", text):
        fraction = float(text)
    if fraction is not None:
        if not 0 <= fraction < 1:
            return None
        minutes = round(fraction * 1440)
        return time(minutes // 60, minutes % 60) if minutes < 1440 else None
    match = re.fullmatch(r"(\d{1,2}):(\d{2})(?::00)?", text)
    if not match or int(match[1]) > 23 or int(match[2]) > 59:
        return None
    return time(int(match[1]), int(match[2]))


def parse_timeseries(workbook: Workbook) -> TimeSeriesResult:
    """'TimeSeries' sheet -> one all-India ``grid_15min_scada`` record (region_code NULL) per 15-minute mark.

    Values are instantaneous readings at each mark, stored as published (zeros from telemetry loss
    included). Storage columns are NULL before they were published. Gaps are reported, never filled.
    """
    from src.pipelines.grid_india.api import IST

    day = workbook.report_date
    if TS_SHEET not in workbook.sheets:
        if day < TIMESERIES_FIRST_PUBLISHED:
            return TimeSeriesResult(not_published=True)
        raise InvalidWorkbookError(f"sheet {TS_SHEET!r} not found (published from {TIMESERIES_FIRST_PUBLISHED}; "
                                   f"sheets: {sorted(workbook.sheets)})")
    grid = workbook.sheet(TS_SHEET)
    if not any(_TS_TITLE in norm(v) for row in grid.rows[:3] for v in row):
        raise InvalidWorkbookError(f"TimeSeries title {_TS_TITLE!r} not found; the measure cannot be confirmed")
    reporting = None
    for row in grid.rows[:3]:
        for c, v in enumerate(row):
            if norm(v).startswith("DATE OF REPORTING"):
                reporting = next((d for d in (_as_date(x, grid.datemode) for x in row[c:]) if d), None)
    if reporting != workbook.reporting_date:
        raise InvalidWorkbookError(f"TimeSeries Date of Reporting {reporting} differs from MOP_E {workbook.reporting_date}")
    header = next((r for r in range(min(8, len(grid.rows))) if grid.rows[r] and norm(grid.rows[r][0]) == "TIME"), None)
    if header is None:
        raise InvalidWorkbookError("TimeSeries header row ('TIME') not found")

    result = TimeSeriesResult()
    columns: dict[str, int] = {}
    for c, v in enumerate(grid.rows[header]):
        if c == 0 or not clean(v):
            continue
        label = _ts_label(v)
        if label.startswith("NET TRANS"):
            if not _TS_IMPORT_POSITIVE.search(label):
                raise InvalidWorkbookError(f"TimeSeries exchange header does not state import as positive: {clean(v)!r}")
            name = TS_EXCHANGE
            result.exchange_heading = clean(v)
        elif label in TS_COLUMNS:
            name = TS_COLUMNS[label]
        else:
            raise InvalidWorkbookError(f"TimeSeries unknown column header {clean(v)!r}")
        if name in columns:
            raise InvalidWorkbookError(f"TimeSeries column {name} appears twice")
        columns[name] = c
    result.layout = next((n for n, cols in TS_LAYOUTS.items() if frozenset(columns) == cols), None)
    if result.layout is None:
        raise InvalidWorkbookError(f"TimeSeries columns {sorted(columns)} do not match a known layout")

    seen: dict[time, list[ParsedRecord]] = defaultdict(list)
    for r in range(header + 1, len(grid.rows)):
        row = grid.rows[r]
        if not any(clean(v) for v in row):
            continue
        first = row[0] if row else ""
        mark = _ts_mark(first)
        if mark is None:
            text = clean(first)
            if r == header + 1 and all(clean(v).startswith("(") for v in row[1:] if clean(v)):
                continue                                   # formula-letter row, e.g. '(A)', '(I=A-(C+D))'
            if text[:1] in ("*", "¹") or len(text) > 60:
                continue                                   # footnotes and disclaimer
            result.unknown_rows.append(f"row {r + 1}: " + " | ".join(clean(v) for v in row if clean(v))[:100])
            continue
        label = mark.strftime("%H:%M")
        key = f"{day}|{label}"
        raw = {"row": r + 1, "time_cell": str(first), "layout": result.layout}
        if mark not in TS_MARKS:
            result.failures.append(ParseFailure(key, raw, f"TimeSeries row {r + 1}: {clean(first)!r} is not a 15-minute mark"))
            continue
        values: dict[str, Any] = {"region_code": None, "interval_start": datetime.combine(day, mark, IST)}
        try:
            for name, c in columns.items():
                values[name] = _number(grid.cell(r, c), 4 if name == "grid_frequency_hz" else 3)
                if values[name] is None:
                    result.missing_values.append(f"{key}|{name}")
        except ValueError as exc:
            result.failures.append(ParseFailure(key, raw, f"TimeSeries row {r + 1} ({label}): {exc}"))
            continue
        for name in ("storage_demand_mw", "storage_generation_mw"):
            values.setdefault(name, None)              # not published in this layout: NULL, never 0 (T4)
        record = ParsedRecord(key, values, raw)
        seen[mark].append(record)
        result.records.append(record)
        _ts_row_checks(result, label, values)
    for mark, records in seen.items():
        if len(records) > 1:
            for record in records:
                record.raw["duplicate"] = True
    if result.records or result.failures:
        result.missing_marks = [m.strftime("%H:%M") for m in TS_MARKS if m not in seen]
    demands = [r.values["demand_met_mw"] for r in result.records if r.values["demand_met_mw"] is not None]
    result.max_demand_mw = max(demands, default=None)
    return result


def _ts_row_checks(result: TimeSeriesResult, label: str, v: dict[str, Any]) -> None:
    demand, wind, solar = v["demand_met_mw"], v["wind_generation_mw"], v["solar_generation_mw"]
    if None not in (v["net_demand_met_mw"], demand, wind, solar):
        expected = demand - wind - solar
        if abs(v["net_demand_met_mw"] - expected) > MW_CHECK_TOLERANCE:
            result.net_demand_mismatches.append((label, v["net_demand_met_mw"], expected))
    sources = [v[s] for s in TS_SOURCES if s in v and v[s] is not None]
    total = v["total_generation_mw"]
    if total is not None and sources:
        parts = sum(sources, Decimal(0))
        storage = v.get("storage_generation_mw") or Decimal(0)
        if abs(total - parts) > MW_CHECK_TOLERANCE:
            if storage and abs(total - (parts - storage)) <= MW_CHECK_TOLERANCE:
                result.total_excludes_storage.append(label)
            else:
                result.total_mismatches.append((label, total, parts))
    frequency = v["grid_frequency_hz"]
    if frequency is not None and not FREQUENCY_RANGE[0] <= frequency <= FREQUENCY_RANGE[1]:
        result.telemetry_suspect.append((label, f"frequency {frequency} Hz outside 49-51 Hz"))
    generation = [v[s] for s in TS_SOURCES if v.get(s) is not None]
    if demand and generation and all(g == 0 for g in generation):
        result.telemetry_suspect.append((label, f"every generation source is 0 while demand met is {demand} MW"))
