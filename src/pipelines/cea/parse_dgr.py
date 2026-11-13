"""Parsers for the CEA Daily Generation Report (DGR) on the NPP mirror.

* Sub-report 10, "Daily Maintenance Report (coal, lignite and nuclear)": one
  row per unit under outage with planned / forced (major) / forced (minor) /
  others MW, outage start, expected/sync date of return and reason.
* Sub-report 2, region > state > sector > type > station > unit generation:
  station rows (generation, coal stock in days, capacity under outage) and
  unit rows (unit capacity, outage details). Also the source of the plant and
  unit master (``reference``).

Columns are located by their header labels (positions differ between years).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from src.ingestion.models import ParseFailure, ParsedRecord
from src.pipelines.cea.cells import Grid, clean, find_report_date, is_blank, is_number, norm, to_decimal, to_ist_datetime
from src.pipelines.cea.errors import InvalidResponseError
from src.pipelines.cea.geography import DGR_REGION_HEADINGS


def _find_columns(grid: Grid, row: int, labels: dict[str, str]) -> dict[str, int]:
    """Columns whose normalised header text starts with each label, in ``row``."""
    found: dict[str, int] = {}
    for c, value in enumerate(grid.rows[row]):
        text = norm(value)
        for key, label in labels.items():
            if key not in found and text.startswith(label):
                found[key] = c
    missing = sorted(set(labels) - set(found))
    if missing:
        raise InvalidResponseError(f"header label(s) not found in row {row + 1}: "
                                   f"{[labels[k] for k in missing]}")
    return found


# ---------------------------------------------------------------- sub-report 10

DGR10_LABELS = {
    "state": "STATE/SYSTEM", "station": "POWER STATION", "unit": "UNIT NO", "planned": "PLANNED MAINTENANCE",
    "forced_major": "FORCED MAINTENANCE (MAJOR)", "forced_minor": "FORCED MAINTENANCE (MINOR)",
    "others": "OTHERS MAINTENANCE", "start": "DATE& TIME OF MAINTENANCE", "sync": "EXPECTED/SYNC DATE",
    "reason": "REASONS",
}


@dataclass
class OutageReport:
    report_date: date
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    published_total_mw: Decimal | None = None   # the final "Total" row (all four categories)


def parse_outage_report(grid: Grid, *, expected_date: date, plants: dict[str, int],
                        units: dict[tuple[int, str], int]) -> OutageReport:
    """Unit rows -> ``cea_dgr_unit_outage`` records.

    Raises:
        InvalidResponseError: Not sub-report 10, header missing, or a different report date.
    """
    if not any("MAINTENANCE REPORT" in norm(v) for row in grid.rows[:6] for v in row):
        raise InvalidResponseError("not a DGR daily maintenance report (title not found)")
    report_date = find_report_date(grid, r"MAINTENANCE REPORT.*?(\d{2}-\d{2}-\d{4})")
    if report_date != expected_date:
        raise InvalidResponseError(f"maintenance report is for {report_date}, expected {expected_date}")
    header = next((r for r in range(min(15, len(grid.rows)))
                   if any(norm(v) == "POWER STATION" for v in grid.rows[r])), None)
    if header is None:
        raise InvalidResponseError("maintenance report header row not found")
    cols = _find_columns(grid, header, DGR10_LABELS)
    report = OutageReport(report_date)
    for r in range(header + 1, len(grid.rows)):
        first = norm(grid.cell(r, 0))
        if first.startswith("NOTE"):
            break
        station, unit = clean(grid.cell(r, cols["station"])), clean(grid.cell(r, cols["unit"]))
        if first == "TOTAL":
            mw = [grid.cell(r, cols[k]) for k in ("planned", "forced_major", "forced_minor", "others")]
            try:
                report.published_total_mw = sum((to_decimal(v, 3) or Decimal(0) for v in mw), Decimal(0))
            except ValueError:
                report.published_total_mw = None
            continue
        if not station or not unit or first.startswith("TOTAL") or is_number(station):
            continue  # headings, totals, and the column-number row ("1 2 3 ...")
        state = clean(grid.cell(r, cols["state"]))
        key = f"{report_date}|{norm(station)}|{unit}"
        raw = {"row": r + 1, **{k: clean(grid.cell(r, c)) for k, c in cols.items()}}
        try:
            parts = {k: to_decimal(grid.cell(r, cols[k]), 3) for k in ("planned", "forced_major", "forced_minor", "others")}
            if all(v is None for v in parts.values()):
                raise ValueError("no outage capacity values")
            plant_id = plants.get(norm(station))
            values = {
                "plant_id": plant_id,
                "unit_id": units.get((plant_id, unit)) if plant_id is not None else None,
                "unit_label": unit,
                "state_name": state or None,
                "report_date": report_date,
                "planned_mw": parts["planned"],
                "forced_major_mw": parts["forced_major"],
                "forced_minor_mw": parts["forced_minor"],
                "other_mw": parts["others"],
                "outage_capacity_mw": sum((v for v in parts.values() if v is not None), Decimal(0)),
                "outage_start_at": to_ist_datetime(grid.cell(r, cols["start"]), grid.datemode),
                "expected_sync_at": to_ist_datetime(grid.cell(r, cols["sync"]), grid.datemode),
                "outage_reason": clean(grid.cell(r, cols["reason"])) or None,
            }
        except ValueError as exc:
            report.failures.append(ParseFailure(key, raw, f"Outage row {r + 1} ({station} unit {unit}): {exc}"))
            continue
        previous = report.records[-1] if report.records else None
        if previous is not None and _is_continuation(previous, station, values):
            report.records[-1] = _merge_continuation(previous, station, values, raw, report_date, plants, units)
            continue
        report.records.append(ParsedRecord(key, values, raw))
    return report


# Cells compared to recognise a continuation row (everything except station name, ids and reason).
_SAME_CELLS = ("unit_label", "state_name", "report_date", "planned_mw", "forced_major_mw", "forced_minor_mw",
               "other_mw", "outage_start_at", "expected_sync_at")


def _is_continuation(previous: ParsedRecord, station: str, values: dict[str, Any]) -> bool:
    """A row that repeats the previous unit row because a long text cell wrapped onto it.

    Seen in the reports: the reason wraps ("RESERVE SHUT DOWN /" + "STANDBY UNIT", same station) or the
    station name wraps ("ADANI POWER LIMITED MUNDRA" + "TPP - I & II"). All other cells, including the
    outage start time, are identical. Anything else stays a separate row, so a genuine conflict is still
    rejected by the loader.
    """
    if any(previous.values[k] != values[k] for k in _SAME_CELLS):
        return False
    same_station = norm(previous.raw["station"]) == norm(station)
    return same_station or values["outage_start_at"] is not None


def _merge_continuation(previous: ParsedRecord, station: str, values: dict[str, Any], raw: dict[str, Any],
                        report_date: date, plants: dict[str, int], units: dict[tuple[int, str], int]) -> ParsedRecord:
    merged_values, merged_raw, key = dict(previous.values), dict(previous.raw), previous.key
    if norm(previous.raw["station"]) != norm(station):
        joined = f"{previous.raw['station']} {station}"
        merged_raw["station"] = joined
        key = f"{report_date}|{norm(joined)}|{values['unit_label']}"
        plant_id = plants.get(norm(joined))
        merged_values["plant_id"] = plant_id
        merged_values["unit_id"] = units.get((plant_id, values["unit_label"])) if plant_id is not None else None
    reason, new_reason = previous.values["outage_reason"], values["outage_reason"]
    if new_reason and new_reason != reason:
        merged_values["outage_reason"] = " ".join(p for p in (reason, new_reason) if p)
    merged_raw["continuation_rows"] = [*previous.raw.get("continuation_rows", []), raw]
    return ParsedRecord(key, merged_values, merged_raw)


# ----------------------------------------------------------------- sub-report 2

DGR2_TOP_LABELS = {
    "capacity": "MONITORED CAP", "coal_days": "COAL STOCK IN DAYS", "outage_mw": "CAP. UNDER OUTAGE",
    "outage_date": "OUTAGE DATE", "sync_date": "EXPECTED DATE", "remarks": "REMARKS",
}
DGR2_MID_LABELS = {"today_program": "TODAY'S PROGRAM", "today_actual": "TODAY'S ACTUAL"}
DGR2_LOW_LABELS = {"ytd_program": "PROG.", "ytd_actual": "ACTUAL"}


@dataclass
class Station:
    name: str
    region_code: str | None
    state_name: str | None
    sector: str | None
    plant_type: str | None
    row: int
    cells: dict[str, Any]
    foreign: bool = False          # Bhutan import section


@dataclass
class Unit:
    station: str
    label: str
    flag: str | None
    capacity_mw: Decimal | None
    outage_mw: Decimal | None
    outage_start_at: datetime | None
    expected_sync_at: datetime | None
    reason: str | None
    row: int


@dataclass
class Dgr2Report:
    report_date: date
    stations: list[Station] = field(default_factory=list)
    units: list[Unit] = field(default_factory=list)
    unit_failures: list[ParseFailure] = field(default_factory=list)


def read_dgr2(grid: Grid, *, expected_date: date | None = None) -> Dgr2Report:
    """Walk the region/state/sector/type/station/unit hierarchy of sub-report 2.

    Raises:
        InvalidResponseError: Not sub-report 2, header missing, or a different report date.
    """
    if not any("STATION WISE, UNIT WISE GENERATION REPORT" in norm(v) for row in grid.rows[:4] for v in row):
        raise InvalidResponseError("not DGR sub-report 2 (title not found)")
    report_date = find_report_date(grid, r"REPORT FOR\s+(\d{2}/\d{2}/\d{4})", fmt="%d/%m/%Y")
    if expected_date is not None and report_date != expected_date:
        raise InvalidResponseError(f"generation report is for {report_date}, expected {expected_date}")
    header = next((r for r in range(min(10, len(grid.rows))) if norm(grid.cell(r, 0)) == "POWER STATION"), None)
    if header is None or header + 2 >= len(grid.rows):
        raise InvalidResponseError("generation report header row not found")
    cols = {**_find_columns(grid, header, DGR2_TOP_LABELS), **_find_columns(grid, header + 1, DGR2_MID_LABELS),
            **_find_columns(grid, header + 2, DGR2_LOW_LABELS)}
    capacity_col = cols["capacity"]
    report = Dgr2Report(report_date)
    region = state = sector = plant_type = None
    foreign = False
    station: Station | None = None
    for r in range(header + 3, len(grid.rows)):
        first = norm(grid.cell(r, 0))
        if first.startswith("NOTE") or first.startswith("REMARKS"):
            break
        rest_blank = all(is_blank(v) for v in grid.rows[r][1:])
        if first in ("REGION TOTAL", "STATE TOTAL"):
            continue
        if first.startswith("SECTOR:") or first.startswith("TYPE:"):
            label = next((clean(v) for v in grid.rows[r][1:capacity_col] if not is_blank(v)), None)
            if first.startswith("SECTOR:"):
                sector = label
            else:
                plant_type = label
            continue
        if first and rest_blank:
            if first in DGR_REGION_HEADINGS:
                region, state, foreign = DGR_REGION_HEADINGS[first], None, False
            elif "BHUTAN" in first:
                region, state, foreign = None, clean(grid.cell(r, 0)), True
            else:
                state = clean(grid.cell(r, 0))
            continue
        if first == "UNIT":
            if station is None:
                continue
            labels = [clean(v) for v in grid.rows[r][1:capacity_col] if not is_blank(v)]
            if not labels:
                continue
            try:
                report.units.append(Unit(
                    station=station.name, label=labels[0], flag=labels[1] if len(labels) > 1 else None,
                    capacity_mw=to_decimal(grid.cell(r, capacity_col), 3),
                    outage_mw=to_decimal(grid.cell(r, cols["outage_mw"]), 3),
                    outage_start_at=to_ist_datetime(grid.cell(r, cols["outage_date"]), grid.datemode),
                    expected_sync_at=to_ist_datetime(grid.cell(r, cols["sync_date"]), grid.datemode),
                    reason=clean(grid.cell(r, cols["remarks"])) or None, row=r + 1))
            except ValueError as exc:
                report.unit_failures.append(ParseFailure(
                    f"{report_date}|{norm(station.name)}|{labels[0]}", {"row": r + 1},
                    f"Unit row {r + 1} ({station.name}): {exc}"))
            continue
        if first and is_number(grid.cell(r, capacity_col)):
            station = Station(clean(grid.cell(r, 0)), region, state, sector, plant_type, r + 1,
                              {k: grid.cell(r, c) for k, c in cols.items()}, foreign)
            report.stations.append(station)
    if not report.stations:
        raise InvalidResponseError("generation report contains no station rows")
    return report


def plant_daily_records(report: Dgr2Report, *, plants: dict[str, int]) -> tuple[list[ParsedRecord], list[ParseFailure]]:
    """Indian station rows -> ``cea_dgr_plant_daily`` records (Bhutan import stations are skipped)."""
    records, failures = [], []
    for station in report.stations:
        if station.foreign:
            continue
        key = f"{report.report_date}|{norm(station.name)}"
        raw = {"row": station.row, "station": station.name, "state": station.state_name, "type": station.plant_type,
               **{k: clean(v) for k, v in station.cells.items()}}
        c = station.cells
        try:
            values = {
                "plant_id": plants.get(norm(station.name)),
                "report_date": report.report_date,
                "monitored_capacity_mw": to_decimal(c["capacity"], 3),
                "todays_program_mu": to_decimal(c["today_program"], 4),
                "todays_actual_mu": to_decimal(c["today_actual"], 4),
                "ytd_program_mu": to_decimal(c["ytd_program"], 4),
                "ytd_actual_mu": to_decimal(c["ytd_actual"], 4),
                "coal_stock_days": to_decimal(c["coal_days"], 2),
                "capacity_under_outage_mw": to_decimal(c["outage_mw"], 3),
                "remarks": clean(c["remarks"]) or None,
            }
        except ValueError as exc:
            failures.append(ParseFailure(key, raw, f"Station row {station.row} ({station.name}): {exc}"))
            continue
        records.append(ParsedRecord(key, values, raw))
    return records, failures
