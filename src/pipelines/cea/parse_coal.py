"""Parser for the CEA Daily Coal Stock Report (``dailyCoal1-YYYY-MM-DD.xls``).

Layout (2024-2026 files): a title block with ``(AS ON DD-MM-YYYY)``, a header
row containing "Name of Thermal Power Station", then plant rows grouped into
categories A-D with GENCO / category subtotal rows, ``DCB`` / ``ICB`` /
``Grand Total`` rows and a summary block.

Column positions shift between years, and merged header cells do not line up
with the data cells, so a plant row is read as the *sequence* of its non-empty
cells after the station name and checked against a strict pattern::

    capacity, PLF%, normative days, daily requirement, normative stock,
    stock indigenous, stock import, stock total, % of normative,
    [critical "*"], receipt, consumption, [remarks]

A row that does not match is a parse failure (never silently mis-mapped).
Stock values are '000 tonnes as published.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any

from src.ingestion.models import ParseFailure, ParsedRecord
from src.pipelines.cea.cells import Grid, clean, find_report_date, is_blank, is_number, norm, to_decimal
from src.pipelines.cea.errors import InvalidResponseError

NAME_HEADER = "NAME OF THERMAL POWER STATION"


@dataclass
class CoalReport:
    report_date: date
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    grand_total_stock: Decimal | None = None   # '000 t, from the "Grand Total" row

    @property
    def plant_stock_sum(self) -> Decimal:
        return sum((r.values["actual_stock_000_tonnes"] for r in self.records
                    if r.values.get("actual_stock_000_tonnes") is not None), Decimal(0))


def _header(grid: Grid) -> tuple[int, int]:
    for r in range(min(15, len(grid.rows))):
        for c, value in enumerate(grid.rows[r]):
            if norm(value).startswith(NAME_HEADER):
                return r, c
    raise InvalidResponseError("coal report header 'Name of Thermal Power Station' not found")


def _tokens(row: list[Any], after: int) -> list[Any]:
    return [value for value in row[after + 1:] if not is_blank(value)]


def _is_pct(value: Any) -> bool:
    return isinstance(value, str) and clean(value).endswith("%") and is_number(value)


def _split_plant_tokens(tokens: list[Any]) -> dict[str, Any]:
    """Map the cells after the station name; raises ValueError if the pattern does not match."""
    if len(tokens) < 11:
        raise ValueError(f"expected at least 11 values after the station name, found {len(tokens)}")
    fixed = tokens[:9]
    kinds = ["num", "pct", "num", "num", "num", "num", "num", "num", "pct"]
    for value, kind in zip(fixed, kinds):
        ok = _is_pct(value) if kind == "pct" else (is_number(value) and not _is_pct(value))
        if not ok:
            raise ValueError(f"unexpected value {value!r} where a {'percentage' if kind == 'pct' else 'number'} "
                             "was expected")
    rest = tokens[9:]
    critical = False
    if rest and clean(rest[0]) == "*":
        critical, rest = True, rest[1:]
    if len(rest) < 2 or not all(is_number(v) and not _is_pct(v) for v in rest[:2]):
        raise ValueError(f"receipt and consumption values not found (remaining cells {rest!r})")
    remarks = " ".join(clean(v) for v in rest[2:]) or None
    names = ["capacity_mw", "plf_pct", "normative_days", "daily_requirement", "normative_stock", "stock_indigenous",
             "stock_import", "stock_total", "stock_vs_normative_pct"]
    return {**dict(zip(names, fixed)), "critical": critical, "receipt": rest[0], "consumption": rest[1],
            "remarks": remarks}


def parse_coal_report(grid: Grid, *, expected_date: date, plants: dict[str, int]) -> CoalReport:
    """Plant rows -> ``cea_coal_stock_daily`` records (``plant_id`` None if the name is not mapped).

    Raises:
        InvalidResponseError: Header or report date missing, or the date differs from the requested one.
    """
    report_date = find_report_date(grid, r"AS ON\s+(\d{2}-\d{2}-\d{4})")
    if report_date != expected_date:
        raise InvalidResponseError(f"coal report is AS ON {report_date}, expected {expected_date}")
    header_row, name_col = _header(grid)
    report = CoalReport(report_date)
    for r in range(header_row + 1, len(grid.rows)):
        row = grid.rows[r]
        name = clean(grid.cell(r, name_col))
        if norm(name).startswith("GRAND TOTAL"):
            # Total stock is the value just before the "% of normative" cell (the last percentage);
            # the cells before it differ between years.
            tokens = _tokens(row, name_col)
            pct = [i for i, v in enumerate(tokens) if _is_pct(v)]
            try:
                report.grand_total_stock = to_decimal(tokens[pct[-1] - 1], 3) if len(pct) >= 2 else None
            except ValueError:
                report.grand_total_stock = None
            break
        serial = grid.cell(r, 0)
        # Plant rows carry a serial number in the first column; subtotal and heading rows do not.
        if not name or "TOTAL" in norm(name) or not is_number(serial) or is_number(name):
            continue  # also skips the column-number row ("1 2 3 ...")
        key = f"{report_date}|{norm(name)}"
        raw = {"row": r + 1, "station": name, "cells": [clean(v) for v in row if not is_blank(v)]}
        try:
            cells = _split_plant_tokens(_tokens(row, name_col))
            values = {
                "plant_id": plants.get(norm(name)),
                "report_date": report_date,
                "normative_stock_days": to_decimal(cells["normative_days"], 2),
                "daily_requirement_000_tonnes": to_decimal(cells["daily_requirement"], 3),
                "indigenous_stock_000_tonnes": to_decimal(cells["stock_indigenous"], 3),
                "imported_stock_000_tonnes": to_decimal(cells["stock_import"], 3),
                "actual_stock_000_tonnes": to_decimal(cells["stock_total"], 3),
                "stock_vs_normative_pct": to_decimal(cells["stock_vs_normative_pct"], 2),
                "critical_stock_flag": cells["critical"],
                "receipt_000_tonnes": to_decimal(cells["receipt"], 3),
                "consumption_000_tonnes": to_decimal(cells["consumption"], 3),
                "remarks": cells["remarks"],
            }
        except ValueError as exc:
            report.failures.append(ParseFailure(key, raw, f"Coal stock row {r + 1} ({name}): {exc}"))
            continue
        raw["capacity_mw"] = clean(cells["capacity_mw"])
        raw["plf"] = clean(cells["plf_pct"])
        raw["normative_stock_000_tonnes"] = clean(cells["normative_stock"])
        report.records.append(ParsedRecord(key, values, raw))
    if not report.records and not report.failures:
        raise InvalidResponseError("coal report contains no plant rows")
    return report
