"""Synthetic CEA reports for tests.

The files reproduce the *structure* of the real reports (title rows, header
labels, column positions of the 2026 layout, subtotal/total rows, PDF text
lines) with made-up stations and values. No CEA data is stored in the
repository, following the precedent of the IEX and Operation-Intelligence tests.

* ``.xls`` workbooks are written with xlwt (BIFF, read back by xlrd);
* the monthly report is a minimal PDF with a Helvetica text layer.
"""

from __future__ import annotations

import io
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import httpx
import xlwt

DAY = date(2026, 11, 11)
TODAY = date(2026, 11, 13)
MONTH = date(2026, 8, 1)
NPP = "https://npp.test/public-reports/cea"
SITE = "https://cea.test"
LAST_MODIFIED = "Thu, 12 Nov 2026 10:01:04 GMT"


def xls_bytes(rows: list[list[Any]]) -> bytes:
    book = xlwt.Workbook()
    sheet = book.add_sheet("report")
    for r, row in enumerate(rows):
        for c, value in enumerate(row):
            if value not in ("", None):
                sheet.write(r, c, value)
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


def _row(width: int, cells: dict[int, Any]) -> list[Any]:
    row: list[Any] = [""] * width
    for c, value in cells.items():
        row[c] = value
    return row


def excel_serial(value: datetime) -> float:
    return (value - datetime(1899, 12, 30)).total_seconds() / 86400


# ------------------------------------------------------------- DGR sub-report 2

@dataclass
class UnitSpec:
    label: float | str
    capacity: float
    outage: float = 0.0
    outage_at: datetime | None = None
    reason: str = ""
    flag: str = ""


@dataclass
class StationSpec:
    name: str
    region: str           # DGR heading, e.g. "NORTHERN"; "BHUTAN IMP." for the import section
    state: str            # e.g. "HARYANA"; "BHUTAN"
    plant_type: str = "THERMAL"
    sector: str = "STATE SECTOR"
    capacity: float = 0.0
    program: float = 0.0
    actual: float = 0.0
    ytd_program: float = 0.0
    ytd_actual: float = 0.0
    coal_days: float | None = None
    outage: float = 0.0
    remarks: str = ""
    units: list[UnitSpec] = field(default_factory=list)


def default_stations() -> list[StationSpec]:
    return [
        StationSpec("TEST ALPHA TPS", "NORTHERN", "HARYANA", capacity=460.0, program=8.5, actual=6.25,
                    ytd_program=1500.5, ytd_actual=1400.25, coal_days=7.0, outage=210.0,
                    units=[UnitSpec(1.0, 210.0, 210.0, datetime(2026, 11, 8, 9, 58), "RESERVE SHUT DOWN", "S"),
                           UnitSpec(2.0, 250.0)]),
        StationSpec("TEST BETA HPS", "NORTHERN", "HARYANA", plant_type="HYDRO", capacity=100.0, program=1.2,
                    actual=1.1, ytd_program=300.0, ytd_actual=310.0, units=[UnitSpec(1.0, 100.0)]),
        StationSpec("TEST GAMMA TPS", "WESTERN", "CHHATISGARH", sector="PVT SECTOR", capacity=660.0, program=12.0,
                    actual=11.5, ytd_program=2000.0, ytd_actual=1950.75, coal_days=12.0,
                    units=[UnitSpec(1.0, 660.0)]),
        StationSpec("TEST DRUK HPS", "BHUTAN IMP.", "BHUTAN", plant_type="HYDRO", capacity=0.0, actual=2.0),
    ]


def dgr2_rows(day: date = DAY, stations: list[StationSpec] | None = None) -> list[list[Any]]:
    stations = default_stations() if stations is None else stations
    w = 18
    rows = [
        _row(w, {6: "CENTRAL ELECTRICITY AUTHORITY \n           G O & D WING"}),
        _row(w, {0: f"REGION WISE, STATE WISE, SECTOR WISE, TYPE WISE, STATION WISE, UNIT WISE GENERATION REPORT "
                    f"FOR     {day:%d/%m/%Y}", 17: "Subreport-2"}),
        _row(w, {0: "POWER STATION", 7: "MONITORED\nCAP.\n(MW)", 8: "GENERATION  (MU)", 12: "COAL STOCK\nIN DAYS",
                 13: "CAP. UNDER\nOUTAGE\n(MW)", 14: "OUTAGE DATE\n(DD/MM/YYYY HH24:MI)",
                 16: "EXPECTED DATE /\nSYNC. DATE", 17: "REMARKS"}),
        _row(w, {8: "TODAY'S\nPROGRAM", 9: "TODAY'S\nACTUAL", 10: "APRIL 1 TILL DATE (2026-27)"}),
        _row(w, {10: "PROG.", 11: "ACTUAL", 12: float((day - date(1899, 12, 30)).days)}),
    ]
    region = state = None
    for s in stations:
        if s.region != region:
            region, state = s.region, None
            rows.append(_row(w, {0: s.region}))
            rows.append(_row(w, {0: "REGION TOTAL", 7: 1.0, 8: 1.0}))
        if s.state != state:
            state = s.state
            rows.append(_row(w, {0: s.state}))
            rows.append(_row(w, {0: "STATE TOTAL", 7: 1.0, 8: 1.0}))
        rows.append(_row(w, {0: "              SECTOR: ", 4: s.sector, 7: s.capacity}))
        rows.append(_row(w, {0: "             TYPE:", 4: s.plant_type, 7: s.capacity}))
        rows.append(_row(w, {0: s.name, 7: s.capacity, 8: s.program, 9: s.actual, 10: s.ytd_program,
                             11: s.ytd_actual, 12: s.coal_days if s.coal_days is not None else "",
                             13: s.outage, 17: s.remarks}))
        for u in s.units:
            rows.append(_row(w, {0: "Unit", 2: u.label, 5: u.flag, 7: u.capacity, 8: 1.0, 9: 1.0, 10: 1.0, 11: 1.0,
                                 13: u.outage, 14: excel_serial(u.outage_at) if u.outage_at else "", 17: u.reason}))
    rows.append(_row(w, {0: "Note:  1.Actual generation is gross generation from stations above 25 MW."}))
    rows.append(_row(w, {1: "Report Version :-"}))
    return rows


# ------------------------------------------------------------ DGR sub-report 10

@dataclass
class OutageSpec:
    state: str
    station: str
    unit: float | str
    planned: float = 0.0
    forced_major: float = 0.0
    forced_minor: float = 0.0
    others: float = 0.0
    start: str = "08/11/2026 09:58"
    sync: str = ""
    reason: str = "RESERVE SHUT DOWN / STANDBY UNIT"


def default_outages() -> list[OutageSpec]:
    return [
        OutageSpec("Haryana", "TEST ALPHA TPS", 1.0, forced_minor=210.0),
        OutageSpec("Haryana", "TEST ALPHA TPS", 2.0, planned=250.0, start="24/10/2026 00:00",
                   reason="BOILER OVERHAUL/ ANNUAL MAINTENANCE"),
        OutageSpec("Chhatisgarh", "TEST GAMMA TPS", 1.0, forced_major=660.0, start="13/10/2026 15:30",
                   sync="20/11/2026 00:00", reason="TURBINE OIL SYSTEM PROBLEM"),
    ]


def dgr10_rows(day: date = DAY, outages: list[OutageSpec] | None = None, total: float | None = None) -> list[list[Any]]:
    outages = default_outages() if outages is None else outages
    w = 12
    rows = [
        _row(w, {0: "CENTRAL ELECTRICITY AUTHORITY \n           G O & D WING"}),
        _row(w, {0: f"DAILY MAINTENANCE REPORT (COAL, LIGNITE AND NUCLEAR)  {day:%d-%m-%Y}"}),
        _row(w, {9: "Sub-Report-10"}),
        _row(w, {}),
        _row(w, {0: "State/System", 3: "Power Station", 4: "Unit No.", 5: "Planned Maintenance (MW)",
                 6: "Forced Maintenance (Major) (MW)", 7: "Forced Maintenance (Minor) (MW)",
                 8: "Others Maintenance (MW)", 9: "Date& Time of Maintenance", 10: "Expected/sync Date of Return",
                 11: "Reasons/Present Status"}),
        _row(w, {10: "DD/MM/YYYY HH24:MM"}),
        _row(w, {9: "DD/MM/YYYY HH24:MM"}),
        _row(w, {0: "1", 3: "2", 4: "3", 5: "4", 6: "5", 7: "6", 8: "7", 9: "8", 10: "9", 11: "10"}),
        _row(w, {0: "Northern"}),
    ]
    for o in outages:
        rows.append(_row(w, {0: o.state, 3: o.station, 4: o.unit, 5: o.planned, 6: o.forced_major,
                             7: o.forced_minor, 8: o.others, 9: o.start, 10: o.sync, 11: o.reason}))
    sums = [sum(getattr(o, a) for o in outages) for a in ("planned", "forced_major", "forced_minor", "others")]
    if total is not None:
        sums = [total, 0.0, 0.0, 0.0]
    rows.append(_row(w, {0: "Total Northern", 5: sums[0], 6: sums[1], 7: sums[2], 8: sums[3]}))
    rows.append(_row(w, {0: "Total", 5: sums[0], 6: sums[1], 7: sums[2], 8: sums[3]}))
    rows.append(_row(w, {0: "Note: 1.Actual generation is gross generation from stations above 25 MW. "
                            "2.Data published in this report is for operational purpose only and not for "
                            "commercial purpose."}))
    return rows


# -------------------------------------------------------------- coal stock

@dataclass
class CoalSpec:
    state: str
    genco: str
    name: str
    capacity: float
    plf: str
    normative_days: float
    daily_requirement: float
    normative_stock: float
    indigenous: float
    imported: float
    total: float
    pct: str
    critical: bool
    receipt: float
    consumption: float
    remarks: str = ""


def default_coal() -> list[CoalSpec]:
    return [
        CoalSpec("Haryana", "HPGCL", "TEST ALPHA TPS", 460.0, "52%", 22.0, 10.283639999999998, 226.2, 88.7, 0.0,
                 88.7, "39%", False, 12.4, 0.0),
        CoalSpec("Chhatisgarh", "IPP", "TEST GAMMA TPS", 660.0, "60%", 22.0, 17.072352, 375.6, 70.0, 13.9, 83.9,
                 "22%", True, 12.0, 8.1, "Plant to augment coal supplies from its captive mines."),
    ]


def coal_rows(day: date = DAY, plants: list[CoalSpec] | None = None, grand_total: float | None = None,
              extra_rows: list[list[Any]] | None = None) -> list[list[Any]]:
    plants = default_coal() if plants is None else plants
    w = 40
    rows = [
        _row(w, {0: "CENTRAL ELECTRICITY AUTHORITY"}),
        _row(w, {0: "FUEL MANAGEMENT DIVISION"}),
        _row(w, {38: f"(AS ON {day:%d-%m-%Y})"}),
        _row(w, {0: "Sl. No./State", 1: "State", 2: "GENCO", 4: "Mode of Transport",
                 6: "Name of Thermal Power Station/Performance of Utility #", 8: "Capacity (MW)",
                 9: "Current Month PLF (Tentative)", 12: "Normative Stock Reqd.(Days)",
                 14: "Daily Requirement @85% PLF (In '000 Tonnes)", 17: "Normative Stock Reqd.(In '000 Tonnes')",
                 18: "Actual Stock (In 000 Tonnes)", 27: "% of Actual Stock vis-a-vis Normative  Stock",
                 29: "Critical (*)", 31: "Receipt of the day('000 T)", 32: "Consumption of the day ('000 T)",
                 35: "Reasons for critical coal stock/Remarks"}),
        _row(w, {18: "Indigenous", 21: "Import", 24: "Total"}),
        _row(w, {0: "1", 1: "2", 3: "3", 4: "4", 6: "5", 8: "6", 9: "7", 12: "8", 14: "9", 17: "10", 18: "11",
                 23: "13", 27: "14", 29: "15", 31: "16", 32: "17", 35: "18"}),
        _row(w, {0: "A. PLANT HAVING COAL LINKAGES, NO LINKAGES AND COAL BLOCK"}),
    ]
    for i, p in enumerate(plants, start=1):
        rows.append(_row(w, {0: float(i), 1: p.state, 2: p.genco, 4: "RAIL", 6: p.name, 8: p.capacity, 9: p.plf,
                             12: p.normative_days, 14: p.daily_requirement, 17: p.normative_stock, 18: p.indigenous,
                             21: p.imported, 24: p.total, 27: p.pct, 29: "*" if p.critical else "",
                             31: p.receipt, 32: p.consumption, 35: p.remarks}))
        rows.append(_row(w, {6: f"{p.genco}-Total", 8: p.capacity, 9: p.plf, 12: 22.0, 14: 1.0, 17: 1.0, 18: 1.0,
                             21: 0.0, 24: 1.0, 27: "1%", 29: 0.0, 31: 0.0, 32: 0.0}))
    rows.extend(extra_rows or [])
    total = sum(p.total for p in plants) if grand_total is None else grand_total
    rows.append(_row(w, {6: "Grand Total = A+B+C+D", 8: 1120.0, 9: "64%", 12: 20.5, 17: 601.8, 18: 158.7, 21: 13.9,
                         24: total, 27: "35%", 29: 1.0, 31: 24.4, 32: 8.1}))
    rows.append(_row(w, {0: "Sl.No.", 1: "Plants Category", 6: "Capacity (MW)"}))
    rows.append(_row(w, {0: "NOTE: (i) *Plant has been shown critical if coal stock is less than 25% of normative"}))
    return rows


# ---------------------------------------------------------- monthly PSP (PDF)

def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def pdf_bytes(lines: list[str]) -> bytes:
    """A one-page PDF whose text layer is ``lines`` (one per text line, top to bottom)."""
    ops = ["BT", "/F1 8 Tf"]
    y = 800
    for line in lines:
        ops.append(f"1 0 0 1 20 {y} Tm ({_pdf_escape(line)}) Tj")
        y -= 14
    ops.append("ET")
    stream = "\n".join(ops).encode("latin-1")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 842 842] /Contents 4 0 R "
        b"/Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = io.BytesIO()
    out.write(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, start=1):
        offsets.append(out.tell())
        out.write(f"{number} 0 obj\n".encode() + body + b"\nendobj\n")
    xref = out.tell()
    out.write(f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode())
    for offset in offsets:
        out.write(f"{offset:010d} 00000 n \n".encode())
    out.write(f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode())
    return out.getvalue()


MONTH_NAMES = ("January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
               "November", "December")

# (label line, numbers) in report order; a None label means the name wraps onto the next line.
DEFAULT_PSP_ROWS: list[tuple[str, str]] = [
    # Synthetic figures (deliberately not real values), consistent: requirement - supplied = not supplied.
    ("/ Chandigarh", "111 111 0 0.0 555 555 0 0.0"),
    (" / Haryana", "8,611 8,600 11 0.1 37,711 37,550 161 0.4"),
    ("WRAP:UT of J&K and Ladakh", "1,511 1,501 10 0.7 7,911 7,880 31 0.4"),
    (" / Northern Region (##)", "10,233 10,212 21 0.2 46,177 45,985 192 0.4"),
    (" / Gujarat", "14,711 14,700 11 0.1 74,911 74,850 61 0.1"),
    ("WRAP:Dadra & Nagar Haveli and Daman & Diu", "911 911 0 0.0 4,511 4,511 0 0.0"),
    ("Others", "1,311 1,311 0 0.0 6,611 6,611 0 0.0"),
    (" / Western Region (##)", "16,933 16,922 11 0.1 86,033 85,972 61 0.1"),
    (" / DVC", "2,111 2,111 0 0.0 10,711 10,709 2 0.0"),
    ("-/ Andaman- Nicobar (#) (**)", "31 31 0 0.0 181 179 2 1.1"),
    (" / Eastern Region (##)", "2,142 2,142 0 0.0 10,892 10,888 4 0.0"),
    ("BEFORE:- / North-Eastern Region (##)", "2,311 2,309 2 0.1 10,211 10,198 13 0.1"),
    (" / All India", "1,61,111 1,60,911 200 0.1 8,11,111 8,10,011 1,100 0.1"),
]


def psp_lines(measure: str = "ENERGY", month: date = MONTH, rows: list[tuple[str, str]] | None = None,
              april_only: bool = False, title: str | None = None) -> list[str]:
    rows = DEFAULT_PSP_ROWS if rows is None else rows
    name = MONTH_NAMES[month.month - 1]
    unit = "MU" if measure == "ENERGY" else "MW"
    if measure == "ENERGY":
        cols = "Energy Requirement Energy Supplied Energy not Supplied"
    else:
        cols = "Peak Demand Peak Met Demand not Met"
    lines = [title or f" () /{measure.title()} Power Supply Position Report (Revised)",
             f" / Figures in {unit} net"]
    if april_only:
        lines += [f",{month.year} / {name},{month.year}", "/State", f" / System {cols}",
                  f" / Region ( {unit} ) ( {unit} ) ( {unit} ) ( % )"]
    else:
        lines += [f",{month.year} / {name},{month.year} ,{month.year} - ,{month.year}/ April,{month.year} to "
                  f"{name},{month.year}", "/State", f" / System {cols} {cols}",
                  f" / Region ( {unit} ) ( {unit} ) ( {unit} ) ( % ) ( {unit} ) ( {unit} ) ( {unit} ) ( % )"]
    for label, numbers in rows:
        values = " ".join(numbers.split()[:4]) if april_only else numbers
        if label.startswith("WRAP:"):
            lines += [" /", values, label[5:]]
        elif label.startswith("BEFORE:"):
            lines += [label[7:], values]
        else:
            lines.append(f"{label} {values}")
    lines += ["(#) Lakshadweep and Andaman & Nicobar Islands are stand- alone systems.",
              "Note:", "1. Power Supply Position Report has been compiled based on the data furnished by States."]
    return lines


def listing_html(title: str, entries: list[tuple[str, str]]) -> bytes:
    """A listing page: ``entries`` are (YYYY-MM, uuid)."""
    rows = "".join(
        f"<tr><td>{i}</td><td>{title}</td><td>{month}</td><td><a href=\"/api/dashboardReports/download-pdf/en/"
        f"{uuid}\">(431.98 KB)</a></td><td><a href=\"/api/dashboardReports/view-pdf/en/{uuid}\">view</a></td>"
        f"<td>--</td></tr>" for i, (month, uuid) in enumerate(entries, start=1))
    return (f"<!DOCTYPE html><html><body><table><tr><th>{title}</th></tr><tr><th>sr no</th><th>Report Name</th>"
            f"<th>Months</th><th>Download</th><th>View</th><th>Excel</th></tr>{rows}</table></body></html>").encode()


ENERGY_UUID = "cfc1705f-1a05-4dac-af5b-e7d92b4209c6"
PEAK_UUID = "534c16e0-515c-42cb-9609-47b52c753d91"


# ------------------------------------------------------------------ transport

Response = tuple[int, bytes, dict[str, str]]


class MockCea:
    """httpx handler serving ``routes`` (url -> response or callable); unknown URLs return 404."""

    def __init__(self, routes: dict[str, Response | Callable[[httpx.Request], httpx.Response]] | None = None) -> None:
        self.routes = routes or {}
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        route = self.routes.get(str(request.url))
        if route is None:
            return httpx.Response(404, text="<!doctype html><html>Not Found</html>")
        if callable(route):
            return route(request)
        status, content, headers = route
        return httpx.Response(status, content=content, headers=headers)


def xls_response(rows: list[list[Any]]) -> Response:
    return 200, xls_bytes(rows), {"content-type": "application/vnd.ms-excel", "last-modified": LAST_MODIFIED}


def daily_url(stem: str, day: date = DAY) -> str:
    folder = "fuel" if stem == "dailyCoal1" else "dgr"
    return f"{NPP}/daily/{folder}/{day:%d-%m-%Y}/{stem}-{day:%Y-%m-%d}.xls"


def listing_url(sub_sub_heading_id: int) -> str:
    return (f"{SITE}/reports/dashboardView?headingId=107&subHeadingId=1003&subSubHeadingId={sub_sub_heading_id}"
            "&lang=en")


def pdf_url(uuid: str) -> str:
    return f"{SITE}/api/dashboardReports/download-pdf/en/{uuid}"
