"""Synthetic Grid-India Daily PSP workbooks and a mocked listing API / CDN.

The workbooks reproduce the *structure* of the real NLDC Daily PSP (sheet
names, section headings, header rows, label wording, the "x/ -y" max OD/UD
cells, the region label on one row of each block, section J in MOP_E1) with
made-up values. No Grid-India data is stored in the repository.
"""

from __future__ import annotations

import io
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

import httpx
import xlwt

API = "https://gi.test/api/v1"
CDN = "https://cdn.gi.test"
REGIONS = ("NR", "WR", "SR", "ER", "NER")

# Section A: label -> regional values (TOTAL is the sum unless overridden)
SECTION_A = [
    ("Demand Met (MW) during Evening Peak hr (19:00hrs)¹", [50000, 60000, 50000, 20000, 3000], None),
    ("Shoratge (MW) during Evening Peak hr (19:00hrs)¹", [0, 0, 0, 0, 0], None),
    ("Energy Met (MU)", [1100.5, 1400.25, 1200.0, 500.0, 50.0], None),
    ("Hydro Gen (MU)", [130.0, 40.0, 50.0, 30.0, 20.5], None),
    ("Wind Gen (MU)", [10.0, 70.0, 60.0, "-", "-"], None),
    ("Solar Gen (MU)*", [200.0, 220.0, 180.0, 4.0, 1.5], None),
    ("Energy Shortage (MU)", [0.03, 0.0, 0.0, 0.17, 0.0], None),
    ("Maximum Demand Met During the Day (MW)\n(From NLDC SCADA-1 min Instantaneous data)",
     [60000, 70000, 60000, 25000, 3200], 212345),
    ("Time of Maximum Demand Met", ["18:20", "09:41", "09:53", "18:01", "17:05"], "09:28"),
]


@dataclass
class AreaRow:
    region_label: str   # "" except on one row per block, as in the real sheet
    name: str
    max_demand: float = 1000.0
    shortage: float = 0.0
    energy: float = 20.0
    drawal: float = 10.0
    od_ud: float = -0.5
    max_od_ud: Any = "100/ -200"
    energy_shortage: float = 0.0


def default_section_c() -> list[AreaRow]:
    rows = [AreaRow("", n) for n in ("Punjab", "Haryana", "Rajasthan", "Delhi")]
    rows += [AreaRow("NR", "UP"), AreaRow("", "Uttarakhand"), AreaRow("", "HP"),
             AreaRow("", "J&K(UT) & Ladakh(UT)"), AreaRow("", "Chandigarh"),
             AreaRow("", "Bulk Consumer_NR ISTS", energy=6.0, drawal=5.0)]
    rows += [AreaRow("", "Chhattisgarh"), AreaRow("", "Gujarat"), AreaRow("", "MP"), AreaRow("WR", "Maharashtra"),
             AreaRow("", "Goa"), AreaRow("", "DNHDDPDCL", max_demand=1278.0, energy=27.5, drawal=27.5, od_ud=0.0,
                                         max_od_ud="65/ -108"),
             AreaRow("", "Bulk Consumer_WR ISTS", energy=30.0, drawal=29.0)]
    rows += [AreaRow("", n) for n in ("Andhra Pradesh", "Telangana")] + [AreaRow("SR", "Karnataka")]
    rows += [AreaRow("", n) for n in ("Kerala", "Tamil Nadu", "Puducherry")]
    rows += [AreaRow("", "Bihar"), AreaRow("", "DVC", drawal=-30.9), AreaRow("", "Jharkhand"), AreaRow("ER", "Odisha"),
             AreaRow("", "West Bengal"), AreaRow("", "Sikkim")]
    rows += [AreaRow("", n) for n in ("Arunachal Pradesh", "Assam", "Manipur")] + [AreaRow("NER", "Meghalaya")]
    rows += [AreaRow("", n) for n in ("Mizoram", "Nagaland", "Tripura")]
    return rows


def default_section_j() -> list[AreaRow]:
    return [AreaRow("", "Railways_NR ISTS", energy=4.0, drawal=3.0),
            AreaRow("NR", "Hindustan Zinc Ltd. Daripa", energy=2.0, drawal=2.0),
            AreaRow("", "AMNSIL", energy=10.0, drawal=10.0), AreaRow("WR", "BALCO", energy=20.0, drawal=19.0)]


def _write(sheet: Any, rows: list[list[Any]]) -> None:
    for r, row in enumerate(rows):
        for c, value in enumerate(row):
            if value not in ("", None):
                sheet.write(r, c, value)


def _area_rows(rows: list[AreaRow]) -> list[list[Any]]:
    return [[a.region_label, a.name, a.max_demand, a.shortage, a.energy, a.drawal, a.od_ud, a.max_od_ud,
             a.energy_shortage] for a in rows]


def _state_header(shortage_label: str = "Shortage at") -> list[list[Any]]:
    return [["", "", "Max.Demand", shortage_label, "Energy Met", "Drawal", "OD(+)/UD(-)", "Max", "Energy"],
            ["Region", "States", "Met during the day (MW)¹", "Maximum Demand (MW)", "(MU)", "Schedule (MU)", "(MU)",
             "OD(+)/ UD(-) (MW)", "Shortage (MU)"]]


def workbook_bytes(report_date: date, *, section_a: list | None = None, section_c: list[AreaRow] | None = None,
                   section_j: list[AreaRow] | None = None, with_j: bool = True, omit_section: str | None = None,
                   total_override: dict[str, float] | None = None, energy_scale: float = 1.0,
                   section_g: list[tuple[str, list[Any]]] | None = None, g_all_india_override: dict[str, float] | None = None,
                   g_total_override: dict[str, float] | None = None, coal_scale: float = 1.0,
                   g_padding_rows: int = 0, section_f: list[tuple[str, list[Any]]] | None = None,
                   f_total_override: dict[str, float] | None = None) -> bytes:
    """A Daily PSP workbook for operating day ``report_date`` (Date of Reporting = D+1)."""
    section_a = SECTION_A if section_a is None else section_a
    reporting = report_date + timedelta(days=1)
    rows: list[list[Any]] = [[""], [""], [""],
                             ["Report for previous day", "", "", "", "", "", "Date of Reporting:", "",
                              reporting.strftime("%d-%b-%Y")]]
    if omit_section != "A":
        rows.append(["A. Power Supply Position at All India and Regional level"])
        rows.append(["", "", *REGIONS, "TOTAL"])
        for label, values, total in section_a:
            values = [v * energy_scale if isinstance(v, float) and label.startswith("Energy Met") else v
                      for v in values]
            if total is None:
                total = sum(v for v in values if isinstance(v, (int, float)))
            if total_override and label in total_override:
                total = total_override[label]
            rows.append([label, "", *values, total])
    rows.append(["B. Frequency Profile (%)"])
    rows.append(["Region", "FVI", "< 49.7", "49.7 - 49.8", "49.8 - 49.9", "< 49.9", "49.9 - 50.05", "> 50.05"])
    rows.append(["All India", 0.05, 0.0, 1.6, 6.2, 7.8, 71.6, 20.5])
    if omit_section != "C":
        rows.append(["C. Power Supply Position in States"])
        rows += _state_header()
        rows += _area_rows(section_c if section_c is not None else default_section_c())
    rows.append([""])
    rows.append(["D. Transnational Exchanges (MU) - Import(+ve)/Export(-ve)"])
    rows.append(["", "", "Bhutan", "Nepal", "Bangladesh"])
    rows.append(["E. Import/Export by Regions (in MU) - Import(+ve)/Export(-ve); OD(+)/UD(-)"])
    rows.append(["", "", *REGIONS, "TOTAL"])
    if omit_section != "F":
        rows += _section_f_rows(section_f, f_total_override or {})
    rows += [[""]] * g_padding_rows  # shifts section G down, as different years do
    if omit_section != "G":
        rows += _section_g_rows(section_g, g_all_india_override or {}, g_total_override or {}, coal_scale)
    rows.append(["H. All India Demand Diversity Factor", "", "", "", "I. All India Peak Demand and shortage"])

    book = xlwt.Workbook()
    _write(book.add_sheet("MOP_E"), rows)
    if with_j:
        j_rows: list[list[Any]] = [[""], ["J. Power Supply Position of ISTS Bulk Consumers"], [""]]
        j_rows += _state_header("Shortage during")
        j_rows += _area_rows(section_j if section_j is not None else default_section_j())
        j_rows += [[""], ["Executive Director-NLDC"]]
        _write(book.add_sheet("MOP_E1"), j_rows)
    _write(book.add_sheet("IR-Line"), [["INTER-REGIONAL EXCHANGES"]])
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


# Section F: (sector label, regional outage MW NR..NER). TOTAL column and Total row are computed sums.
# Synthetic values with odd decimals, so they never coincide with real rows loaded for the same day.
SECTION_F = [
    ("Central Sector", [3927.041, 9636.613, 7368.017, 4030.019, 434.523]),
    ("State Sector", [9491.029, 13903.537, 7651.041, 2937.553, 276.061]),
]


def _section_f_rows(section_f: list | None, total_override: dict[str, float]) -> list[list[Any]]:
    sectors = SECTION_F if section_f is None else section_f
    numeric = lambda v: v if isinstance(v, (int, float)) else 0.0  # noqa: E731
    rows: list[list[Any]] = [["F. Generation Outage(MW)"], ["", "", *REGIONS, "TOTAL", "% Share"]]
    region_totals = [round(sum(numeric(values[i]) for _, values in sectors), 3) for i in range(len(REGIONS))]
    grand = round(sum(region_totals), 3)
    for label, values in sectors:
        total = total_override.get(label, round(sum(numeric(v) for v in values), 3))
        rows.append([label, "", *values, total, 100 * total / grand if grand else 0.0])
    rows.append(["Total", "", *[total_override.get(r, t) for r, t in zip(REGIONS, region_totals)],
                 total_override.get("TOTAL", grand), 100.0])
    rows.append([""])
    return rows


# Section G: (published fuel label, regional MU values NR..NER). All India and Total are computed sums.
# Synthetic values with odd decimals (no zeros), so they can never coincide with real rows loaded for the same
# day: each Section G record holds a single value, and an equal value would be skipped as unchanged.
SECTION_G = [
    ("Coal", [712.42, 1386.413, 634.547, 725.071, 11.719]),
    ("Lignite", [24.463, 18.127, 48.871, 0.013, 0.017]),
    ("Hydro", [131.973, 45.757, 50.391, 35.853, 22.422]),
    ("Nuclear", [52.581, 54.263, 69.267, 0.019, 0.023]),
    ("Gas, Naptha & Diesel", [8.583, 0.347, 4.301, 0.029, 21.378]),
    ("RES (Wind, Solar, Biomass & Others)", [231.817, 307.713, 268.759, 7.831, 1.709]),
]


def _section_g_rows(section_g: list | None, all_india_override: dict[str, float], total_override: dict[str, float],
                    coal_scale: float) -> list[list[Any]]:
    fuels = SECTION_G if section_g is None else section_g
    fuels = [(label, [round(v * coal_scale, 3) if label == "Coal" and isinstance(v, float) else v for v in values])
             for label, values in fuels]
    rows: list[list[Any]] = [["G. Sourcewise generation (Gross) (MU)"],
                             ["", "", *REGIONS, "All India", "% Share"]]
    numeric = lambda v: v if isinstance(v, (int, float)) else 0.0  # noqa: E731
    region_totals = [round(sum(numeric(values[i]) for _, values in fuels), 4) for i in range(len(REGIONS))]
    grand = round(sum(region_totals), 4)
    for label, values in fuels:
        all_india = all_india_override.get(label, round(sum(numeric(v) for v in values), 4))
        rows.append([label, "", *values, all_india, 100 * all_india / grand if grand else 0.0])
    totals = [total_override.get(r, t) for r, t in zip(REGIONS, region_totals)]
    rows.append(["Total", "", *totals, total_override.get("All India", grand), 100.0])
    rows.append([""])
    rows.append(["Share of RES in total generation (%)", "", 19.9, 16.9, 24.9, 1.0, 2.9, 16.7])
    rows.append(["Share of Non-fossil fuel (Hydro,Nuclear and RES) in total generation(%)", "", 35.8, 22.4, 36.0, 5.6,
                 42.1, 26.2])
    rows.append([""])
    return rows


# --------------------------------------------------------------- listing / CDN

def fy_label(day: date) -> str:
    start = day.year if day.month >= 4 else day.year - 1
    return f"{start}-{(start + 1) % 100:02d}"


@dataclass
class FileSpec:
    report_date: date
    content: bytes
    created_on: datetime | None = None
    suffix: str = "247"
    extension: str = "xls"
    listed_month: date | None = None     # listing month (default: the report month)

    @property
    def path(self) -> str:
        d = self.report_date
        return f"files/grdw/{d.year}/{d.month:02d}/{d:%d.%m.%y}_NLDC_PSP_{self.suffix}.{self.extension}"

    def entry(self, file_id: int) -> dict[str, Any]:
        created = self.created_on or datetime.combine(self.report_date + timedelta(days=1),
                                                      datetime.min.time()).replace(hour=10, minute=40)
        return {"Id": file_id, "Source_": "grdw", "FileType": "DAILY_PSP_REPORT",
                "Title_": f"{self.report_date:%d.%m.%y}_NLDC_PSP",
                "MimeType": "application/vnd.ms-excel" if self.extension == "xls" else "application/pdf",
                "FilePath": self.path, "CreatedOn": created.strftime("%d-%m-%Y %H:%M"),
                "ModifiedOn": created.strftime("%d-%m-%Y %H:%M")}


@dataclass
class MockGridIndia:
    """httpx handler: POST {API}/file listings by (FY, month), GET {CDN}/<path> files; unknown -> 404."""

    files: list[FileSpec] = field(default_factory=list)
    requests: list[httpx.Request] = field(default_factory=list)
    listing_status: int = 200
    file_overrides: dict[str, Callable[[httpx.Request], httpx.Response]] = field(default_factory=dict)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        url = str(request.url)
        if request.method == "POST" and url == f"{API}/file":
            if self.listing_status != 200:
                return httpx.Response(self.listing_status, text="error")
            body = json.loads(request.content)
            items = [f.entry(i + 1) for i, f in enumerate(self.files)
                     if fy_label(f.listed_month or f.report_date) == body["_fileDate"]
                     and f"{(f.listed_month or f.report_date).month:02d}" == body["_month"]]
            return httpx.Response(200, json={"flagType": 0, "retMessage": "Operation completed successfully.",
                                             "retData": items})
        for f in self.files:
            if url == f"{CDN}/{f.path}":
                if f.path in self.file_overrides:
                    return self.file_overrides[f.path](request)
                return httpx.Response(200, content=f.content, headers={"content-type": "application/vnd.ms-excel"})
        return httpx.Response(404, text="not found")

    @property
    def downloads(self) -> list[str]:
        return [str(r.url) for r in self.requests if r.method == "GET"]
