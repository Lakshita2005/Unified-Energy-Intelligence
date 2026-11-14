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
CB_FIRST_PUBLISHED = date(2023, 7, 6)

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
                   f_total_override: dict[str, float] | None = None,
                   section_e: list[tuple[str, list[Any]]] | None = None,
                   e_total_override: dict[str, float] | None = None, e_heading: str | None = None,
                   ir_blocks: list | None = None, intl_rows: list | None = None,
                   ir_subtotal_override: dict[str, tuple[float, float, float]] | None = None,
                   section_d_override: dict[str, float] | None = None,
                   intl_heading_convention: str = "Import(+ve)/Export(-ve)",
                   cb: dict[str, Any] | None = None, with_cb: bool | None = None,
                   ts: dict[str, Any] | None = None, with_ts: bool | None = None) -> bytes:
    """A Daily PSP workbook for operating day ``report_date`` (Date of Reporting = D+1).

    The CrossBorder sheet is included from 2023-07-06 (as published) unless ``with_cb`` says otherwise or
    ``omit_section="CB"``; ``cb`` holds keyword arguments for :func:`crossborder_rows`. Likewise TimeSeries from
    2024-11-04 (``with_ts``, ``omit_section="TS"``, ``ts`` for :func:`timeseries_rows`).
    """
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
    actuals = {**section_d_actuals(intl_rows), **(section_d_override or {})}
    rows.append(["", "", *actuals.keys()])
    rows.append(["Actual (MU)", "", *actuals.values()])
    if omit_section != "E":
        rows += _section_e_rows(section_e, e_total_override or {}, e_heading or E_HEADING)
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
    if omit_section != "IR":
        _write(book.add_sheet("IR-Line"), ir_line_rows(report_date, blocks=ir_blocks, intl_rows=intl_rows,
                                                       subtotal_override=ir_subtotal_override,
                                                       intl_heading_convention=intl_heading_convention))
    if with_cb is None:
        with_cb = report_date >= CB_FIRST_PUBLISHED
    if with_cb and omit_section != "CB":
        _write(book.add_sheet("CrossBorder"), crossborder_rows(report_date, **(cb or {})))
    if with_ts is None:
        with_ts = report_date >= TS_FIRST_PUBLISHED
    if with_ts and omit_section != "TS":
        _write(book.add_sheet("TimeSeries"), timeseries_rows(report_date, **(ts or {})))
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


# IR-Line, inter-regional part: (from, to, [(voltage, line, circuits, max import MW, max export MW, import MU,
# export MU[, published NET override])], lines left out of the published subtotal). Synthetic values.
IR_BLOCKS = [
    ("ER", "NR", [("765 kV", "TEST GAYA-VARANASI", "2", 357.013, 1339.017, 0.013, 17.217),
                  ("HVDC", "TEST PUSAULI B/B", "-", 0.0, 47.011, 0.0, 2.451)], ()),
    ("ER", "SR", [("HVDC", "TEST TALCHER-KOLAR BIPOLE", "2", 0.0, 1500.019, 0.0, 36.751),
                  ("400 kV", "TEST TALCHER-I/C", "2", 0.0, 210.023, 0.0, 5.031),
                  ("400 kV", "TEST ANGUL-SRIKAKULAM", "4", 0.0, 2000.029, 0.0, 47.233)], ("TEST TALCHER-I/C",)),
    ("WR", "NR", [("765 kV", "TEST GWALIOR-AGRA", "2", 1200.031, 0.0, 25.537, 0.0)], ()),
]
# IR-Line, international part: (State label, Region cell, line name, Max MW, Min MW, Avg MW, energy MU).
# As in the real sheet, the country label is printed on one row of each group only. Line names are real
# published names (the country reference is keyed on them); the values are synthetic.
INTL_ROWS = [
    ("", "ER", "400kV TALA-BINAGURI 1,2 ,400kV MALBASE - BINAGURI & 400kV NORBUGANG - BINAGURI i.e. BINAGURI "
               "RECEIPT (from TALA HEP 6*170MW)", 0.0, 0.0, 0.0, 0.421),
    ("BHUTAN", "ER", "220kV CHUKHA-BIRPARA 1&2 (& 220kV MALBASE - BIRPARA) i.e. BIRPARA RECEIPT (from CHUKHA HEP "
                     "4*84MW)", 0.0, 0.0, 0.0, -3.661),
    ("", "NER", "132kV GELEPHU-SALAKATI", -74.013, 0.0, -58.751, -1.411),
    ("", "NR", "NEPAL IMPORT (FROM UP)", 12.017, 0.0, 9.013, 0.217),
    ("NEPAL", "NR", "132kV MAHENDRANAGAR-TANAKPUR(NHPC)", 68.761, 0.0, 61.013, 1.471),
    ("", "ER", "400kV DHALKEBAR-MUZAFFARPUR 1&2", 606.013, 476.017, 532.917, 12.793),
    ("", "ER", "BHERAMARA B/B HVDC (B'DESH)", -974.013, -954.017, -966.671, -23.201),
    ("BANGLADESH", "ER\n(Isolated from Indian Grid)", "400kV GODDA_TPS-RAHANPUR (B'DESH) D/C", -947.013, 0.0,
     -863.333, -20.721),
    ("", "NER", "132kV COMILLA-SURAJMANI NAGAR 1&2", -101.013, 0.0, -86.667, -2.081),
]
COUNTRY_OF_LABEL = {"BHUTAN": ("BHUTAN", 0, 3), "NEPAL": ("NEPAL", 3, 6), "BANGLADESH": ("BANGLADESH", 6, 9)}


def section_d_actuals(intl_rows: list | None = None) -> dict[str, float]:
    """Section D 'Actual (MU)' consistent with the default international rows (Godda reported separately)."""
    rows = INTL_ROWS if intl_rows is None else intl_rows
    out = {"Bhutan": 0.0, "Nepal": 0.0, "Bangladesh": 0.0, "Godda -> Bangladesh": 0.0}
    groups = [("Bhutan", 0, 3), ("Nepal", 3, 6), ("Bangladesh", 6, 9)]
    for name, lo, hi in groups:
        for row in rows[lo:hi]:
            key = "Godda -> Bangladesh" if "Isolated" in row[1] else name
            out[key] = round(out[key] + row[6], 4)
    return out


def ir_line_rows(report_date: date, *, blocks: list | None = None, intl_rows: list | None = None,
                 subtotal_override: dict[str, tuple[float, float, float]] | None = None,
                 intl_heading_convention: str = "Import(+ve)/Export(-ve)",
                 net_convention: str = "Import=(+ve) /Export =(-ve) for NET (MU)") -> list[list[Any]]:
    blocks = IR_BLOCKS if blocks is None else blocks
    intl_rows = INTL_ROWS if intl_rows is None else intl_rows
    reporting = report_date + timedelta(days=1)
    rows: list[list[Any]] = [
        ["INTER-REGIONAL EXCHANGES"], [net_convention],
        ["", "", "", "", "", "", "Date of Reporting:", "", "", reporting.strftime("%d-%b-%Y")],
        ["Sl No", "Voltage Level", "Line Details", "No. of Circuit", "Max Import (MW)", "Max Export (MW)",
         "Import (MU)", "Export (MU)", "", "NET (MU)"],
    ]
    for frm, to, lines, excluded in blocks:
        rows.append([f"Import/Export of {frm} (With {to})"])
        totals = [0.0, 0.0, 0.0]
        for n, line in enumerate(lines, start=1):
            voltage, name, circuits, max_imp, max_exp, imp, exp = line[:7]
            net = line[7] if len(line) > 7 else round(imp - exp, 4)
            rows.append([float(n), voltage, name, circuits, max_imp, max_exp, imp, exp, "", net])
            if name not in excluded:
                totals = [round(totals[0] + imp, 4), round(totals[1] + exp, 4), round(totals[2] + net, 4)]
        label = f"{frm}-{to}"
        imp_t, exp_t, net_t = (subtotal_override or {}).get(label, tuple(totals))
        rows.append([label, "", "", "", "", "", imp_t, exp_t, "", net_t])
    rows.append([""])
    rows.append(["INTERNATIONAL EXCHANGES", "", "", "", "", "", "", "", intl_heading_convention])
    rows.append(["State", "", "Region", "Line Name", "", "Max (MW)", "Min (MW)", "Avg (MW)", "", "Energy Exchange (MU)"])
    for label, region, name, mx, mn, avg, energy in intl_rows:
        rows.append([label, "", region, name, "", mx, mn, avg, "", energy])
    return rows


# Section E: (row label, regional MU values NR..NER). Schedules balance to 0 across regions, as published.
# O/D/U/D is published rounded independently of Actual - Schedule (by up to ~0.013 MU), as in the real reports.
# Synthetic values with odd decimals, so they never coincide with real rows loaded for the same day.
SECTION_E = [
    ("Schedule(MU)", [121.483, -237.497, 274.141, -162.401, 4.274]),
    ("Actual(MU)", [125.307, -235.941, 275.003, -184.593, 2.817]),
    ("O/D/U/D(MU)", [3.82, 1.55, 0.86, -22.2, -1.46]),
]
E_HEADING = "E. Import/Export by Regions (in MU) - Import(+ve)/Export(-ve); OD(+)/UD(-)"


# CrossBorder sheet (from report date 2023-07-06). Synthetic values, 2 decimals. Component order per row:
# GNA (or pre-2023-10 PPA), T-GNA (or STOA) bilateral, IDAM IEX/PXIL/HPX, RTM IEX/PXIL/HPX -> columns below.
CB_COMPONENT_COLUMNS = (1, 3, 4, 5, 6, 7, 9, 10)
CB_TOTAL_COLUMN = 12
CB_COUNTRIES = ("Bhutan", "Nepal", "Bangladesh", "Myanmar")
# Every synthetic value has 3 decimals (published values have at most 2) and none is 0, so the fixture can never
# coincide with rows already loaded in the database the integration tests run against.
CB_ROWS: dict[str, dict[str, list[float]]] = {
    "EXPORT": {"Bhutan": [0.001, 0.31, 2.07, 0.002, 0.003, 0.004, 0.005, 0.006],
               "Nepal": [0.007, 0.008, 0.009, 0.011, 0.012, 1.25, 0.013, 0.014],
               "Bangladesh": [24.41, 0.07, 0.015, 0.016, 0.017, 0.018, 0.019, 0.021],
               "Myanmar": [0.022, 0.023, 0.024, 0.025, 0.026, 0.027, 0.028, 0.029]},
    "IMPORT": {"Bhutan": [0.81, 0.031, 0.032, 0.033, 0.034, 0.035, 0.036, 0.037],
               "Nepal": [0.89, 0.05, 0.038, 0.039, 0.041, 10.53, 0.042, 0.043],
               "Bangladesh": [0.004, 0.044, 0.045, 0.046, 0.047, 0.048, 0.049, 0.051],
               "Myanmar": [0.052, 0.053, 0.054, 0.055, 0.056, 0.057, 0.058, 0.059]},
}
CB_HEADINGS = {"EXPORT": "Export From India (in MU)", "IMPORT": "Import by India(in MU)",
               "NET": "Net from India(in MU)" + " " * 60 + "-ve : Export / +ve : Import"}


def crossborder_rows(report_date: date, *, layout: str = "GNA", rows: dict | None = None,
                     labels: tuple[str, ...] = CB_COUNTRIES, net_override: dict[tuple[str, int], float] | None = None,
                     total_override: dict[tuple[str, str], float] | None = None,
                     header_override: dict[tuple[int, int], str] | None = None,
                     net_heading: str | None = None, value_override: dict[tuple[str, str, int], Any] | None = None
                     ) -> list[list[Any]]:
    """The CrossBorder sheet as rows. ``labels`` are the printed country names (rows reuse CB_ROWS by position).

    Overrides: ``net_override[(country, column)]`` published Net cell; ``total_override[(block, country|'Total')]``
    published row TOTAL; ``header_override[(row offset in block 1..4, column)]`` header text (all blocks);
    ``value_override[(block, country, column)]`` any cell value (e.g. '' or 'n/a').
    """
    rows = CB_ROWS if rows is None else rows
    width = 13
    out: list[list[Any]] = [[""] * width for _ in range(1)]
    out[0][2] = "CROSS BORDER EXCHANGE SCHEDULE"
    out[0][8] = "Date of Reporting:"
    out[0][11] = (report_date + timedelta(days=1)).strftime("%d-%b-%Y")
    for block in ("EXPORT", "IMPORT", "NET"):
        header = [[""] * width for _ in range(4)]
        if layout == "GNA":
            header[0][3], header[1][1] = "T-GNA", "GNA"
            header[2][1] = "(ISGA/PPA)" if block == "IMPORT" else "(ISGS/PPA)"
        else:
            header[0][3], header[1][1], header[2][1] = "STOA", "(ISGS/LTA/MTOA)", "PPA"
        header[1][4] = "COLLECTIVE"
        header[2][0], header[2][3], header[2][4], header[2][7], header[2][12] = "Country", "BILATERAL", "IDAM", "RTM", "TOTAL"
        for c, text in zip((3, 4, 5, 6, 7, 9, 10), ("TOTAL", "IEX", "PXIL", "HPX", "IEX", "PXIL", "HPX")):
            header[3][c] = text
        for (offset, c), text in (header_override or {}).items():
            header[offset - 1][c] = text
        heading = net_heading if (block == "NET" and net_heading is not None) else CB_HEADINGS[block]
        out.append([heading] + [""] * (width - 1))
        out += header
        totals = [0.0] * 8
        for position, label in enumerate(labels):
            base = CB_COUNTRIES[min(position, len(CB_COUNTRIES) - 1)]
            if block == "NET":
                values = [round(i - e, 3) for i, e in zip(rows["IMPORT"][base], rows["EXPORT"][base])]
                for col_index, col in enumerate(CB_COMPONENT_COLUMNS):
                    if (label, col) in (net_override or {}):
                        values[col_index] = net_override[(label, col)]
            else:
                values = list(rows[block][base])
            row = [""] * width
            row[0] = label
            for col, v in zip(CB_COMPONENT_COLUMNS, values):
                row[col] = v
            row[CB_TOTAL_COLUMN] = (total_override or {}).get((block, label), round(sum(values), 3))
            for col in CB_COMPONENT_COLUMNS + (CB_TOTAL_COLUMN,):
                if (block, label, col) in (value_override or {}):
                    row[col] = value_override[(block, label, col)]
            totals = [round(t + v, 3) for t, v in zip(totals, values)]
            out.append(row)
        total_row = [""] * width
        total_row[0] = {"EXPORT": "Total Export", "IMPORT": "Total Import", "NET": "Total Net"}[block]
        for col, v in zip(CB_COMPONENT_COLUMNS, totals):
            total_row[col] = v
        total_row[CB_TOTAL_COLUMN] = (total_override or {}).get((block, "Total"), round(sum(totals), 3))
        out.append(total_row)
        out.append([""] * width)
    return out[:-1]


# TimeSeries sheet (from report date 2024-11-04). Synthetic values. Layouts as published: 13 columns, 15 (+ storage
# generation, 2026-05-06..31) and 16 (+ storage demand, from 2026-06-01).
TS_FIRST_PUBLISHED = date(2024, 11, 4)
TS_EXCHANGE_HEADINGS = {"13": "NET TRANSNATIONAL EXCHANGE\n(MW)\n(+ve) Import, (-ve) Export",
                        "15": "NET TRANSNATIONAL EXCHANGE\n(MW)\n(+ve) Import, (-ve) Export",
                        "16": "NET \nTRANSNATIONAL\nEXCHANGE\n(MW)\n+ve Imp,-ve Exp"}


def ts_layout_for(report_date: date) -> str:
    return "13" if report_date < date(2026, 5, 6) else "15" if report_date < date(2026, 6, 1) else "16"


def ts_values(index: int) -> dict[str, float]:
    """Synthetic, internally consistent values for mark ``index`` (0 = 0:00 ... 95 = 23:45)."""
    v = {"FREQUENCY": round(50.0 + ((index % 5) - 2) * 0.01, 2), "NUCLEAR": 6500.0, "WIND": 5000.0 + 10 * index,
         "SOLAR": float(max(0, 40000 - abs(index - 50) * 1600)), "HYDRO": 10000.0 + 20 * index, "GAS": 1800.0,
         "THERMAL": 140000.0 - 100 * (index % 7), "OTHERS": 1400.0, "STORAGE": 300.0 if index % 4 else 0.0,
         "STORAGE DEMAND": 250.0 if index < 24 else 0.0, "EXCHANGE": -100.0 + index}
    return v


def timeseries_rows(report_date: date, *, layout: str | None = None, time_format: str = "text",
                    drop_marks: tuple[int, ...] = (), empty: bool = False,
                    value_override: dict[tuple[int, str], Any] | None = None,
                    total_excludes_storage: bool = False, exchange_heading: str | None = None,
                    extra_header: str | None = None, duplicate_mark: int | None = None) -> list[list[Any]]:
    """The TimeSeries sheet as rows. ``value_override[(index, NAME)]`` replaces a published cell (NAME as in
    :func:`ts_values` or 'DEMAND MET' / 'NET DEMAND MET' / 'TOTAL GENERATION'); ``time_format`` is 'text' ('0:15'),
    'seconds' ('0:15:00') or 'fraction_text' ('0.010416...' as text, as on 2024-11-05)."""
    layout = layout or ts_layout_for(report_date)
    names = ["TIME", "FREQUENCY", "DEMAND MET"] + (["STORAGE DEMAND"] if layout == "16" else []) + \
        ["NUCLEAR", "WIND", "SOLAR", "HYDRO", "GAS", "THERMAL"] + (["STORAGE"] if layout in ("15", "16") else []) + \
        ["OTHERS", "NET DEMAND MET", "TOTAL GENERATION"]
    labels = {"TIME": "TIME", "FREQUENCY": "FREQUENCY\n(Hz)", "DEMAND MET": "DEMAND\nMET¹\n(MW)",
              "STORAGE DEMAND": "STORAGE\nDEMAND\n(PSP&BESS)\n(MW)", "HYDRO": "HYDRO²\n(MW)",
              "STORAGE": "STORAGE\n(PSP&BESS)\n(MW)", "OTHERS": "OTHERS³\n(MW)"}
    width = len(names) + (2 if layout != "13" else 1)
    exchange_col = width - 1
    header = [labels.get(n, f"{n}\n(MW)") for n in names] + [""] * (width - len(names))
    header[exchange_col] = exchange_heading if exchange_heading is not None else TS_EXCHANGE_HEADINGS[layout]
    if extra_header:
        header.insert(len(names), extra_header)
        header = header[:width + 1]
    formula = [""] * width
    for c in range(2, len(names)):
        formula[c] = f"({chr(ord('A') + c - 2)})"
    formula[exchange_col] = f"({chr(ord('A') + len(names) - 2)})"
    title = [""] * width
    title[0] = "Date of Reporting:"
    title[width - 2] = (report_date + timedelta(days=1)).strftime("%d-%b-%Y")
    rows: list[list[Any]] = [title, ["15 Min (INSTANTANEOUS) ALL INDIA GRID FREQUENCY, GENERATION & DEMAND MET "
                                     "(SCADA DATA)"], header, formula]
    marks = [] if empty else [i for i in range(96) if i not in drop_marks]
    if duplicate_mark is not None:
        marks.insert(marks.index(duplicate_mark) + 1, duplicate_mark)
    for i in marks:
        v = ts_values(i)
        sources = ["NUCLEAR", "WIND", "SOLAR", "HYDRO", "GAS", "THERMAL", "OTHERS"] + \
            (["STORAGE"] if layout in ("15", "16") and not total_excludes_storage else [])
        total = sum(v[s] for s in sources)
        demand = total + v["EXCHANGE"] - 2500.0 + (v["STORAGE DEMAND"] if layout == "16" else 0.0)
        v.update({"DEMAND MET": demand, "NET DEMAND MET": demand - v["WIND"] - v["SOLAR"], "TOTAL GENERATION": total})
        for (index, name), value in (value_override or {}).items():
            if index == i:
                v[name] = value
        h, m = divmod(i * 15, 60)
        stamp = {"text": f"{h}:{m:02d}", "seconds": f"{h}:{m:02d}:00", "fraction_text": repr(i / 96)}[time_format]
        row = [stamp] + [v[n] for n in names[1:]] + [""] * (width - len(names))
        row[exchange_col] = v["EXCHANGE"]
        rows.append(row)
    rows.append(["¹Demand Met (Demand incidental on transmission system): synthetic footnote text for tests. "
                 "Disclaimer: synthetic."])
    return rows


def _section_e_rows(section_e: list | None, total_override: dict[str, float], heading: str) -> list[list[Any]]:
    rows_in = SECTION_E if section_e is None else section_e
    numeric = lambda v: v if isinstance(v, (int, float)) else 0.0  # noqa: E731
    rows: list[list[Any]] = [[heading], ["", "", *REGIONS, "TOTAL"]]
    for label, values in rows_in:
        rows.append([label, "", *values, total_override.get(label, round(sum(numeric(v) for v in values), 4))])
    rows.append([""])
    return rows


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
