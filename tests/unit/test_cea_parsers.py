"""Unit tests for the CEA parsers, cell conversions and request rules (synthetic reports, no network)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from src.pipelines.cea import api
from src.pipelines.cea.cells import Grid, read_grid, to_decimal, to_ist_datetime
from src.pipelines.cea.errors import InvalidResponseError
from src.pipelines.cea.geography import STATES, normalise_name, state_code
from src.pipelines.cea.parse_coal import parse_coal_report
from src.pipelines.cea.parse_dgr import parse_outage_report, plant_daily_records, read_dgr2
from src.pipelines.cea.parse_psp import classify_area, parse_listing, parse_psp_report
from src.pipelines.cea.reference import plant_code
from tests.support.cea import (
    DAY,
    ENERGY_UUID,
    MONTH,
    CoalSpec,
    OutageSpec,
    StationSpec,
    UnitSpec,
    coal_rows,
    default_coal,
    dgr2_rows,
    dgr10_rows,
    listing_html,
    pdf_bytes,
    psp_lines,
    xls_bytes,
)

IST = api.IST
PLANTS = {"TEST ALPHA TPS": 1, "TEST BETA HPS": 2, "TEST GAMMA TPS": 3}


def grid(rows: list[list[object]]) -> Grid:
    return read_grid(xls_bytes(rows))


# ------------------------------------------------------------------ cells


@pytest.mark.parametrize("value, scale, expected", [
    (10.283639999999998, 3, Decimal("10.284")),
    ("2,57,783", 3, Decimal("257783.000")),
    ("39%", 2, Decimal("39.00")),
    ("-7", 3, Decimal("-7.000")),
    (0.0, 2, Decimal("0.00")),
    ("", 3, None),
    ("  ", 3, None),
])
def test_to_decimal(value: object, scale: int, expected: Decimal | None) -> None:
    assert to_decimal(value, scale) == expected


@pytest.mark.parametrize("value", ["n/a", "12a", "*"])
def test_to_decimal_rejects_text(value: str) -> None:
    with pytest.raises(ValueError):
        to_decimal(value, 3)


@pytest.mark.parametrize("value, expected", [
    ("08/11/2026 09:58", datetime(2026, 11, 8, 9, 58, tzinfo=IST)),
    ("25/01/2019 22.30.00", datetime(2019, 1, 25, 22, 30, tzinfo=IST)),
    ("21/08/2018", datetime(2018, 8, 21, tzinfo=IST)),
    ("", None),
    (46334.41527777778, datetime(2026, 11, 8, 9, 58, tzinfo=IST)),
])
def test_to_ist_datetime(value: object, expected: datetime | None) -> None:
    result = to_ist_datetime(value)
    if expected is None:
        assert result is None
    else:
        assert result is not None and abs((result - expected).total_seconds()) < 1


def test_to_ist_datetime_rejects_garbage() -> None:
    with pytest.raises(ValueError):
        to_ist_datetime("8")


def test_read_grid_rejects_non_workbook() -> None:
    with pytest.raises(InvalidResponseError):
        read_grid(b"<!doctype html><html>login</html>")


# ---------------------------------------------------------- geography


def test_all_states_have_unique_codes_and_regions() -> None:
    assert len({code for code, *_ in STATES}) == len(STATES) == 36
    assert state_code("CHHATISGARH") == state_code("Chhattisgarh") == "CG"
    assert state_code("Andaman- Nicobar") == state_code("ANDAMAN & NICOBAR ISLANDS") == "AN"
    assert state_code("Dadra & Nagar Haveli and Daman & Diu") == "DH"
    assert state_code("Atlantis") is None
    assert normalise_name("  North -  Eastern  Region ") == "NORTH-EASTERN REGION"


def test_plant_code_is_normalised_and_bounded() -> None:
    assert plant_code(" Panipat   TPS ") == "CEA:PANIPAT TPS"
    with pytest.raises(ValueError):
        plant_code("X" * 80)


# --------------------------------------------------------------- coal


def test_coal_report_maps_plant_rows_and_skips_totals() -> None:
    report = parse_coal_report(grid(coal_rows()), expected_date=DAY, plants=PLANTS)

    assert not report.failures and len(report.records) == 2
    alpha, gamma = (r.values for r in report.records)
    assert alpha == {
        "plant_id": 1, "report_date": DAY, "normative_stock_days": Decimal("22.00"),
        "daily_requirement_000_tonnes": Decimal("10.284"), "indigenous_stock_000_tonnes": Decimal("88.700"),
        "imported_stock_000_tonnes": Decimal("0.000"), "actual_stock_000_tonnes": Decimal("88.700"),
        "stock_vs_normative_pct": Decimal("39.00"), "critical_stock_flag": False,
        "receipt_000_tonnes": Decimal("12.400"), "consumption_000_tonnes": Decimal("0.000"), "remarks": None,
    }
    assert gamma["critical_stock_flag"] is True and gamma["remarks"].startswith("Plant to augment")
    assert "stock_days" not in alpha and "actual_stock_tonnes" not in alpha  # D3: not derived
    assert report.grand_total_stock == Decimal("172.600") == report.plant_stock_sum
    assert report.records[0].raw["capacity_mw"] == "460"


def test_coal_unmapped_station_keeps_record_with_null_plant() -> None:
    report = parse_coal_report(grid(coal_rows()), expected_date=DAY, plants={})
    assert [r.values["plant_id"] for r in report.records] == [None, None]


def test_coal_row_with_unexpected_cells_is_a_parse_failure() -> None:
    bad = CoalSpec("Haryana", "X", "TEST BAD TPS", 100.0, "50%", 22.0, 1.0, 22.0, 1.0, 0.0, 1.0, "oops", False,
                   0.0, 0.0)
    report = parse_coal_report(grid(coal_rows(plants=[*default_coal(), bad])), expected_date=DAY, plants=PLANTS)
    assert len(report.records) == 2 and len(report.failures) == 1
    assert "TEST BAD TPS" in report.failures[0].description and "percentage" in report.failures[0].description


def test_coal_report_date_mismatch_and_missing_header() -> None:
    with pytest.raises(InvalidResponseError, match="AS ON 2026-11-11, expected 2026-11-10"):
        parse_coal_report(grid(coal_rows()), expected_date=date(2026, 11, 10), plants=PLANTS)
    rows = coal_rows()
    rows[3] = [""] * 40
    with pytest.raises(InvalidResponseError, match="header"):
        parse_coal_report(grid(rows), expected_date=DAY, plants=PLANTS)


# ------------------------------------------------------------ outages


def test_outage_report_maps_categories_units_and_times() -> None:
    report = parse_outage_report(grid(dgr10_rows()), expected_date=DAY, plants=PLANTS, units={(1, "1"): 11})

    assert not report.failures and len(report.records) == 3
    first, second, third = (r.values for r in report.records)
    assert first["plant_id"] == 1 and first["unit_id"] == 11 and first["unit_label"] == "1"
    assert (first["planned_mw"], first["forced_minor_mw"], first["outage_capacity_mw"]) == (
        Decimal("0.000"), Decimal("210.000"), Decimal("210.000"))
    assert first["outage_start_at"] == datetime(2026, 11, 8, 9, 58, tzinfo=IST)
    assert first["expected_sync_at"] is None and first["state_name"] == "Haryana"
    assert second["unit_id"] is None and second["planned_mw"] == Decimal("250.000")  # unit not in master
    assert third["expected_sync_at"] == datetime(2026, 11, 20, tzinfo=IST)
    assert third["outage_reason"] == "TURBINE OIL SYSTEM PROBLEM"
    assert report.published_total_mw == Decimal("1120.000")


def test_outage_reason_wrapped_onto_next_row_is_joined() -> None:
    first = OutageSpec("Uttar Pradesh", "TEST ALPHA TPS", 8.0, start="14/01/2026 22:17", reason="RESERVE SHUT DOWN /")
    second = OutageSpec("Uttar Pradesh", "TEST ALPHA TPS", 8.0, start="14/01/2026 22:17", reason="STANDBY UNIT")
    conflicting = OutageSpec("Uttar Pradesh", "TEST GAMMA TPS", 1.0, forced_major=10.0, reason="A")
    conflicting_2 = OutageSpec("Uttar Pradesh", "TEST GAMMA TPS", 1.0, forced_major=20.0, reason="B")
    report = parse_outage_report(grid(dgr10_rows(outages=[first, second, conflicting, conflicting_2])),
                                 expected_date=DAY, plants=PLANTS, units={})
    assert [r.values["outage_reason"] for r in report.records] == ["RESERVE SHUT DOWN / STANDBY UNIT", "A", "B"]
    assert report.records[0].raw["continuation_rows"][0]["reason"] == "STANDBY UNIT"


def test_outage_station_name_wrapped_onto_next_row_is_joined() -> None:
    first = OutageSpec("Gujarat", "TEST ALPHA", 5.0, forced_minor=660.0, start="07/11/2026 11:40", reason="X")
    second = OutageSpec("Gujarat", "TPS", 5.0, forced_minor=660.0, start="07/11/2026 11:40", reason="X")
    report = parse_outage_report(grid(dgr10_rows(outages=[first, second])), expected_date=DAY,
                                 plants=PLANTS, units={(1, "5"): 51})
    assert len(report.records) == 1
    record = report.records[0]
    assert record.key == "2026-11-11|TEST ALPHA TPS|5" and record.values["plant_id"] == 1
    assert record.values["unit_id"] == 51 and record.values["outage_capacity_mw"] == Decimal("660.000")
    assert record.values["outage_reason"] == "X" and record.raw["station"] == "TEST ALPHA TPS"
    assert report.published_total_mw == Decimal("1320.000")  # the report's own Total double counts it


def test_outage_bad_timestamp_is_a_parse_failure() -> None:
    rows = dgr10_rows(outages=[OutageSpec("Haryana", "TEST ALPHA TPS", 1.0, forced_minor=210.0, start="yesterday")])
    report = parse_outage_report(grid(rows), expected_date=DAY, plants=PLANTS, units={})
    assert not report.records and "unrecognised date/time" in report.failures[0].description


def test_outage_report_rejects_other_reports_and_dates() -> None:
    with pytest.raises(InvalidResponseError, match="maintenance report"):
        parse_outage_report(grid(coal_rows()), expected_date=DAY, plants=PLANTS, units={})
    with pytest.raises(InvalidResponseError, match="expected 2026-11-10"):
        parse_outage_report(grid(dgr10_rows()), expected_date=date(2026, 11, 10), plants=PLANTS, units={})


# ------------------------------------------------------- sub-report 2


def test_dgr2_hierarchy_stations_units_and_bhutan() -> None:
    report = read_dgr2(grid(dgr2_rows()), expected_date=DAY)

    assert [(s.name, s.region_code, s.state_name, s.plant_type, s.foreign) for s in report.stations] == [
        ("TEST ALPHA TPS", "NR", "HARYANA", "THERMAL", False),
        ("TEST BETA HPS", "NR", "HARYANA", "HYDRO", False),
        ("TEST GAMMA TPS", "WR", "CHHATISGARH", "THERMAL", False),
        ("TEST DRUK HPS", None, "BHUTAN", "HYDRO", True),
    ]
    assert [(u.station, u.label, u.capacity_mw) for u in report.units][:2] == [
        ("TEST ALPHA TPS", "1", Decimal("210.000")), ("TEST ALPHA TPS", "2", Decimal("250.000"))]
    assert report.units[0].outage_start_at == datetime(2026, 11, 8, 9, 58, tzinfo=IST)
    assert report.units[0].flag == "S"

    records, failures = plant_daily_records(report, plants=PLANTS)
    assert not failures and len(records) == 3  # Bhutan import station skipped
    alpha = records[0].values
    assert alpha["coal_stock_days"] == Decimal("7.00") and alpha["todays_actual_mu"] == Decimal("6.2500")
    assert records[1].values["coal_stock_days"] is None


def test_dgr2_rejects_wrong_report() -> None:
    with pytest.raises(InvalidResponseError, match="sub-report 2"):
        read_dgr2(grid(dgr10_rows()))
    stations = [StationSpec("TEST X", "NORTHERN", "HARYANA", capacity=1.0, units=[UnitSpec(1.0, 1.0)])]
    with pytest.raises(InvalidResponseError, match="expected 2026-11-12"):
        read_dgr2(grid(dgr2_rows(stations=stations)), expected_date=date(2026, 11, 12))


# --------------------------------------------------------------- PSP


def test_psp_energy_rows_wrapped_names_regions_and_flags() -> None:
    report = parse_psp_report(pdf_bytes(psp_lines()), measure="ENERGY", expected_month=MONTH)

    assert report.status == "Revised" and not report.failures
    assert len(report.records) == 26  # 13 rows x (month, April-to-month)
    month_rows = {r.values["area_code"]: r.values for r in report.records if r.values["period_scope"] == "MONTH"}
    assert month_rows["HR"]["requirement"] == Decimal("8611.000") and month_rows["HR"]["region_code"] == "NR"
    assert month_rows["JK_LA"]["area_type"] == "SYSTEM" and month_rows["JK_LA"]["supplied"] == Decimal("1501.000")
    assert month_rows["DH"]["state_code"] == "DH" and month_rows["DH"]["region_code"] == "WR"
    assert month_rows["OTHERS_WR"]["area_type"] == "OTHER"
    assert month_rows["AN"]["footnote_flags"] == "(#) (**)" and month_rows["AN"]["region_code"] == "ER"
    assert month_rows["NER"]["area_type"] == "REGION" and month_rows["NER"]["requirement"] == Decimal("2311.000")
    assert month_rows["ALL_INDIA"]["requirement"] == Decimal("161111.000")
    assert month_rows["ALL_INDIA"]["unit"] == "MU"
    fytd = next(r.values for r in report.records if r.values["area_code"] == "ALL_INDIA"
                and r.values["period_scope"] == "FYTD")
    assert fytd["requirement"] == Decimal("811111.000") and fytd["not_supplied"] == Decimal("1100.000")


def test_psp_peak_and_april_single_period() -> None:
    peak = parse_psp_report(pdf_bytes(psp_lines("PEAK")), measure="PEAK", expected_month=MONTH)
    assert {r.values["unit"] for r in peak.records} == {"MW"} and {r.values["measure"] for r in peak.records} == {"PEAK"}
    april = date(2026, 4, 1)
    report = parse_psp_report(pdf_bytes(psp_lines(month=april, april_only=True)), measure="ENERGY",
                              expected_month=april)
    assert len(report.records) == 13 and {r.values["period_scope"] for r in report.records} == {"MONTH"}


def test_psp_rejects_wrong_measure_month_and_layout() -> None:
    with pytest.raises(InvalidResponseError, match="found a peak report"):
        parse_psp_report(pdf_bytes(psp_lines("PEAK")), measure="ENERGY", expected_month=MONTH)
    with pytest.raises(InvalidResponseError, match="expected 2026-07"):
        parse_psp_report(pdf_bytes(psp_lines()), measure="ENERGY", expected_month=date(2026, 7, 1))
    generation = pdf_bytes(["REGIONWISE MONTHLY AND CUMULATIVE GROSS GENERATION DURING THE YEAR", "NORTHERN 1 2 3"])
    with pytest.raises(InvalidResponseError, match="supported layout"):
        parse_psp_report(generation, measure="ENERGY", expected_month=MONTH)
    with pytest.raises(InvalidResponseError, match="readable PDF"):
        parse_psp_report(b"not a pdf", measure="ENERGY", expected_month=MONTH)


def test_psp_unknown_area_and_wrong_number_count() -> None:
    rows = [("/ Atlantis", "1 1 0 0.0 2 2 0 0.0"), ("/ Haryana", "1 1 0"), (" / All India", "9 9 0 0.0 9 9 0 0.0")]
    report = parse_psp_report(pdf_bytes(psp_lines(rows=rows)), measure="ENERGY", expected_month=MONTH)
    assert [r.values["area_code"] for r in report.records if r.values["area_name"] == "Atlantis"] == [None, None]
    assert len(report.failures) == 1 and "expected 8 numbers" in report.failures[0].description


def test_psp_corrigendum_and_bare_markers() -> None:
    lines = psp_lines(rows=[("/ Tripura *", "194 194 0 0.0 863 863 0 0.0"), (" / All India", "9 9 0 0.0 9 9 0 0.0")])
    lines.insert(1, "Corrigendum")
    report = parse_psp_report(pdf_bytes(lines), measure="ENERGY", expected_month=MONTH)
    assert report.status == "Revised; Corrigendum"
    tripura = report.records[0].values
    assert tripura["area_code"] == "TR" and tripura["footnote_flags"] == "(*)"


def test_classify_area_former_uts_and_others_without_region() -> None:
    assert classify_area("Daman & Diu", "WR")["area_code"] == "DD"
    assert classify_area("Others", None) is None


def test_parse_listing() -> None:
    html = listing_html("Power Supply Position - Energy", [("2026-08", ENERGY_UUID.upper()), ("bad", "x")])
    entries = parse_listing(html, listing_title="Power Supply Position - Energy")
    assert [(e.month, e.uuid) for e in entries] == [(MONTH, ENERGY_UUID)]
    with pytest.raises(InvalidResponseError):
        parse_listing(html, listing_title="Power Supply Position - Peak")


# ------------------------------------------------------------------ api


def test_urls() -> None:
    assert api.COAL.url("https://npp.gov.in/public-reports/cea/", DAY) == (
        "https://npp.gov.in/public-reports/cea/daily/fuel/11-11-2026/dailyCoal1-2026-11-11.xls")
    assert api.DGR10.url("https://npp.gov.in/public-reports/cea", DAY).endswith("/daily/dgr/11-11-2026/dgr10-2026-11-11.xls")
    assert api.PSP_PEAK.listing_url("https://cea.nic.in").endswith("subSubHeadingId=10011&lang=en")
    assert api.pdf_url("https://cea.nic.in", ENERGY_UUID).endswith(f"/download-pdf/en/{ENERGY_UUID}")


@pytest.mark.parametrize("start, end, message", [
    (date(2026, 11, 13), date(2026, 11, 13), "cannot be published yet"),
    (date(2026, 10, 1), date(2026, 11, 1), "at most 31 days"),
    (date(2024, 6, 1), date(2024, 6, 2), "available from 2024-06-15"),
    (date(2026, 11, 5), date(2026, 11, 4), "before start date"),
])
def test_validate_date_range(start: date, end: date, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        api.validate_date_range(api.COAL, start, end, today=date(2026, 11, 13))


def test_validate_month_range() -> None:
    api.validate_month_range(date(2026, 1, 1), date(2026, 10, 1), today=date(2026, 11, 13))
    for start, end, message in ((date(2026, 11, 1), date(2026, 11, 1), "cannot be published"),
                                (date(2019, 6, 1), date(2019, 6, 1), "supported from 2020-04"),
                                (date(2024, 1, 1), date(2025, 1, 1), "at most 12 months")):
        with pytest.raises(ValueError, match=message):
            api.validate_month_range(start, end, today=date(2026, 11, 13))
    assert api.months(date(2025, 11, 1), date(2026, 2, 1)) == [date(2025, 11, 1), date(2025, 12, 1),
                                                               date(2026, 1, 1), date(2026, 2, 1)]
    with pytest.raises(ValueError):
        api.parse_month("2026-8")
