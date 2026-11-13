"""Unit tests for Grid-India listing rules, availability, entity mapping and section A/C/J parsers."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal

import pytest

from src.pipelines.grid_india import api
from src.pipelines.grid_india.entities import resolve_area
from src.pipelines.grid_india.errors import GridIndiaApiError, InvalidWorkbookError
from src.pipelines.grid_india.parse_psp import parse_section_a, parse_states, read_workbook
from tests.support.grid_india import SECTION_A, AreaRow, FileSpec, default_section_c, workbook_bytes

IST = api.IST
D = date(2026, 11, 10)


def wb(**kwargs):
    return read_workbook(workbook_bytes(D, **kwargs), expected_date=D)


# --------------------------------------------------------------------- listing

def test_fy_label_and_listing_months() -> None:
    assert api.fy_label(date(2026, 3, 31)) == "2025-26" and api.fy_label(date(2026, 4, 1)) == "2026-27"
    assert api.listing_months(date(2026, 8, 1), date(2026, 8, 31)) == [date(2026, 7, 1), date(2026, 8, 1),
                                                                       date(2026, 9, 1)]


def test_parse_listing_and_selection_keeps_every_version_oldest_first() -> None:
    original = FileSpec(D, b"x", datetime(2026, 11, 11, 10, 47), suffix="269")
    revision = FileSpec(D, b"y", datetime(2026, 11, 11, 16, 38), suffix="269_611")
    pdf = FileSpec(D, b"z", datetime(2026, 11, 11, 10, 46), suffix="613", extension="pdf")
    other_day = FileSpec(date(2026, 11, 20), b"w")
    payload = {"flagType": 0, "retData": [revision.entry(2), pdf.entry(3), original.entry(1), other_day.entry(4),
                                          {"Title_": "Week ahead forecast", "FilePath": "files/x.pdf"}]}
    entries = api.parse_listing(payload)
    assert len(entries) == 4  # the non-PSP title is ignored
    selected = api.select_workbooks(entries, D, D)
    assert [e.file_name for e in selected] == ["10.11.26_NLDC_PSP_269.xls", "10.11.26_NLDC_PSP_269_611.xls"]
    assert selected[0].created_on == datetime(2026, 11, 11, 10, 47, tzinfo=IST)


def test_parse_listing_rejects_unexpected_payload() -> None:
    with pytest.raises(GridIndiaApiError):
        api.parse_listing({"unexpected": True})
    with pytest.raises(GridIndiaApiError, match="error"):
        api.parse_listing({"flagType": 1, "retMessage": "error", "retData": []})


@pytest.mark.parametrize("start, end, message", [
    (date(2022, 12, 31), date(2022, 12, 31), "PDF only"),
    (date(2026, 10, 1), date(2026, 11, 10), "at most 31 days"),
    (date(2026, 11, 13), date(2026, 11, 13), "cannot be published yet"),
    (date(2026, 11, 5), date(2026, 11, 4), "before start date"),
])
def test_validate_date_range(start: date, end: date, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        api.validate_date_range(start, end, today=date(2026, 11, 13))


# ---------------------------------------------------------------- availability

def test_availability_actual_from_2025_05_12() -> None:
    created = datetime(2026, 11, 11, 10, 40, tzinfo=IST)
    a = api.availability(D, created)
    assert (a.published_at, a.available_at, a.basis, a.note) == (created, created, "PUBLISHED", None)


def test_availability_estimated_before_cutoff_even_with_created_on() -> None:
    a = api.availability(date(2025, 5, 11), datetime(2026, 1, 20, 9, 0, tzinfo=IST))  # bulk re-upload date
    assert a.published_at is None and a.basis == "ESTIMATED_D_PLUS_2_0900"
    assert a.available_at == datetime(2025, 5, 13, 9, 0, tzinfo=IST) and a.note is None


def test_availability_implausible_or_missing_created_on_falls_back_with_a_note() -> None:
    early = api.availability(D, datetime(2026, 11, 10, 23, 0, tzinfo=IST))
    assert early.basis == "ESTIMATED_D_PLUS_2_0900" and early.published_at is None and "before" in early.note
    missing = api.availability(D, None)
    assert missing.basis == "ESTIMATED_D_PLUS_2_0900" and missing.available_at == datetime(2026, 11, 12, 9, tzinfo=IST)


# -------------------------------------------------------------------- entities

@pytest.mark.parametrize("name, expected", [
    ("UP", ("UP", "STATE", "UP", "NR")),
    ("MP", ("MP", "STATE", "MP", "WR")),
    ("DNHDDPDCL", ("DH", "STATE", "DH", "WR")),
    ("Odisha", ("OD", "STATE", "OD", "ER")),
    ("Chhattisgarh", ("CG", "STATE", "CG", "WR")),
    ("DVC", ("DVC", "SYSTEM", None, "ER")),
    ("J&K(UT) & Ladakh(UT)", ("JK_LA", "SYSTEM", None, "NR")),
    ("Bulk Consumer_WR ISTS", ("BULK_WR", "BULK_CONSUMER", None, "WR")),
    ("Railways_ER ISTS", ("RAILWAYS_ER", "ENTITY", None, "ER")),
    ("RIL Jamnagar", ("RIL_JAMNAGAR", "ENTITY", None, "WR")),
    ("RIL JAMNAGAR", ("RIL_JAMNAGAR", "ENTITY", None, "WR")),
    ("Hindustan Zinc Ltd. Daripa", ("HZL_DARIPA", "ENTITY", None, "NR")),
])
def test_resolve_area(name: str, expected: tuple) -> None:
    area = resolve_area(name)
    assert area is not None and (area.area_code, area.area_type, area.state_code, area.region_code) == expected


def test_unknown_area_is_not_guessed() -> None:
    assert resolve_area("Some New Captive Plant") is None


# ------------------------------------------------------------------ workbook

def test_read_workbook_checks_the_report_date_and_sheet() -> None:
    with pytest.raises(InvalidWorkbookError, match="listing title says 2026-11-09"):
        read_workbook(workbook_bytes(D), expected_date=date(2026, 11, 9))
    with pytest.raises(InvalidWorkbookError, match="readable"):
        read_workbook(b"<html>login</html>", expected_date=D)


def test_section_a_regions_all_india_and_totals() -> None:
    result = parse_section_a(wb())
    assert not result.failures and not result.unknown_labels and len(result.records) == 6
    by_region = {r.values["region_code"]: r.values for r in result.records}
    nr, total = by_region["NR"], by_region[None]
    assert nr["energy_met_mu"] == Decimal("1100.5000") and nr["evening_peak_demand_met_mw"] == Decimal("50000.000")
    assert nr["max_demand_met_time"] == time(18, 20)
    assert by_region["ER"]["wind_gen_mu"] is None  # '-' in the report
    assert total["max_demand_met_mw"] == Decimal("212345.000") and total["max_demand_met_time"] == time(9, 28)
    assert total["energy_met_mu"] == Decimal("4250.7500")
    regional, published = result.totals["energy_met_mu"]
    assert regional == published
    assert "max_demand_met_mw" not in result.totals  # not additive


def test_section_a_total_mismatch_is_reported_not_corrected() -> None:
    result = parse_section_a(wb(total_override={"Energy Met (MU)": 4300.0}))
    regional, published = result.totals["energy_met_mu"]
    assert published == Decimal("4300.0000") and regional == Decimal("4250.7500")
    total = next(r for r in result.records if r.values["region_code"] is None)
    assert total.values["energy_met_mu"] == Decimal("4300.0000")  # the published value is kept


def test_section_a_older_labels_and_unknown_label() -> None:
    older = [("Demand Met during Evening Peak hrs(MW) (at 20:00 hrs; from RLDCs)", [1, 1, 1, 1, 1], None),
             ("Peak Shortage (MW)", [0, 0, 0, 0, 0], None),
             ("Energy Met (MU)", [1.0, 1.0, 1.0, 1.0, 1.0], None),
             ("Maximum Demand Met During the Day (MW) (From NLDC SCADA)", [2, 2, 2, 2, 2], 9),
             ("Time Of Maximum Demand Met (From NLDC SCADA)", ["10:00"] * 5, "10:00"),
             ("Brand New Metric (MU)", [1, 1, 1, 1, 1], None)]
    result = parse_section_a(wb(section_a=older))
    assert result.unknown_labels == ["Brand New Metric (MU)"]
    nr = next(r.values for r in result.records if r.values["region_code"] == "NR")
    assert nr["evening_peak_shortage_mw"] == Decimal("0.000") and nr["hydro_gen_mu"] is None


def test_section_a_missing_required_label_or_section_fails() -> None:
    without_energy = [row for row in SECTION_A if not row[0].startswith("Energy Met")]
    with pytest.raises(InvalidWorkbookError, match="energy_met_mu"):
        parse_section_a(wb(section_a=without_energy))
    with pytest.raises(InvalidWorkbookError, match="section A not found"):
        parse_section_a(wb(omit_section="A"))


def test_states_sections_c_and_j() -> None:
    result, has_j = parse_states(wb())
    assert has_j and not result.failures and not result.region_label_mismatches
    values = {r.values["area_code"]: r.values for r in result.records}
    assert len(values) == 34 + 2 + 4  # 32 states + DVC + JK_LA, 2 bulk consumers, 4 section J entities
    dh = values["DH"]
    assert (dh["area_type"], dh["state_code"], dh["area_name"], dh["region_code"]) == ("STATE", "DH", "DNHDDPDCL", "WR")
    assert (dh["max_od_mw"], dh["max_ud_mw"]) == (Decimal("65.000"), Decimal("-108.000"))
    assert values["DVC"]["state_code"] is None and values["DVC"]["drawal_schedule_mu"] == Decimal("-30.9000")
    assert values["BULK_NR"]["area_type"] == "BULK_CONSUMER"
    balco = next(r for r in result.records if r.values["area_code"] == "BALCO")
    assert balco.raw["section"] == "J" and balco.values["region_code"] == "WR"


def test_states_older_single_max_od_and_no_section_j() -> None:
    rows = default_section_c()
    for row in rows:
        row.max_od_ud = 133.0
    result, has_j = parse_states(wb(section_c=rows, with_j=False))
    assert not has_j
    pb = next(r.values for r in result.records if r.values["area_code"] == "PB")
    assert (pb["max_od_mw"], pb["max_ud_mw"]) == (Decimal("133.000"), None)


def test_states_unknown_name_bad_value_and_region_label_mismatch() -> None:
    rows = default_section_c() + [AreaRow("", "Mystery Captive Ltd"), AreaRow("", "Goa", energy="n/a")]
    rows[0].region_label = "SR"  # Punjab labelled SR
    result, _ = parse_states(wb(section_c=rows))
    assert [r.values["area_code"] for r in result.records if r.values["area_name"] == "Mystery Captive Ltd"] == [None]
    assert len(result.failures) == 1 and "Goa" in result.failures[0].description
    assert result.region_label_mismatches == ["Punjab: listed under SR, reference region NR"]


# ------------------------------------------------------------------ section G

from src.pipelines.grid_india.parse_psp import FUEL_TYPES, parse_section_g  # noqa: E402
from tests.support.grid_india import SECTION_G  # noqa: E402

ALL_FUELS = ["COAL", "GAS_NAPHTHA_DIESEL", "HYDRO", "LIGNITE", "NUCLEAR", "RES"]


def _mismatches(result) -> list[str]:
    return [what for what, parts, published, _ in result.totals if abs(parts - published) > Decimal("0.05")]


def test_section_g_region_by_fuel_records_with_published_labels() -> None:
    result = parse_section_g(wb())
    assert not result.failures and not result.missing_fuels and len(result.records) == 30
    by_key = {(r.values["region_code"], r.values["fuel_type"]): r.values for r in result.records}
    assert sorted({f for _, f in by_key}) == ALL_FUELS
    assert {r for r, _ in by_key} == {"NR", "WR", "SR", "ER", "NER"}  # no All-India row
    assert by_key[("NR", "COAL")]["gross_generation_mu"] == Decimal("712.4200")
    assert by_key[("NER", "HYDRO")]["gross_generation_mu"] == Decimal("22.4220")
    assert by_key[("WR", "GAS_NAPHTHA_DIESEL")]["fuel_label"] == "Gas, Naptha & Diesel"
    assert by_key[("SR", "RES")]["fuel_label"] == "RES (Wind, Solar, Biomass & Others)"
    # derived rows are not records
    assert not any(r.values["fuel_label"].startswith(("Total", "Share")) for r in result.records)
    assert len(result.totals) == 6 + 5 + 1 and _mismatches(result) == []


@pytest.mark.parametrize("variant", [
    {"with_j": False},                              # 2023-2025 workbooks: no MOP_E1 sheet
    {"g_padding_rows": 3},                          # section G further down (row positions differ by year)
    {"section_c": default_section_c()[:-5]},        # fewer state rows above section G
])
def test_section_g_is_found_by_label_in_historical_layouts(variant: dict) -> None:
    result = parse_section_g(wb(**variant))
    assert len(result.records) == 30 and not result.failures


def test_fuel_label_mapping_covers_every_published_label() -> None:
    assert {FUEL_TYPES[label.upper()] for label, _ in SECTION_G} == set(ALL_FUELS)


def test_section_g_unknown_fuel_is_kept_unmapped_not_guessed() -> None:
    fuels = SECTION_G + [("Hydrogen", [1.0, 1.0, 1.0, 1.0, 1.0])]
    result = parse_section_g(wb(section_g=fuels))
    unknown = [r for r in result.records if r.values["fuel_label"] == "Hydrogen"]
    assert len(unknown) == 5 and all(r.values["fuel_type"] is None for r in unknown)
    assert not any("sum of fuels vs Total" in what for what, *_ in result.totals)  # not comparable while unmapped


def test_section_g_dash_bad_value_and_missing_fuel() -> None:
    fuels = [(label, list(values)) for label, values in SECTION_G if label != "Lignite"]
    fuels[0][1][3] = "-"        # Coal ER published as '-'
    fuels[1][1][0] = "n/a"      # Hydro NR not a number
    result = parse_section_g(wb(section_g=fuels))
    assert result.missing_fuels == ["LIGNITE"]
    assert next(r for r in result.records if r.values["fuel_type"] == "COAL"
                and r.values["region_code"] == "ER").values["gross_generation_mu"] is None
    assert len(result.failures) == 1 and "Hydro" in result.failures[0].description


def test_section_g_published_totals_that_do_not_add_up_are_reported() -> None:
    result = parse_section_g(wb(g_all_india_override={"Coal": 3489.0}, g_total_override={"WR": 1743.72}))
    assert _mismatches(result) == ["COAL (sum of regions vs All India)", "WR (sum of fuels vs Total)",
                                   "All India (sum of fuels vs Total)"]
    coal_nr = next(r for r in result.records if r.values["fuel_type"] == "COAL" and r.values["region_code"] == "NR")
    assert coal_nr.values["gross_generation_mu"] == Decimal("712.4200")  # published values untouched


def test_section_g_missing_section_or_header_fails() -> None:
    with pytest.raises(InvalidWorkbookError, match="section G not found"):
        parse_section_g(wb(omit_section="G"))


# ------------------------------------------------------------------ section F

from src.pipelines.grid_india.parse_psp import OUTAGE_SECTORS, parse_section_f  # noqa: E402
from tests.support.grid_india import SECTION_F  # noqa: E402


def _f_mismatches(result) -> list[str]:
    return [what for what, parts, published in result.totals if abs(parts - published) > 1]


def test_section_f_region_by_sector_records() -> None:
    result = parse_section_f(wb())
    assert not result.failures and not result.missing_sectors and len(result.records) == 10
    by_key = {(r.values["region_code"], r.values["sector"]): r.values for r in result.records}
    assert {s for _, s in by_key} == {"CENTRAL", "STATE"}
    assert {r for r, _ in by_key} == {"NR", "WR", "SR", "ER", "NER"}  # no All-India row
    assert by_key[("WR", "CENTRAL")]["outage_capacity_mw"] == Decimal("9636.613")
    assert by_key[("NER", "STATE")]["outage_capacity_mw"] == Decimal("276.061")
    assert "plant_id" not in by_key[("NR", "STATE")] and "outage_reason" not in by_key[("NR", "STATE")]
    assert len(result.totals) == 2 + 5 + 1 and _f_mismatches(result) == []


@pytest.mark.parametrize("variant", [{"with_j": False}, {"g_padding_rows": 3}, {"section_c": default_section_c()[:-5]}])
def test_section_f_is_found_by_label_in_historical_layouts(variant: dict) -> None:
    result = parse_section_f(wb(**variant))
    assert len(result.records) == 10 and not result.failures


def test_sector_mapping_covers_every_published_label() -> None:
    assert {OUTAGE_SECTORS[label.upper()] for label, _ in SECTION_F} == {"CENTRAL", "STATE"}


def test_section_f_unknown_sector_dash_bad_value_and_missing_sector() -> None:
    sectors = [("Central Sector", [1.5, "-", 2.5, 3.5, "x"]), ("Private Sector", [1.0, 1.0, 1.0, 1.0, 1.0])]
    result = parse_section_f(wb(section_f=sectors))
    assert result.missing_sectors == ["STATE"]
    assert len(result.failures) == 1 and "Central Sector" in result.failures[0].description
    assert next(r for r in result.records if r.values["sector"] == "CENTRAL"
                and r.values["region_code"] == "WR").values["outage_capacity_mw"] is None
    private = [r for r in result.records if r.raw["sector_label"] == "Private Sector"]
    assert len(private) == 5 and all(r.values["sector"] is None for r in private)
    assert not any("sum of sectors vs Total" in what for what, *_ in result.totals)


def test_section_f_published_totals_that_do_not_add_up_are_reported() -> None:
    result = parse_section_f(wb(f_total_override={"Central Sector": 25000.0, "ER": 7000.0}))
    assert _f_mismatches(result) == ["CENTRAL (sum of regions vs TOTAL)", "ER (sum of sectors vs Total)",
                                     "All India (sum of sectors vs Total)"]


def test_section_f_missing_section_fails() -> None:
    with pytest.raises(InvalidWorkbookError, match="section F not found"):
        parse_section_f(wb(omit_section="F"))
