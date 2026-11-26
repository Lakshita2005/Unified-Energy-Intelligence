"""Unit tests for Grid-India listing rules, availability, entity mapping and section A/C/J parsers."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pytest

from config.settings import Settings
from src.ingestion.models import RawDocument, Severity
from src.pipelines.grid_india import api
from src.pipelines.grid_india.entities import resolve_area
from src.pipelines.grid_india.errors import GridIndiaApiError, InvalidWorkbookError
from src.pipelines.grid_india.parse_psp import LAYOUT_ACE, LAYOUT_OD_UD, parse_section_a, parse_states, read_workbook
from src.pipelines.grid_india.pipeline import PreparedWorkbook, StatesPipeline
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


def test_states_od_ud_layout_is_recorded_and_has_no_ace_values() -> None:
    result, _ = parse_states(wb())
    assert result.layouts == {"C": LAYOUT_OD_UD, "J": LAYOUT_OD_UD}
    assert not any("ace_max_positive_mw" in r.raw for r in result.records)
    for record in result.records:   # reports before 2026-11-18: the ACE columns stay NULL
        assert record.values["max_ace_pos_mw"] is None and record.values["max_ace_neg_mw"] is None


ACE_KEYS = ("max_od_mw", "max_ud_mw", "max_ace_pos_mw", "max_ace_neg_mw")


def test_states_ace_layout_stores_ace_and_keeps_od_ud_null() -> None:
    # From report date 2026-11-18 the eighth column is "ACE²(Max) +ve/ -ve (MW)" (Area Control Error) in C and J.
    result, has_j = parse_states(wb(peak_layout="ace"))
    assert has_j and not result.failures and result.layouts == {"C": LAYOUT_ACE, "J": LAYOUT_ACE}
    assert len(result.records) == 34 + 2 + 4
    for record in result.records:   # ACE is never written to the OD/UD columns
        assert record.values["max_od_mw"] is None and record.values["max_ud_mw"] is None
    dh = next(r for r in result.records if r.values["area_code"] == "DH")
    # values and signs as published: "65/ -108" -> +65, -108 (migration 0010 columns)
    assert (dh.values["max_ace_pos_mw"], dh.values["max_ace_neg_mw"]) == (Decimal("65.000"), Decimal("-108.000"))
    assert (dh.raw["ace_max_positive_mw"], dh.raw["ace_max_negative_mw"]) == ("65.000", "-108.000")
    assert dh.raw["max_ace"] == "65/ -108"   # the published cell text is kept as well
    balco = next(r for r in result.records if r.values["area_code"] == "BALCO")
    assert (balco.raw["section"], balco.values["max_ace_pos_mw"], balco.values["max_ace_neg_mw"]) == (
        "J", Decimal("100.000"), Decimal("-200.000"))
    # every other value is parsed exactly as in the OD/UD layout
    old = {r.values["area_code"]: r.values for r in parse_states(wb())[0].records}
    for record in result.records:
        same = {k: v for k, v in old[record.values["area_code"]].items() if k not in ACE_KEYS}
        assert {k: v for k, v in record.values.items() if k not in ACE_KEYS} == same


def test_states_ace_in_section_c_only_and_single_ace_number() -> None:
    rows = default_section_c()
    rows[0].max_od_ud = 512.0   # a single number in the ACE column -> positive only
    result, _ = parse_states(wb(section_c=rows, peak_layout="ace", j_peak_layout="od_ud"))
    assert result.layouts == {"C": LAYOUT_ACE, "J": LAYOUT_OD_UD}
    pb = next(r for r in result.records if r.values["area_code"] == "PB")
    assert (pb.values["max_od_mw"], pb.raw["ace_max_positive_mw"], pb.raw["ace_max_negative_mw"]) == (
        None, "512.000", None)
    assert (pb.values["max_ace_pos_mw"], pb.values["max_ace_neg_mw"]) == (Decimal("512.000"), None)  # missing stays NULL
    balco = next(r.values for r in result.records if r.values["area_code"] == "BALCO")   # section J: OD/UD
    assert (balco["max_od_mw"], balco["max_ud_mw"]) == (Decimal("100.000"), Decimal("-200.000"))
    assert (balco["max_ace_pos_mw"], balco["max_ace_neg_mw"]) == (None, None)


@pytest.mark.parametrize("layout", ["both", "neither"])
def test_states_need_exactly_one_of_od_ud_or_ace(layout: str) -> None:
    with pytest.raises(InvalidWorkbookError, match="expected exactly one of"):
        parse_states(wb(peak_layout=layout))


@pytest.mark.parametrize(("layout", "expected"), [("ace", 1), ("od_ud", 0)])
def test_states_pipeline_one_ace_layout_note_per_workbook(layout: str, expected: int) -> None:
    content = workbook_bytes(D, peak_layout=layout)
    created = datetime(2026, 11, 11, 10, 40, tzinfo=IST)
    entry = api.ListingEntry(1, "10.11.26_NLDC_PSP", D, "files/grdw/2026/11/10.11.26_NLDC_PSP_1.xls", created, None,
                             None, {})
    avail = api.availability(D, created)
    document = RawDocument(content=content, retrieved_at=created, extension="xls", content_type=None,
                           published_at=avail.published_at, source_url="https://example.invalid/x.xls",
                           document_name=entry.file_name, source_report_date=D)
    pipeline = StatesPipeline(settings=Settings(_env_file=None, log_to_file=False), database=object(),
                              raw_storage=object(), workbooks=[PreparedWorkbook(entry, document, None, avail)])
    parsed = pipeline.parse(document, None)
    all_issues = pipeline.document_issues(document, parsed, None)
    assert not [i for i in all_issues if i.issue_code == "ACE_NOT_STORED"]   # stored since migration 0010
    issues = [i for i in all_issues if i.issue_code == "ACE_LAYOUT"]
    assert len(issues) == expected
    if expected:
        assert issues[0].severity is Severity.INFO and "40 area(s)" in issues[0].issue_description
        assert "C+J" in issues[0].issue_description and all(r.values["max_od_mw"] is None for r in parsed.records)
        assert all(r.values["max_ace_pos_mw"] is not None for r in parsed.records)


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


# ------------------------------------------------------------------ section E

from src.pipelines.grid_india.parse_psp import EXCHANGE_FIELDS, parse_section_e  # noqa: E402
from tests.support.grid_india import SECTION_E  # noqa: E402


def test_section_e_signed_values_per_region_and_convention() -> None:
    result = parse_section_e(wb())
    assert not result.failures and not result.unknown_labels and not result.missing_fields
    assert result.sign_convention == "Import(+ve)/Export(-ve); OD(+)/UD(-)"
    by_region = {r.values["region_code"]: r.values for r in result.records}
    assert set(by_region) == {"NR", "WR", "SR", "ER", "NER"}  # no TOTAL row
    er = by_region["ER"]
    assert (er["scheduled_exchange_mu"], er["actual_exchange_mu"], er["od_ud_mu"]) == (
        Decimal("-162.4010"), Decimal("-184.5930"), Decimal("-22.2000"))  # signs and published O/D/U/D kept
    assert er["sign_convention"] == result.sign_convention and "exchange_direction" not in er
    assert len(result.totals) == 3 and all(abs(c - p) <= Decimal("0.05") for _, c, p in result.totals)
    # published O/D/U/D differs from Actual - Schedule only by rounding
    assert len(result.od_checks) == 6 and all(abs(c - p) <= Decimal("0.05") for _, c, p in result.od_checks)


@pytest.mark.parametrize("variant", [{"with_j": False}, {"g_padding_rows": 3}, {"section_c": default_section_c()[:-5]}])
def test_section_e_is_found_by_label_in_historical_layouts(variant: dict) -> None:
    result = parse_section_e(wb(**variant))
    assert len(result.records) == 5 and not result.failures


def test_exchange_row_mapping_covers_every_published_label() -> None:
    assert {EXCHANGE_FIELDS[label.upper().replace(" ", "")] for label, _ in SECTION_E} == {
        "scheduled_exchange_mu", "actual_exchange_mu", "od_ud_mu"}


def test_section_e_unknown_row_dash_and_bad_value() -> None:
    rows = [("Schedule(MU)", [1.5, -1.5, "-", 0.0, 0.0]), ("Actual(MU)", [2.5, -2.0, 0.5, "x", 0.0]),
            ("O/D/U/D(MU)", [1.0, -0.5, 0.5, 0.0, 0.0]), ("Net Transfer(MU)", [1, 1, 1, 1, 1])]
    result = parse_section_e(wb(section_e=rows))
    assert result.unknown_labels == ["Net Transfer(MU)"]
    assert len(result.failures) == 1 and "ER" in result.failures[0].description
    sr = next(r.values for r in result.records if r.values["region_code"] == "SR")
    assert sr["scheduled_exchange_mu"] is None and sr["actual_exchange_mu"] == Decimal("0.5000")


def test_section_e_without_od_row_is_reported_not_derived() -> None:
    result = parse_section_e(wb(section_e=SECTION_E[:2]))
    assert result.missing_fields == ["od_ud_mu"] and result.od_checks == []
    assert all(r.values["od_ud_mu"] is None for r in result.records)


def test_section_e_total_and_od_discrepancies_are_reported() -> None:
    rows = [SECTION_E[0], SECTION_E[1], ("O/D/U/D(MU)", [3.82, 1.55, 0.86, -25.0, -1.46])]
    result = parse_section_e(wb(section_e=rows, e_total_override={"Actual(MU)": -20.0}))
    assert [w for w, c, p in result.totals if abs(c - p) > Decimal("0.05")] == ["actual_exchange_mu (sum of regions vs TOTAL)"]
    assert {w for w, c, p in result.od_checks if abs(c - p) > Decimal("0.05")} == {"ER", "TOTAL"}


def test_section_e_requires_sign_convention_header_and_rows() -> None:
    with pytest.raises(InvalidWorkbookError, match="sign convention"):
        parse_section_e(wb(e_heading="E. Import/Export by Regions (in MU)"))
    with pytest.raises(InvalidWorkbookError, match="rows not found"):
        parse_section_e(wb(section_e=[SECTION_E[2]]))
    with pytest.raises(InvalidWorkbookError, match="section E not found"):
        parse_section_e(wb(omit_section="E"))


# ------------------------------------------------------------------ IR-Line

from src.pipelines.grid_india.parse_psp import parse_intl_lines, parse_ir_lines  # noqa: E402
from src.pipelines.grid_india.reference import COUNTRIES, INTERNATIONAL_LINES, line_country  # noqa: E402
from tests.support.grid_india import INTL_ROWS, IR_BLOCKS  # noqa: E402


def test_ir_lines_regions_values_signs_and_convention() -> None:
    result = parse_ir_lines(wb())
    assert not result.failures and not result.unknown_rows and len(result.records) == 6
    assert result.direction_convention == "Import=(+ve) /Export =(-ve) for NET (MU)"
    by_line = {r.values["line_name"]: r.values for r in result.records}
    gaya = by_line["TEST GAYA-VARANASI"]
    assert (gaya["from_region_code"], gaya["to_region_code"], gaya["voltage_level"], gaya["circuits"]) == (
        "ER", "NR", "765 kV", "2")
    assert (gaya["max_import_mw"], gaya["max_export_mw"], gaya["import_mu"], gaya["export_mu"], gaya["net_exchange_mu"]) \
        == (Decimal("357.013"), Decimal("1339.017"), Decimal("0.0130"), Decimal("17.2170"), Decimal("-17.2040"))
    assert by_line["TEST PUSAULI B/B"]["circuits"] is None  # '-' in the source
    assert by_line["TEST GWALIOR-AGRA"]["from_region_code"] == "WR"
    assert all(r.values["direction_convention"] == result.direction_convention for r in result.records)


def test_ir_subtotals_reconcile_and_single_excluded_line_is_identified() -> None:
    result = parse_ir_lines(wb())
    by_pair = {pair: (total, published, excluded) for pair, total, published, excluded in result.subtotals}
    assert set(by_pair) == {"ER-NR", "ER-SR", "WR-NR"}
    assert by_pair["ER-NR"][2] is None and by_pair["ER-NR"][0] == by_pair["ER-NR"][1]
    assert by_pair["ER-SR"][2] == "TEST TALCHER-I/C"   # published subtotal leaves this line out
    off = parse_ir_lines(wb(ir_subtotal_override={"ER-NR": (1.0, 2.0, -1.0)}))
    assert {p: e for p, t, pub, e in off.subtotals}["ER-NR"] is None  # unexplained gap
    assert any(p == "ER-NR" and t != pub for p, t, pub, _ in off.subtotals)


@pytest.mark.parametrize("variant", [{"with_j": False}, {"g_padding_rows": 3}, {"section_c": default_section_c()[:-5]}])
def test_ir_line_sheet_parses_regardless_of_other_sheet_layouts(variant: dict) -> None:
    workbook = wb(**variant)
    assert len(parse_ir_lines(workbook).records) == 6
    assert len(parse_intl_lines(workbook, line_country=line_country).records) == 9


def test_ir_lines_unknown_block_region_bad_value_and_unknown_row() -> None:
    blocks = [("ER", "XR", [("400 kV", "TEST ODD LINE", "1", 1.0, 1.0, 1.0, 0.5)], ()),
              ("ER", "NR", [("400 kV", "TEST BAD LINE", "1", 1.0, 1.0, "n/a", 0.5, 0.5)], ("TEST BAD LINE",))]
    result = parse_ir_lines(wb(ir_blocks=blocks))
    odd = next(r.values for r in result.records if r.values["line_name"] == "TEST ODD LINE")
    assert odd["from_region_code"] == "ER" and odd["to_region_code"] is None
    assert len(result.failures) == 1 and "TEST BAD LINE" in result.failures[0].description


def test_ir_lines_require_convention_and_sheet() -> None:
    with pytest.raises(InvalidWorkbookError, match="sheet 'IR-Line' not found"):
        parse_ir_lines(wb(omit_section="IR"))


def test_international_reference_is_exact_names_only() -> None:
    assert len(INTERNATIONAL_LINES) == 16 and set(INTERNATIONAL_LINES.values()) == {"BTN", "NPL", "BGD"}
    assert set(COUNTRIES) == {"BTN", "NPL", "BGD", "MMR"}   # MMR: CrossBorder only (no lines)
    assert line_country("132kV GELEPHU-SALAKATI") == "BTN"
    assert line_country("  132kv   gelephu-salakati ") == "BTN"   # whitespace / case normalised only
    assert line_country("132kV GELEPHU SALAKATI") is None          # no fuzzy or keyword matching
    assert line_country("NEPAL EXPORT (FROM UP)") is None
    assert line_country("BHERAMARA B/B HVDC (B'DESH)") == "BGD"


def test_international_lines_countries_regions_labels_and_section_d() -> None:
    result = parse_intl_lines(wb(), line_country=line_country)
    assert not result.failures and len(result.records) == 9
    assert result.sign_convention == "Import(+ve)/Export(-ve)"
    by_line = {r.values["line_name"]: r for r in result.records}
    godda = by_line["400kV GODDA_TPS-RAHANPUR (B'DESH) D/C"]
    assert (godda.values["country_code"], godda.values["region_code"], godda.values["region_label"]) == (
        "BGD", "ER", "ER (Isolated from Indian Grid)")
    assert godda.raw["country_label"] == "BANGLADESH" and godda.raw["country_label_code"] == "BGD"
    nepal_up = by_line["NEPAL IMPORT (FROM UP)"]
    assert nepal_up.values["country_code"] == "NPL" and nepal_up.raw["country_label"] is None  # label on another row
    bher = by_line["BHERAMARA B/B HVDC (B'DESH)"].values
    assert (bher["max_mw"], bher["min_mw"], bher["avg_mw"], bher["energy_exchange_mu"]) == (
        Decimal("-974.013"), Decimal("-954.017"), Decimal("-966.671"), Decimal("-23.2010"))  # signs kept
    assert {label: total == published for label, total, published in result.section_d} == {
        "BHUTAN": True, "NEPAL": True, "BANGLADESH": True, "GODDA -> BANGLADESH": True}
    assert len(result.mw_not_reported) == 2   # Tala and Chukha receipt rows: MW 0, energy != 0


def test_international_unknown_line_label_conflict_and_section_d_gap() -> None:
    rows = list(INTL_ROWS)
    rows[2] = ("", "NER", "132kV NEW BORDER LINE", -1.0, 0.0, -1.0, -0.5)        # not in the reference
    rows[4] = ("BHUTAN", "NR", "132kV MAHENDRANAGAR-TANAKPUR(NHPC)", 68.761, 0.0, 61.013, 1.471)  # wrong label
    result = parse_intl_lines(wb(intl_rows=rows, section_d_override={"Nepal": 99.0}), line_country=line_country)
    unknown = next(r for r in result.records if r.values["line_name"] == "132kV NEW BORDER LINE")
    assert unknown.values["country_code"] is None
    conflict = next(r for r in result.records if r.values["line_name"].startswith("132kV MAHENDRANAGAR"))
    assert conflict.values["country_code"] == "NPL" and conflict.raw["country_label_code"] == "BTN"
    assert result.section_d == []  # not comparable while a line is unmapped
    gap = parse_intl_lines(wb(section_d_override={"Nepal": 99.0}), line_country=line_country)
    assert [label for label, t, p in gap.section_d if t != p] == ["NEPAL"]


def test_international_requires_sign_convention() -> None:
    with pytest.raises(InvalidWorkbookError, match="sign convention"):
        parse_intl_lines(wb(intl_heading_convention=""), line_country=line_country)


# --------------------------------------------------------------- CrossBorder

from src.pipelines.grid_india.parse_psp import CROSSBORDER_FIRST_PUBLISHED, parse_crossborder  # noqa: E402

GNA_CONTRACTS = {"GNA", "T_GNA_BILATERAL", "COLLECTIVE_IDAM", "COLLECTIVE_RTM"}


def cb(day: date = D, **cb_kwargs):
    return parse_crossborder(read_workbook(workbook_bytes(day, cb=cb_kwargs or None), expected_date=day))


def _cb_value(result, direction: str, country: str, contract: str, exchange: str | None = None):
    return next(r for r in result.records if (r.values["direction"], r.values["country_code"], r.values["contract_type"],
                                              r.values["power_exchange"]) == (direction, country, contract, exchange))


def test_crossborder_gna_layout_blocks_contracts_and_signs() -> None:
    result = cb()
    assert result.layout == "GNA" and not result.failures and not result.missing_values and not result.unknown_rows
    assert len(result.records) == 96   # 3 blocks x 4 countries x 8 columns; TOTAL column and Total rows not records
    assert {r.values["contract_type"] for r in result.records} == GNA_CONTRACTS
    assert {r.values["country_code"] for r in result.records} == {"BTN", "NPL", "BGD", "MMR"}
    assert {(r.values["contract_type"], r.values["power_exchange"]) for r in result.records
            if r.values["contract_type"] in ("GNA", "T_GNA_BILATERAL")} == {("GNA", None), ("T_GNA_BILATERAL", None)}
    assert {r.values["power_exchange"] for r in result.records if r.values["contract_type"].startswith("COLLECTIVE")} \
        == {"IEX", "PXIL", "HPX"}
    assert _cb_value(result, "EXPORT", "BGD", "GNA").values["scheduled_energy_mu"] == Decimal("24.4100")
    assert _cb_value(result, "NET", "BGD", "GNA").values["scheduled_energy_mu"] == Decimal("-24.4060")  # signed Net
    assert _cb_value(result, "IMPORT", "NPL", "COLLECTIVE_RTM", "IEX").values["scheduled_energy_mu"] == Decimal("10.5300")
    conventions = {r.values["direction"]: r.values["sign_convention"] for r in result.records}
    assert conventions["EXPORT"] == "Export From India (in MU)" and conventions["IMPORT"] == "Import by India(in MU)"
    assert conventions["NET"] == "Net from India(in MU) -ve : Export / +ve : Import"
    assert all(abs(s - p) <= Decimal("0.05") for _, s, p in result.totals + result.net_checks)
    assert len(result.totals) == 3 * (5 + 9) and len(result.net_checks) == 5 * 9


def test_crossborder_pre_gna_categories_keep_their_own_codes() -> None:
    result = cb(date(2023, 8, 1), layout="PRE_GNA")
    assert result.layout == "PRE_GNA" and len(result.records) == 96
    assert {r.values["contract_type"] for r in result.records} == {
        "ISGS_LTA_MTOA_PPA", "STOA_BILATERAL", "COLLECTIVE_IDAM", "COLLECTIVE_RTM"}   # never GNA / T-GNA


def test_crossborder_absent_before_first_publication_required_after() -> None:
    day = CROSSBORDER_FIRST_PUBLISHED - timedelta(days=1)
    result = parse_crossborder(read_workbook(workbook_bytes(day), expected_date=day))
    assert result.not_published and result.records == []
    with pytest.raises(InvalidWorkbookError, match="sheet 'CrossBorder' not found"):
        parse_crossborder(wb(omit_section="CB"))
    with pytest.raises(InvalidWorkbookError, match="sheet 'CrossBorder' not found"):
        parse_crossborder(read_workbook(workbook_bytes(CROSSBORDER_FIRST_PUBLISHED, with_cb=False),
                                        expected_date=CROSSBORDER_FIRST_PUBLISHED))


@pytest.mark.parametrize(("override", "message"), [
    ({(3, 4): "DAM"}, "unknown column header"),               # IDAM renamed
    ({(4, 5): "NSE"}, "unknown exchange"),                     # new exchange under IDAM
    ({(1, 8): "TAM"}, "unrecognised header labels"),           # extra label above a column
    ({(1, 3): "LTA"}, "unknown column header"),                # bilateral regime not T-GNA / STOA
])
def test_crossborder_unknown_headers_fail_the_section(override: dict, message: str) -> None:
    with pytest.raises(InvalidWorkbookError, match=message):
        cb(header_override=override)


def test_crossborder_requires_net_sign_convention() -> None:
    with pytest.raises(InvalidWorkbookError, match="sign convention"):
        cb(net_heading="Net from India(in MU)")


def test_crossborder_unknown_country_duplicate_missing_and_bad_cells() -> None:
    unknown = cb(labels=("Bhutan", "Nepal", "Bangladesh", "Sri Lanka"))
    assert {r.values["country_code"] for r in unknown.records if r.raw["country_label"] == "Sri Lanka"} == {None}
    assert unknown.missing_countries == ["EXPORT|MYANMAR", "IMPORT|MYANMAR", "NET|MYANMAR"]
    duplicate = cb(labels=("Bhutan", "Nepal", "Nepal", "Myanmar"))
    assert {r.raw["duplicate"] for r in duplicate.records if r.values["country_code"] == "NPL"} == {True}
    assert not any(r.raw["duplicate"] for r in duplicate.records if r.values["country_code"] != "NPL")
    bad = cb(value_override={("EXPORT", "Bhutan", 4): "n/a", ("IMPORT", "Nepal", 7): ""})
    assert [f.key for f in bad.failures] == [f"{D}|EXPORT|BHUTAN|COLLECTIVE_IDAM/IEX"]
    assert len(bad.records) == 95
    assert bad.missing_values == [f"{D}|IMPORT|NEPAL|COLLECTIVE_RTM/IEX"]
    assert _cb_value(bad, "IMPORT", "NPL", "COLLECTIVE_RTM", "IEX").values["scheduled_energy_mu"] is None  # not derived


def test_crossborder_reconciliation_values_kept_as_published() -> None:
    def off(result, low=Decimal("0.05"), high=None):
        return {what for what, s, p in result.totals + result.net_checks
                if abs(s - p) > low and (high is None or abs(s - p) <= high)}

    net = cb(net_override={("Nepal", 7): -9.0, ("Bhutan", 1): 0.83})
    # Nepal RTM/IEX Net published -9.00 vs Import - Export 9.28 (and the totals that include it)
    assert off(net) == {"NEPAL COLLECTIVE_RTM/IEX", "NEPAL TOTAL", "TOTAL COLLECTIVE_RTM/IEX", "TOTAL TOTAL"}
    assert _cb_value(net, "NET", "NPL", "COLLECTIVE_RTM", "IEX").values["scheduled_energy_mu"] == Decimal("-9.0000")
    # Bhutan GNA 0.83 vs 0.81: a rounding-sized 0.02 difference, within tolerance
    assert off(net, Decimal("0"), Decimal("0.05")) == {"BHUTAN GNA", "BHUTAN TOTAL", "TOTAL GNA"}
    total = cb(total_override={("EXPORT", "Bhutan"): 9.99})
    assert off(total) == {"EXPORT BHUTAN row TOTAL", "EXPORT Total row TOTAL", "BHUTAN TOTAL"}


# ---------------------------------------------------------------- TimeSeries

from src.pipelines.grid_india.parse_psp import TIMESERIES_FIRST_PUBLISHED, parse_timeseries  # noqa: E402


def ts(day: date = D, **ts_kwargs):
    return parse_timeseries(read_workbook(workbook_bytes(day, ts=ts_kwargs or None), expected_date=day))


def _mark(result, hhmm: str):
    return next(r.values for r in result.records if r.values["interval_start"].strftime("%H:%M") == hhmm)


@pytest.mark.parametrize(("day", "layout", "storage_gen", "storage_demand"), [
    (date(2025, 1, 10), "13_COLUMNS", False, False),
    (date(2026, 5, 10), "15_COLUMNS", True, False),
    (D, "16_COLUMNS", True, True),
])
def test_timeseries_layouts_marks_and_storage_columns(day: date, layout: str, storage_gen: bool,
                                                      storage_demand: bool) -> None:
    result = ts(day)
    assert result.layout == layout and not result.failures and not result.missing_marks and not result.unknown_rows
    assert len(result.records) == 96
    starts = [r.values["interval_start"] for r in result.records]
    assert starts[0] == datetime(day.year, day.month, day.day, 0, 0, tzinfo=IST)
    assert starts[-1] == datetime(day.year, day.month, day.day, 23, 45, tzinfo=IST)
    assert {r.values["region_code"] for r in result.records} == {None}      # All India
    v = _mark(result, "12:30")
    assert v["solar_generation_mw"] == Decimal("40000.000") and v["grid_frequency_hz"] == Decimal("49.9800")
    assert (v["storage_generation_mw"] is not None) is storage_gen        # NULL when not published, never 0 (T4)
    assert (v["storage_demand_mw"] is not None) is storage_demand
    assert result.exchange_heading.startswith("NET")
    assert not (result.net_demand_mismatches or result.total_mismatches or result.telemetry_suspect)


@pytest.mark.parametrize("time_format", ["seconds", "fraction_text"])
def test_timeseries_time_cell_variants(time_format: str) -> None:
    day = date(2024, 11, 5)
    plain, variant = ts(day), ts(day, time_format=time_format)
    assert [r.values["interval_start"] for r in variant.records] == [r.values["interval_start"] for r in plain.records]


def test_timeseries_absent_before_first_publication_required_after() -> None:
    day = TIMESERIES_FIRST_PUBLISHED - timedelta(days=1)
    assert parse_timeseries(read_workbook(workbook_bytes(day), expected_date=day)).not_published
    with pytest.raises(InvalidWorkbookError, match="sheet 'TimeSeries' not found"):
        parse_timeseries(wb(omit_section="TS"))


def test_timeseries_unknown_header_and_exchange_sign_fail() -> None:
    with pytest.raises(InvalidWorkbookError, match="unknown column header"):
        ts(extra_header="COAL\n(MW)")
    with pytest.raises(InvalidWorkbookError, match="import as positive"):
        ts(exchange_heading="NET TRANSNATIONAL EXCHANGE\n(MW)\n(+ve) Export, (-ve) Import")


def test_timeseries_gaps_duplicates_and_bad_cells() -> None:
    missing = ts(drop_marks=(59,))
    assert len(missing.records) == 95 and missing.missing_marks == ["14:45"]   # reported, not filled
    empty = ts(empty=True)
    assert empty.records == [] and empty.missing_marks == [] and not empty.not_published
    dup = ts(duplicate_mark=10)
    assert [r.raw.get("duplicate") for r in dup.records if r.values["interval_start"].strftime("%H:%M") == "02:30"] \
        == [True, True]
    bad = ts(value_override={(3, "WIND"): "n/a", (4, "GAS"): ""})
    assert [f.key for f in bad.failures] == [f"{D}|00:45"] and len(bad.records) == 95
    assert bad.missing_values == [f"{D}|01:00|gas_generation_mw"] and _mark(bad, "01:00")["gas_generation_mw"] is None


def test_timeseries_checks_keep_published_values() -> None:
    result = ts(value_override={(40, "TOTAL GENERATION"): 1.0, (41, "NET DEMAND MET"): 5.0, (42, "FREQUENCY"): 0.0,
                                (50, "SOLAR"): -63.0})
    # 12:30 too: its solar cell was overridden without changing the published total / net demand
    assert [m for m, _, _ in result.total_mismatches] == ["10:00", "12:30"]
    assert [m for m, _, _ in result.net_demand_mismatches] == ["10:15", "12:30"]
    assert result.telemetry_suspect == [("10:30", "frequency 0.0000 Hz outside 49-51 Hz")]
    assert _mark(result, "10:00")["total_generation_mw"] == Decimal("1.000")      # stored as published
    assert _mark(result, "12:30")["solar_generation_mw"] == Decimal("-63.000")    # negative kept, not rejected
    zeros = {(43, n): 0.0 for n in ("NUCLEAR", "WIND", "SOLAR", "HYDRO", "GAS", "THERMAL", "STORAGE", "OTHERS")}
    dead = ts(value_override=zeros)
    assert any(m == "10:45" and "every generation source is 0" in reason for m, reason in dead.telemetry_suspect)


def test_timeseries_total_excluding_storage_is_recognised() -> None:
    result = ts(date(2026, 5, 10), total_excludes_storage=True)
    assert len(result.total_excludes_storage) == 72 and result.total_mismatches == []   # storage 0 at 24 marks
