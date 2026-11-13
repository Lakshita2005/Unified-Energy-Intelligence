"""Tests for the NPP response parser and API helpers (no network, no database)."""

from __future__ import annotations

import copy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.pipelines.npp import api
from src.pipelines.npp.errors import EmptyResponseError, InvalidResponseError
from src.pipelines.npp.parser import parse_response
from tests.support.npp import DAY, DEMAND_ITEMS, GENERATION_ITEMS, to_bytes

IST = timezone(timedelta(hours=5, minutes=30))


def _parse(items, *, day: date = DAY, series=api.DEMAND.series):
    return parse_response(to_bytes(items), requested_date=day, expected_series=series)


# ------------------------------------------------------------------ parser


def test_demand_records_preserve_source_values_and_timestamps() -> None:
    result = _parse(DEMAND_ITEMS)

    assert result.failures == []
    first = result.records[0]
    assert first.key == "DEMAND MET|1794249093000"
    assert first.values == {
        "state_code": None, "region_code": None,
        "observed_at": datetime(2026, 11, 10, 0, 1, 33, tzinfo=IST),
        "name_of_data": "DEMAND MET", "value_of_data": Decimal("153196"), "unit_code": "MW",
        "source_updated_on_ms": 1794249093000,
    }
    assert first.raw == DEMAND_ITEMS[0]


def test_generation_series_names_are_stored_verbatim() -> None:
    result = _parse(GENERATION_ITEMS, series=api.GENERATION.series)

    assert len(result.records) == 12 and result.failures == []
    assert {r.values["name_of_data"] for r in result.records} == set(api.GENERATION.series)
    thermal = next(r for r in result.records if r.values["name_of_data"] == "THERMAL GENERATION")
    assert thermal.values["value_of_data"] == Decimal("127297")


def test_snapshot_times_are_not_rounded() -> None:
    items = [{"updated_on": 1794249093123, "name_of_data": "DEMAND MET", "value_of_data": 1}]

    observed = _parse(items).records[0].values["observed_at"]

    assert observed == datetime(2026, 11, 10, 0, 1, 33, 123000, tzinfo=IST)


def test_decimal_values_are_kept_exactly() -> None:
    items = [{"updated_on": 1794249093000, "name_of_data": "DEMAND MET", "value_of_data": 153196.25}]

    assert _parse(items).records[0].values["value_of_data"] == Decimal("153196.25")


def test_empty_response_is_an_error() -> None:
    with pytest.raises(EmptyResponseError, match="no data for 2026-11-10"):
        _parse([])


@pytest.mark.parametrize(
    ("content", "message"),
    [
        (b"<html>maintenance</html>", "not valid JSON"),
        (b'{"error": "x"}', "Expected a JSON array"),
        (b"[1, 2]", "expected an object"),
        (to_bytes([{"updated_on": 1, "name_of_data": "DEMAND MET"}]), "fields"),
        (to_bytes([{**DEMAND_ITEMS[0], "unit": "MW"}]), "fields"),
        (to_bytes(GENERATION_ITEMS), "None of the expected series"),
    ],
)
def test_structural_changes_fail_the_whole_response(content: bytes, message: str) -> None:
    with pytest.raises(InvalidResponseError, match=message):
        parse_response(content, requested_date=DAY, expected_series=api.DEMAND.series)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"updated_on": "1794249093000"}, "not integer epoch ms"),
        ({"updated_on": True}, "not integer epoch ms"),
        ({"value_of_data": "153196"}, "not numeric"),
        ({"value_of_data": None}, "not numeric"),
        ({"name_of_data": ""}, "non-empty string"),
        ({"name_of_data": "X" * 81}, "at most 80"),
        ({"updated_on": 1794249093000 - 86_400_000}, "outside the requested IST day"),
    ],
)
def test_bad_items_become_parse_failures_and_others_still_parse(change: dict, message: str) -> None:
    items = copy.deepcopy(DEMAND_ITEMS)
    items[1].update(change)

    result = _parse(items)

    assert len(result.records) == 2
    assert len(result.failures) == 1 and message in result.failures[0].description
    assert result.failures[0].raw == items[1]


def test_day_boundaries_are_ist() -> None:
    midnight_ist = 1794249000000  # 2026-11-10 00:00:00 IST
    items = [{"updated_on": midnight_ist, "name_of_data": "DEMAND MET", "value_of_data": 1},
             {"updated_on": midnight_ist + 86_400_000 - 1, "name_of_data": "DEMAND MET", "value_of_data": 2},
             {"updated_on": midnight_ist + 86_400_000, "name_of_data": "DEMAND MET", "value_of_data": 3}]

    result = _parse(items)

    assert [r.values["value_of_data"] for r in result.records] == [1, 2]
    assert len(result.failures) == 1


# --------------------------------------------------------------------- api


def test_endpoint_urls_and_parameters() -> None:
    assert api.endpoint_url("https://npp.gov.in/dashBoard/", api.DEMAND) == \
        "https://npp.gov.in/dashBoard/demandmet1chartdata"
    assert api.endpoint_url("https://npp.gov.in/dashBoard", api.GENERATION) == \
        "https://npp.gov.in/dashBoard/demandmet2chartdata"
    assert api.request_params(date(2026, 11, 1)) == {"date": "2026-11-01"}


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        (date(2026, 11, 5), date(2026, 11, 4), "before start"),
        (date(2026, 11, 12), date(2026, 11, 13), "future"),
        (date(2025, 9, 30), date(2025, 10, 1), "earliest"),
        (date(2026, 10, 1), date(2026, 11, 1), "at most 31 days"),
    ],
)
def test_invalid_date_ranges_are_refused(start: date, end: date, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        api.validate_date_range(start, end, today=date(2026, 11, 12))


def test_valid_ranges_include_31_days_and_today() -> None:
    api.validate_date_range(date(2026, 10, 1), date(2026, 10, 31), today=date(2026, 11, 12))
    api.validate_date_range(date(2026, 11, 12), date(2026, 11, 12), today=date(2026, 11, 12))
    assert api.date_range(date(2026, 10, 30), date(2026, 11, 1)) == \
        [date(2026, 10, 30), date(2026, 10, 31), date(2026, 11, 1)]
