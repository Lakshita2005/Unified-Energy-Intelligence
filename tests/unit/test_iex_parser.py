"""Tests for the IEX parser, endpoints and date guards (no network, no database)."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

import pytest

from src.pipelines.iex import api
from src.pipelines.iex.errors import EmptyResponseError, IexApiError, InvalidResponseError
from src.pipelines.iex.parser import parse_area_prices, parse_snapshot
from tests.support.iex import DAY, area_prices, area_record, dam_record, envelope, gdam_record, snapshot


def _snap(kind: str, content: bytes, start: date = DAY, end: date = DAY):
    return parse_snapshot(content, market=api.MARKETS[kind], start=start, end=end)


# ---------------------------------------------------------------- snapshot


@pytest.mark.parametrize("kind", ["dam", "rtm"])
def test_dam_and_rtm_string_prices_parse_to_96_exact_blocks(kind: str) -> None:
    result = _snap(kind, snapshot(kind))

    assert result.failures == []
    assert [r.values["time_block"] for r in result.records] == list(range(1, 97))
    first = result.records[0]
    assert first.values == {"market_code": f"IEX_{kind.upper()}", "area_code": "MKT", "delivery_date": DAY,
                            "time_block": 1, "price_inr_per_mwh": Decimal("3010.01"), "source_published_at": None}
    assert first.key == f"IEX_{kind.upper()}|MKT|2026-11-10|1"


def test_rtm_periods_without_spaces_and_dam_periods_with_spaces() -> None:
    rtm = json.loads(snapshot("rtm", blocks=range(96, 97)))["data"][0]["period"]
    dam = json.loads(snapshot("dam", blocks=range(96, 97)))["data"][0]["period"]

    assert (rtm, dam) == ("23:45-24:00", "23:45 - 24:00")
    assert _snap("rtm", snapshot("rtm", blocks=range(96, 97))).records[0].values["time_block"] == 96
    assert _snap("dam", snapshot("dam", blocks=range(96, 97))).records[0].values["time_block"] == 96


def test_gdam_json_number_price_is_parsed_exactly() -> None:
    content = envelope([gdam_record(DAY, 1, price=4450.29)])

    result = _snap("gdam", content)

    assert result.records[0].values["price_inr_per_mwh"] == Decimal("4450.29")


@pytest.mark.parametrize(("day", "era"), [(date(2026, 7, 7), "before"), (date(2026, 7, 8), "after")])
def test_gdam_accepts_the_field_set_of_each_era(day: date, era: str) -> None:
    result = _snap("gdam", envelope([gdam_record(day, 1, era=era)]), start=day, end=day)

    assert len(result.records) == 1 and result.failures == []


@pytest.mark.parametrize(("day", "era"), [(date(2026, 7, 7), "after"), (date(2026, 7, 8), "before")])
def test_gdam_rejects_the_wrong_era_field_set(day: date, era: str) -> None:
    with pytest.raises(InvalidResponseError, match="fields"):
        _snap("gdam", envelope([gdam_record(day, 1, era=era)]), start=day, end=day)


def test_multi_day_range() -> None:
    days = [date(2026, 11, 9), date(2026, 11, 10)]

    result = _snap("dam", snapshot("dam", days=days), start=days[0], end=days[1])

    assert len(result.records) == 192
    assert {r.values["delivery_date"] for r in result.records} == set(days)


@pytest.mark.parametrize(
    ("content", "error", "message"),
    [
        (b"<html>blocked</html>", InvalidResponseError, "not valid JSON"),
        (b'[{"date": "10-11-2026"}]', InvalidResponseError, "envelope"),
        (json.dumps({"statusCode": 200, "data": {"x": 1}}).encode(), InvalidResponseError, "expected a list"),
        (envelope([], status=400, message="Data validation error"), IexApiError, "statusCode 400"),
        (envelope([]), EmptyResponseError, "no records"),
        (envelope([dam_record(DAY, 1, extra="x")]), InvalidResponseError, "fields"),
        (envelope([{k: v for k, v in dam_record(DAY, 1).items() if k != "mcp"}]), InvalidResponseError, "fields"),
        (envelope(["not an object"]), InvalidResponseError, "expected an object"),
    ],
)
def test_structural_problems_fail_the_document(content: bytes, error: type, message: str) -> None:
    with pytest.raises(error, match=message):
        _snap("dam", content)


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"mcp": None}, "not numeric"),
        ({"mcp": "n/a"}, "not numeric"),
        ({"mcp": "3,114.65"}, "not numeric"),
        ({"mcp": True}, "not numeric"),
        ({"period": "00:00 - 00:30"}, "15-minute block"),
        ({"period": "00:05 - 00:20"}, "15-minute block"),
        ({"period": "block 1"}, "HH:MM"),
        ({"date": "2026-11-10"}, "does not match format"),
        ({"date": "11-11-2026"}, "outside the requested range"),
    ],
)
def test_bad_records_become_parse_failures(override: dict, message: str) -> None:
    records = [dam_record(DAY, 1), dam_record(DAY, 2, **override), dam_record(DAY, 3)]

    result = _snap("dam", envelope(records))

    assert [r.values["time_block"] for r in result.records] == [1, 3]
    assert len(result.failures) == 1 and message in result.failures[0].description
    assert result.failures[0].raw == records[1]


# -------------------------------------------------------------- area price


def test_area_prices_parse_with_block_start_times() -> None:
    result = parse_area_prices(area_prices("N1"), market=api.DAM, area="N1", start=DAY, end=DAY)

    assert len(result.records) == 96 and result.failures == []
    assert result.records[0].values["area_code"] == "N1"
    assert result.records[0].values["price_inr_per_mwh"] == Decimal("2960.01")
    assert result.records[-1].values["time_block"] == 96


def test_area_price_with_wrong_area_key_is_rejected() -> None:
    with pytest.raises(InvalidResponseError, match="fields"):
        parse_area_prices(envelope([area_record(DAY, 1, "S3")]), market=api.DAM, area="N1", start=DAY, end=DAY)


def test_area_price_timeperiod_must_start_a_block() -> None:
    record = {"date": "10-11-2026", "timeperiod": "00:10", "n1": 1.0}

    result = parse_area_prices(envelope([record]), market=api.DAM, area="N1", start=DAY, end=DAY)

    assert result.records == [] and "start of a 15-minute block" in result.failures[0].description


# --------------------------------------------------------------------- api


def test_urls_and_parameters() -> None:
    base = "https://www.iexindia.com/api/v1/"
    assert api.snapshot_url(base, api.GDAM) == "https://www.iexindia.com/api/v1/gdam/market-snapshot"
    assert api.area_price_url(base, api.DAM) == "https://www.iexindia.com/api/v1/dam/area-price"
    assert api.area_price_params(date(2026, 11, 1), date(2026, 11, 3), "W2") == {
        "interval": "ONE_FOURTH_HOUR", "fromDate": "01-11-2026", "toDate": "03-11-2026", "bidArea": "W2"}


def test_only_dam_and_gdam_fetch_area_prices() -> None:
    assert (api.DAM.area_prices, api.GDAM.area_prices, api.RTM.area_prices) == (True, True, False)
    assert len(api.BID_AREAS) == 13


@pytest.mark.parametrize(
    ("market", "start", "end", "message"),
    [
        (api.DAM, date(2026, 11, 5), date(2026, 11, 4), "before start"),
        (api.DAM, date(2026, 11, 14), date(2026, 11, 14), "after 2026-11-13"),
        (api.RTM, date(2026, 11, 13), date(2026, 11, 13), "after 2026-11-12"),
        (api.RTM, date(2020, 5, 31), date(2020, 6, 1), "earliest"),
        (api.GDAM, date(2021, 10, 26), date(2021, 10, 27), "earliest"),
        (api.DAM, date(2026, 10, 1), date(2026, 11, 1), "at most 31 days"),
    ],
)
def test_invalid_ranges_are_refused(market: api.Market, start: date, end: date, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        api.validate_date_range(market, start, end, today=date(2026, 11, 12))


def test_tomorrow_is_allowed_for_dam_and_gdam_only() -> None:
    api.validate_date_range(api.DAM, date(2026, 11, 13), date(2026, 11, 13), today=date(2026, 11, 12))
    api.validate_date_range(api.GDAM, date(2026, 10, 14), date(2026, 11, 13), today=date(2026, 11, 12))
    api.validate_date_range(api.RTM, date(2026, 11, 12), date(2026, 11, 12), today=date(2026, 11, 12))


def test_price_cap_schedule() -> None:
    assert api.price_cap(date(2019, 12, 31)) is None
    assert api.price_cap(date(2022, 4, 2)) == Decimal("20000")
    assert api.price_cap(date(2022, 4, 3)) == Decimal("12000")
    assert api.price_cap(date(2026, 11, 10)) == Decimal("10000")
